import dataclasses
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import platformdirs
import pydantic
import yaml

from cluv.config import SbatchArgs
from cluv.remote import Remote
from cluv.utils import find_pyproject

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Submission:
    """One job to submit: on which cluster, over which connection, and with which sbatch flags."""

    cluster: str

    remote: Remote | None
    """Remote used to run `sbatch`, or `None` to run it on the current cluster."""

    job_script: Path

    sbatch_args: SbatchArgs
    """The sbatch flags actually used, after merging the config and the CLI."""

    program_args: list[str]

    sbatch_command: str

    n_chunks: int | None

    git_commit: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class Job(Submission):
    """A Job on a Slurm cluster. Returned by `cluv submit`, and persisted to the on-disk
    job-history cache (see `save_job`/`load_jobs`, used e.g. by `cluv status`).
    """

    job_id: int
    submitted_at: datetime


@dataclass()
class ProjectStateOnCluster:
    """The cached info we have about the state of the project on a cluster.

    This is used to avoid redoing a sync unless necessary.
    """

    uv_version: str | None = None
    last_uv_sync_git_commit: str | None = None
    last_pushed_datasets: datetime | None = None
    checked_out_git_commit: str | None = None
    last_fetch_watermark: datetime | None = None
    """Max mtime seen among this cluster's `results_path` run dirs, as of the last
    successful `fetch_results` call. Used by `cluv clean` to distinguish runs the
    user pruned locally from runs that were never fetched."""


@dataclass(frozen=True)
class DisabledCluster:
    """Represents a cluster that has been temporarily or indefinitely disabled."""

    disabled_at: datetime
    """When the cluster was disabled."""

    disabled_until: datetime | None = None
    """When the cluster should automatically re-enable. None means disabled indefinitely."""


@dataclass(frozen=True)
class CacheContent:
    project_states: dict[str, ProjectStateOnCluster] = dataclasses.field(default_factory=dict)
    disabled_clusters: dict[str, DisabledCluster] = dataclasses.field(default_factory=dict)


def _job_record(job: Job) -> dict:
    """The on-disk (JSON-safe) shape of a `Job`, as written by `save_job` and read by
    `load_jobs` -- factored out so a migrated record (see `_migrate_legacy_job_record`) can be
    rewritten in the exact same shape.
    """
    record = asdict(job)
    record["job_script"] = str(job.job_script)
    record["submitted_at"] = job.submitted_at.isoformat()
    # `remote` isn't meaningful once persisted (a stale SSH-multiplexing handle); `cluster`
    # already identifies where the job ran, and callers reconnect by hostname when needed.
    record.pop("remote", None)
    return record


def save_job(job: Job) -> None:
    path = _get_cached_jobs_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(_job_record(job)) + "\n")


def _parse_legacy_sbatch_args(flags: list[str]) -> SbatchArgs:
    """Parse a pre-refactor cache record's flat `sbatch_args` flag list (e.g.
    `["--time=1:00:00", "-N", "2"]`) into the current dict shape.

    Mirrors the CLI-flag-parsing half of `merge_sbatch_args` in `cli/submit.py` -- duplicated
    rather than imported, since `cli.submit` already imports from this module and importing
    back would be circular.
    """
    parsed: SbatchArgs = {}
    index = 0
    while index < len(flags):
        flag = flags[index]
        if flag.startswith("--"):
            key, _, value = flag[2:].partition("=")
            parsed[key] = value if value else True
            index += 1
        elif flag.startswith("-"):
            key = flag[1:]
            has_separate_value = index + 1 < len(flags) and not flags[index + 1].startswith("-")
            if has_separate_value:
                parsed[key] = flags[index + 1]
                index += 2
            else:
                parsed[key] = True
                index += 1
        else:
            index += 1  # Not a flag shape we recognize; skip it rather than fail the record.
    return parsed


def _migrate_legacy_job_record(data: dict) -> dict:
    """Convert a pre-refactor cache record into the current `Job` schema: `sbatch_args` was a
    flat flag list rather than a dict, and there was no `sbatch_command` field at all.

    `sbatch_command` has no old-schema equivalent -- older cluv versions never recorded the
    exact command that ran -- so it's left as an empty string. That's safe: nothing currently
    reads `sbatch_command`/`sbatch_args` back from job history (`cluv status jobs` only shows
    `cluster`, `job_id`, `submitted_at`, `git_commit`).
    """
    data = dict(data)
    data["sbatch_args"] = _parse_legacy_sbatch_args(data["sbatch_args"])
    data.setdefault("sbatch_command", "")
    return data


def load_jobs() -> list[Job]:
    """Load every cached job record, migrating any pre-refactor ones (see
    `_migrate_legacy_job_record`) and dropping any that are unreadable even after that.

    If migrating or dropping anything, rewrites the cache file with just the surviving
    records in the current schema, so this only has to happen once.
    """
    path = _get_cached_jobs_path()
    if not path.exists():
        return []
    jobs = []
    migrated = 0
    skipped = 0
    for line in path.read_text().splitlines():
        try:
            data = json.loads(line)
            if isinstance(data.get("sbatch_args"), list):
                data = _migrate_legacy_job_record(data)
                migrated += 1
            data["job_script"] = Path(data["job_script"])
            data["submitted_at"] = datetime.fromisoformat(data["submitted_at"])
            data.setdefault("remote", None)
            jobs.append(Job(**data))
        except Exception:
            skipped += 1
    if migrated:
        logger.info(f"Migrated {migrated} job record(s) from an older cluv cache format.")
    if skipped:
        # Most likely records written by a version of cluv with a different Job schema
        # (e.g. before a field was added/renamed) -- surfaced so "no jobs shown" is
        # diagnosable instead of silently empty.
        logger.warning(f"Dropped {skipped} unreadable job record(s) from {path}.")
    if migrated or skipped:
        with path.open("w") as f:
            for job in jobs:
                f.write(json.dumps(_job_record(job)) + "\n")
    return jobs


def read_cache() -> CacheContent:
    """Read the cache content from the (local) cache file."""
    cache_file = _get_cache_file()
    if not cache_file.exists():
        logger.debug("Empty cache (file %s does not exist)", cache_file)
        return CacheContent()
    logger.debug("Reading cache from %s", cache_file)
    raw_content = yaml.safe_load(cache_file.read_text())
    return pydantic.TypeAdapter(CacheContent).validate_python(raw_content)


def write_cache(cache: CacheContent):
    """Write the cache content to the (local) cache file."""
    cache_file = _get_cache_file()
    logger.debug("Writing cache to %s: %s", cache_file, cache)
    cache_file.write_text(yaml.dump(asdict(cache), indent=2))


def _get_cached_jobs_path() -> Path:
    """Should be like : ~/.cache/cluv/<PROJECT_NAME>/jobs.jsonl"""
    return _get_cache_dir() / "jobs.jsonl"


def _get_cache_file() -> Path:
    """Get the path to a cache file on the remote cluster."""
    return _get_cache_dir() / "cluv_cache.yaml"


def disable_cluster(cluster: str, disabled_until: datetime | None = None) -> None:
    """Disable a cluster, optionally until a given datetime.

    Parameters:
        cluster: The cluster hostname to disable.
        disabled_until: When to automatically re-enable the cluster. If None, the cluster
            is disabled indefinitely until manually re-enabled with `enable_cluster`.
    """
    cache = read_cache()
    cache.disabled_clusters[cluster] = DisabledCluster(
        disabled_at=datetime.now(tz=timezone.utc),
        disabled_until=disabled_until,
    )
    write_cache(cache)


def _ensure_utc(dt: datetime) -> datetime:
    """Return *dt* as a UTC-aware datetime, assuming UTC if naive."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def enable_cluster(cluster: str) -> bool:
    """Re-enable a previously disabled cluster.

    Parameters:
        cluster: The cluster hostname to re-enable.

    Returns:
        True if the cluster was disabled and has now been enabled, False if it wasn't disabled.
    """
    cache = read_cache()
    if cluster not in cache.disabled_clusters:
        return False
    cache.disabled_clusters.pop(cluster)
    write_cache(cache)
    return True


def is_cluster_disabled(cluster: str) -> bool:
    """Check if a cluster is currently disabled.

    A cluster is considered disabled if it was explicitly disabled and either has no expiry
    or its expiry time has not yet passed.

    Parameters:
        cluster: The cluster hostname to check.

    Returns:
        True if the cluster is currently disabled, False otherwise.
    """
    cache = read_cache()
    disabled = cache.disabled_clusters.get(cluster)
    if disabled is None:
        return False
    if disabled.disabled_until is None:
        return True
    if datetime.now(tz=timezone.utc) >= _ensure_utc(disabled.disabled_until):
        # Expiry has passed — auto-remove and re-enable.
        cache.disabled_clusters.pop(cluster)
        write_cache(cache)
        return False
    return True


def get_disabled_clusters() -> dict[str, DisabledCluster]:
    """Return a mapping of currently disabled clusters.

    Clusters whose disable period has expired are automatically removed and not returned.
    """
    cache = read_cache()
    now = datetime.now(tz=timezone.utc)
    expired = [
        cluster
        for cluster, info in cache.disabled_clusters.items()
        if info.disabled_until is not None and now >= _ensure_utc(info.disabled_until)
    ]
    if expired:
        for cluster in expired:
            cache.disabled_clusters.pop(cluster)
        write_cache(cache)
    return dict(cache.disabled_clusters)


def _get_cache_dir() -> Path:
    """Returns the cluv cache directory (and create it if needed) for the current project."""
    project_name = find_pyproject().parent.name
    cache_dir = Path(platformdirs.PlatformDirs("cluv").user_cache_dir) / project_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
