import dataclasses
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import platformdirs
import pydantic
import yaml

from cluv.utils import find_pyproject

logger = logging.getLogger(__name__)


@dataclass
class Job:
    """A Job on a Slurm cluster. This object is returned by `cluv submit`."""

    job_id: int
    cluster: str
    job_script: str
    git_commit: str
    submitted_at: str  # ISO 8601 UTC
    sbatch_args: list[str]
    program_args: list[str]


@dataclass
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


@dataclass
class DisabledCluster:
    """Represents a cluster that has been temporarily or indefinitely disabled."""

    disabled_at: datetime
    """When the cluster was disabled."""

    disabled_until: datetime | None = None
    """When the cluster should automatically re-enable. None means disabled indefinitely."""


@dataclass
class CacheContent:
    project_states: dict[str, ProjectStateOnCluster] = dataclasses.field(default_factory=dict)
    disabled_clusters: dict[str, DisabledCluster] = dataclasses.field(default_factory=dict)


def save_job(job: Job) -> None:
    path = _get_cached_jobs_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(asdict(job)) + "\n")


def load_jobs() -> list[Job]:
    path = _get_cached_jobs_path()
    if not path.exists():
        return []
    jobs = []
    for line in path.read_text().splitlines():
        try:
            jobs.append(Job(**json.loads(line)))
        except Exception:
            pass
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
