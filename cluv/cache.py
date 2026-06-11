import dataclasses
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import platformdirs
import yaml

from cluv.utils import find_pyproject

logger = logging.getLogger(__name__)


@dataclass
class Job:
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


@dataclass
class CacheContent:
    project_states: dict[str, ProjectStateOnCluster] = dataclasses.field(default_factory=dict)


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
    return _read_cache(_get_cache_file())


def write_cache(cache: CacheContent):
    """Write the cache content to the (local) cache file."""
    _write_cache(cache, _get_cache_file())


def _read_cache(cache_file: Path) -> CacheContent:
    if not cache_file.exists():
        logger.debug("Empty cache (file %s does not exist)", cache_file)
        return CacheContent()
    logger.debug("Reading cache from %s", cache_file)
    return CacheContent(**yaml.safe_load(cache_file.read_text()))


def _write_cache(cache: CacheContent, cache_file: Path):
    logger.debug("Writing cache to %s: %s", cache_file, cache)
    cache_file.write_text(yaml.dump(asdict(cache), indent=2))


def _get_cached_jobs_path() -> Path:
    """Should be like : ~/.cache/cluv/<PROJECT_NAME>/jobs.jsonl"""
    return _get_cache_dir() / "jobs.jsonl"


def _get_cache_file() -> Path:
    """Get the path to a cache file on the remote cluster."""
    return _get_cache_dir() / "cluv_cache.yaml"


def _get_cache_dir() -> Path:
    """Returns the cluv cache directory (and create it if needed) for the current project."""
    project_name = find_pyproject().parent.name
    cache_dir = Path(platformdirs.PlatformDirs("cluv").user_cache_dir) / project_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
