from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import platformdirs

from cluv.utils import find_pyproject


def get_cluv_project_cache_dir() -> Path:
    """Get the path to the cluv cache directory for the current project."""
    project_name = find_pyproject().parent.name
    cache_dir = Path(platformdirs.PlatformDirs("cluv").user_cache_dir) / project_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_cache_path() -> Path:
    """Should be like : ~/.cache/cluv/<PROJECT_NAME>/jobs.jsonl"""
    return get_cluv_project_cache_dir() / "jobs.jsonl"


@dataclass
class Job:
    job_id: int
    cluster: str
    job_script: str
    git_commit: str
    submitted_at: str  # ISO 8601 UTC
    sbatch_args: list[str]
    program_args: list[str]


def save_job(job: Job) -> None:
    path = get_cache_path()

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(asdict(job)) + "\n")


def load_jobs() -> list[Job]:
    path = get_cache_path()
    if not path.exists():
        return []
    jobs = []
    for line in path.read_text().splitlines():
        try:
            jobs.append(Job(**json.loads(line)))
        except Exception:
            pass
    return jobs


def _get_cache_file(cluster: str, filename: str) -> Path:
    """Get the path to a cache file on the remote cluster."""
    uv_cache_dir = get_cluv_project_cache_dir()
    uv_cache_dir.mkdir(parents=True, exist_ok=True)
    return uv_cache_dir / f"{cluster}_{filename}"


def get_cache_content(cluster: str, filename: str) -> str | None:
    """Get the content of a local cache file with respect to an operation done on this cluster."""
    cache_file = _get_cache_file(cluster, filename)
    if not cache_file.exists():
        return None
    return cache_file.read_text().strip()


def write_cache_content(cluster: str, filename: str, content: str):
    """Write content to a local cache file with respect to an operation done on this cluster."""
    cache_file = _get_cache_file(cluster, filename)
    cache_file.write_text(content)
