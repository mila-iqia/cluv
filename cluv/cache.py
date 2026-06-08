from __future__ import annotations

import functools
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from cluv.utils import find_pyproject


@functools.cache
def get_cache_path() -> Path:
    """Should be like : ~/.cache/cluv/<PROJECT_NAME>/jobs.jsonl"""
    project_name = find_pyproject().parent.name
    return Path.home() / ".cache" / "cluv" / project_name / "jobs.jsonl"


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
