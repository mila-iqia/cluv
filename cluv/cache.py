from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

JOBS_CACHE_PATH = Path.home() / ".cache" / "cluv" / "jobs.jsonl"


@dataclass
class CachedJob:
    job_id: int
    cluster: str
    job_script: str
    git_commit: str
    submitted_at: str  # ISO 8601 UTC
    sbatch_args: list[str]
    program_args: list[str]


def save_job(
    job_id: int,
    cluster: str,
    job_script: str,
    git_commit: str,
    sbatch_args: list[str],
    program_args: list[str],
) -> None:
    job = CachedJob(
        job_id=job_id,
        cluster=cluster,
        job_script=job_script,
        git_commit=git_commit,
        submitted_at=datetime.now(timezone.utc).isoformat(),
        sbatch_args=sbatch_args,
        program_args=program_args,
    )
    JOBS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with JOBS_CACHE_PATH.open("a") as f:
        f.write(json.dumps(asdict(job)) + "\n")


def load_jobs() -> list[CachedJob]:
    if not JOBS_CACHE_PATH.exists():
        return []
    jobs = []
    for line in JOBS_CACHE_PATH.read_text().splitlines():
        try:
            jobs.append(CachedJob(**json.loads(line)))
        except Exception:
            pass
    return jobs
