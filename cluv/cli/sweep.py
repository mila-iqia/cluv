"""CLI orchestration for `cluv sweep`: expands a comma-list sweep spec into combos and
submits one small, identically-shaped job per GPU's (or node's) packing capacity.

See `design/cluv-sweep.md` for the full design.
"""

from __future__ import annotations

import asyncio
import math
import re
from pathlib import Path

from cluv.cache import Job
from cluv.cli.submit import (
    _check_job_script_exists_locally,
    get_job_script_path_from_config,
    submit,
)
from cluv.cli.sync import sync
from cluv.sweep import CLUV_SWEEP_NAME_ENV_VAR, CLUV_SWEEP_TASK_OFFSET_ENV_VAR, expand_sweep_args
from cluv.utils import batched, console, current_cluster

_NTASKS_RE = re.compile(r"^--ntasks=(\d+)$")
_NTASKS_PER_GPU_RE = re.compile(r"^--ntasks-per-gpu=(\d+)$")
_GPU_FLAG_RE = re.compile(
    r"^--(gres|gpus|gpus-per-task|gpus-per-node)=(?:gpu:)?(?:[\w.-]+:)?(\d+)$"
)


def compute_job_capacity(sbatch_args: list[str]) -> int:
    """Task-slot capacity of ONE job, from whatever sizing flags the user put in
    `sbatch_args` (last match wins, same convention as `chunking.py`'s time parsing):

    - explicit `--ntasks=N` -> `N`
    - `--ntasks-per-gpu=K` (+ optionally a GPU count parsed from `--gres=gpu:...`/`--gpus=...`,
      defaulting to 1 GPU if a GPU flag is present without an explicit count, or if no GPU
      flag is present at all) -> `K * gpu_count`
    - neither present -> `1` (no packing; one job per combo, i.e. today's default behavior)
    """
    ntasks: int | None = None
    ntasks_per_gpu: int | None = None
    gpu_count: int | None = None
    for arg in sbatch_args:
        if match := _NTASKS_RE.match(arg):
            ntasks = int(match.group(1))
        elif match := _NTASKS_PER_GPU_RE.match(arg):
            ntasks_per_gpu = int(match.group(1))
        elif match := _GPU_FLAG_RE.match(arg):
            gpu_count = int(match.group(2))

    if ntasks is not None:
        return ntasks
    if ntasks_per_gpu is not None:
        return ntasks_per_gpu * (gpu_count or 1)
    return 1


def default_sweep_name(job_script: Path) -> str:
    return job_script.stem  # e.g. scripts/job.sh -> "job" (deterministic re-run default)


async def sweep(
    cluster: str,
    job_script: Path | None,
    name: str | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool = False,
    max_concurrent_submissions: int = 8,
) -> list[Job]:
    """Expand `program_args`' `--flag=v1,v2,...` sweep spec into combos, and submit
    `ceil(n_combos / job_capacity)` identically-shaped jobs to cover them — see
    `design/cluv-sweep.md` for why one job per GPU (or node) schedules better than one
    giant packed job.
    """
    if cluster == "first":
        raise NotImplementedError("`cluv sweep` does not support cluster='first' yet.")

    combos = expand_sweep_args(program_args)
    n_combos = len(combos)

    resolved_job_script = job_script or get_job_script_path_from_config(cluster)
    resolved_job_script = _check_job_script_exists_locally(resolved_job_script, cluster)
    sweep_name = name or default_sweep_name(resolved_job_script)

    job_capacity = compute_job_capacity(sbatch_args)
    num_jobs = math.ceil(n_combos / job_capacity)

    console.log(
        f"[cluv sweep {sweep_name!r}] {n_combos} combo(s), {job_capacity} per job "
        f"-> submitting {num_jobs} job(s)."
    )

    # Sync once up front (same pattern already used by hydra_plugins/cluv/cluv_launcher.py's
    # run_sweep(), which syncs once then calls submit(..., _skip_sync=True) per job).
    if cluster != current_cluster():
        await sync(clusters=[cluster])

    async def _submit_one(job_index: int) -> Job | None:
        offset = job_index * job_capacity
        return await submit(
            cluster=cluster,
            job_script=resolved_job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            autocommit=autocommit,
            chunking=None,
            in_job_packing=True,
            extra_env={
                CLUV_SWEEP_NAME_ENV_VAR: sweep_name,
                CLUV_SWEEP_TASK_OFFSET_ENV_VAR: str(offset),
            },
            _skip_sync=True,
        )

    # Throttle submissions in batches, same cluv.utils.batched() pattern already used by
    # hydra_plugins/cluv/cluv_launcher.py's array_parallelism, rather than firing every
    # `sbatch` call at once.
    jobs: list[Job | None] = []
    for job_indices in batched(range(num_jobs), max_concurrent_submissions or num_jobs):
        jobs.extend(await asyncio.gather(*(_submit_one(i) for i in job_indices)))
    return [j for j in jobs if j is not None]
