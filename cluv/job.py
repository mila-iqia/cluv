"""A script that reads something, and produces some output.

This is a simplified job script, used to test the syncing of the 'dataset' across clusters.
"""

import functools
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

import cluv
import cluv.config
from cluv.utils import current_cluster

SLURM_JOB_ID: int | None = (
    int(os.environ["SLURM_JOB_ID"]) if "SLURM_JOB_ID" in os.environ else None
)
SCRATCH = Path(os.environ["SCRATCH"]) if "SCRATCH" in os.environ else None
SLURM_TMPDIR = Path(os.environ["SLURM_TMPDIR"]) if "SLURM_TMPDIR" in os.environ else None
SLURM_PROCID = int(os.environ["SLURM_PROCID"]) if "SLURM_PROCID" in os.environ else None


in_job_packing = "SLURM_NTASKS_PER_GPU" in os.environ
in_job_array = "SLURM_ARRAY_JOB_ID" in os.environ


@dataclass(frozen=True)
class JobInfo:
    """Information about a "job"/"run".

    Note, there may be multiple "runs" inside a single "job", that's why there is a distinction.
    """

    cluster: str

    run_id: str
    """The unique 'run identifier' for this job/run, used for checkpointing and Weights & Biases.

    This will usually just be {cluster}_{SLURM_JOB_ID}, but can also vary based on whether
    the job is doing job packing (with --ntasks-per-gpu) or job chunking (with --array=...%1) or
    both:

    - Normal job:                        `${SLURM_JOB_ID}`
    - Packing (with --ntasks-per-gpu>1): `${SLURM_JOB_ID}_${SLURM_PROCID}`
    - Chunking (with --array=0-N%1):     `${SLURM_ARRAY_JOB_ID}`
    - Chunking + Packing:                `${SLURM_ARRAY_JOB_ID}_${SLURM_PROCID}`

    Tip: Use this as the run_id for `wandb.init` or whenever you need a unique run identifier.
    """

    results_path: Path

    @property
    def datasets_path(self) -> Path | None:
        """The path where the datasets are located for this job (based on which cluster it runs on.)"""
        cluster_info = cluv.config.current_cluster_config()
        if not cluster_info:
            return None
        return cluster_info.datasets_path


def get_datasets_path() -> Path | None:
    """Returns the resolved 'datasets_path' from the Cluv config."""
    datasets_path = (
        cluv.config.current_cluster_config() or cluv.config.get_cluv_config()
    ).datasets_path
    return Path(os.path.expandvars(datasets_path)) if datasets_path else None


def current_job_info() -> JobInfo | None:
    """Returns information about the current job, such as its unique run id and results path.

    This is useful to determine where to save checkpoints or results for this job, and to have a unique
    identifier for this job that can be used in Weights & Biases or elsewhere.

    The 'run id' is determined based on the cluster name and SLURM job id, and also takes into account
    whether the job is doing job packing (with --ntasks-per-gpu) or job chunking (with --array=...%1).
    """
    if not SLURM_JOB_ID:
        return None  # not in a Slurm job.
    cluster = current_cluster()
    run_id = get_run_id()
    # IDEA: maybe load the cluv config and set the checkpoint_dir
    # from cluv.config import load_cluv_config
    assert cluster, "Example must be run on a cluster."
    config = cluv.config.current_cluster_config()
    assert config, "Example must be run on a cluster."
    assert config.results_path
    assert config.datasets_path
    return JobInfo(
        run_id=run_id,
        cluster=cluster,
        results_path=config.results_path / run_id,
    )


@functools.cache
def _get_max_active_jobs() -> int | None:
    """When in a job array, returns the max number of active jobs at the same time.

    For example, with --array=0-20%4, this returns 4.
    Returns `None` when not in a job array.
    Result is cached since this calls scontrol in a subprocess.
    """
    if "SLURM_ARRAY_JOB_ID" not in os.environ:
        return None
    output = subprocess.check_output(
        ["scontrol", "--oneliner", "show", "job", os.environ["SLURM_ARRAY_JOB_ID"]],
        text=True,
    )
    match = re.search(r"ArrayTaskId=\S+%(\d+)", output)
    return int(match.group(1)) if match else None


def _in_job_chunking() -> bool:
    return in_job_array and _get_max_active_jobs() == 1


def get_run_id():
    cluster = current_cluster()
    doing_job_packing = "SLURM_NTASKS_PER_GPU" in os.environ
    doing_job_chunking = _in_job_chunking()
    task_index = int(os.environ["SLURM_PROCID"])
    if doing_job_chunking:
        # IF we have --array=...%1, use the id of the first job.
        first_job_id = int(os.environ["SLURM_ARRAY_JOB_ID"])
        if doing_job_packing:
            # Running with --array=0-5%1 for chunking and --ntasks-per-gpu for packing! Awesome!!
            return f"{cluster}_{first_job_id}_task{task_index}"
        # IDEA: If we support doing an arrays of 'chunked' jobs, then we could use this:
        # IF we have --array=0-20%4, this means there are 4 jobs with 5 chunks each (weird).
        # max_active_jobs = get_max_active_jobs()
        # assert max_active_jobs is not None and max_active_jobs > 1
        # index_in_array = int(os.environ["SLURM_ARRAY_TASK_ID"])
        # return str(first_job_id + (index_in_array % max_active_jobs))
        # Keeping it simple for now, only support chunking with --array=...%1, so we always use
        # the id of the first job in the array.
        return f"{cluster}_{first_job_id}"
    if doing_job_packing:
        return f"{cluster}_{SLURM_JOB_ID}_task{SLURM_PROCID}"
    return f"{cluster}_{SLURM_JOB_ID}"
