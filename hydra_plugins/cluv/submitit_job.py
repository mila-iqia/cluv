from collections.abc import Sequence

from remote_slurm_executor.slurm_remote import RemoteSlurmJob
from submitit.slurm.slurm import SlurmJob

from cluv.job import JobInfo
from cluv.utils import current_cluster


def _convert_job_info_to_slurm_job(job: JobInfo) -> Sequence[SlurmJob | RemoteSlurmJob]:
    job_ids = [f"{job.job_id}_{i}" for i in range(job.n_chunks)] if job.n_chunks else [job.job_id]

    # TODO: Unclear if this makes sense when tasks>1 (for example when doing job packing).
    if job.cluster == current_cluster():
        return [
            SlurmJob(
                folder=job.tasks[0].results_path,
                job_id=job_id,
                tasks=list(range(len(job.tasks))),
            )
            for job_id in job_ids
        ]

    return [
        RemoteSlurmJob(
            job.cluster,
            folder=job.tasks[0].results_path,
            job_id=job_id,
            tasks=list(range(len(job.tasks))),
            remote_dir_sync=None,  # type: ignore
        )
        for job_id in job_ids
    ]


def get_job_state(job: JobInfo) -> str:
    """Reuse the state polling logic from submitit to get the state of the job.
    Note: This doesn't call sacct too often, there is a caching mechanism in submitit.
    """
    slurm_job = _convert_job_info_to_slurm_job(job)
    states = [chunk_job.state for chunk_job in slurm_job]

    if "FAILED" in states:
        return "FAILED"
    for state in reversed(states):
        if state != "UNKNOWN":
            return state
    return "UNKNOWN"


def is_job_done(job: JobInfo, force_check: bool = False) -> bool:
    """Whether all the chunks of this job are finished.
    Reuses the result-file/state-polling logic from submitit (see `SlurmJob.done`).
    """
    slurm_job = _convert_job_info_to_slurm_job(job)
    return all(chunk_job.done(force_check=force_check) for chunk_job in slurm_job)
