import asyncio
import logging
import shlex

from cluv.cache import Job
from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct
from cluv.utils import console

logger = logging.getLogger(__name__)


async def cancel_job(remote: Remote | None, job_id: int, print: bool = False) -> str:
    """Cancel the job with the given id on the remote cluster."""
    scancel_command = f"scancel {job_id}"
    if remote:
        output = await remote.get_output(scancel_command, hide=True)
        if print:
            console.print(f"Cancelled job {job_id} on cluster {remote.hostname}.")
    else:
        result = await run(tuple(shlex.split(scancel_command)), hide=True)
        if print:
            console.print(f"Cancelled job {job_id} on the current cluster.")
        output = result.stdout
    return output


async def wait_for_running_job(
    job_to_state: dict[Job, str],
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> tuple[Job, str] | None:
    """Watch the jobs with sacct until one of them starts (or completes).
    Returns the first job that starts with its state, or None if all jobs fail before any start.

    NOTE: modifies `job_to_state` in-place.
    """
    first_running_job: Job | None = None
    wait_time = 1
    to_query = None
    while first_running_job is None and (to_query := list(job_to_state.keys())):
        # Initial sleep after sbatch to give time for job to appear in sacct.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)
        jobs_by_cluster: dict[str, list[Job]] = {}
        for job in to_query:
            jobs_by_cluster.setdefault(job.cluster, []).append(job)

        cluster_states = await asyncio.gather(
            *(
                run_sacct(
                    cluster_to_remote[cluster],
                    ",".join(str(job.job_id) for job in jobs),
                )
                for cluster, jobs in jobs_by_cluster.items()
            )
        )
        job_states_by_job = {
            job: state
            for jobs, states in zip(jobs_by_cluster.values(), cluster_states)
            for job, state in zip(jobs, states.splitlines())
        }

        for job in to_query.copy():
            job_state = job_states_by_job[job]
            if (previous_state := job_to_state[job]) != job_state:
                console.print(
                    f"Job {job.job_id} on cluster {job.cluster}: {previous_state} -> {job_state}"
                )

            job_to_state[job] = job_state

            if job_state.startswith(("RUNNING", "COMPLETED")):
                return job, job_state
            if job_state in FAILED_JOB_STATES:
                to_query.remove(job)

    # If all failed, `job_to_state` is empty.
    assert not to_query
    return None


async def wait_for_jobs_to_cancel(
    job_to_state: dict[Job, str],
    first_running_job: Job,
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> None:
    """Wait for all jobs except the first running job to be cancelled or completed."""
    start_wait_time = 5
    to_cancel = list(job_to_state.keys())
    to_cancel.remove(first_running_job)

    job_states = await asyncio.gather(
        *(run_sacct(cluster_to_remote[job.cluster], job.job_id) for job in to_cancel)
    )
    for job, job_state in zip(to_cancel, job_states):
        logger.info(f"Job {job.job_id} on cluster {job.cluster} state: {job_state}")
        job_state = clean_job_state(job_state)
        job_to_state[job] = job_state

    to_cancel = [
        job
        for job, job_state in zip(to_cancel, job_states)
        if not job_state.startswith(("CANCELLED", "COMPLETED"))
    ]

    logger.info(f"Need to cancel the following jobs: {to_cancel}")

    await asyncio.gather(
        *[cancel_job(cluster_to_remote[job.cluster], job.job_id, print=True) for job in to_cancel]
    )

    wait_time = min(start_wait_time, max_wait_time_seconds)

    while not all(
        job_to_state[cluster_jobid].startswith(tuple(["CANCELLED"] + FAILED_JOB_STATES))
        for cluster_jobid in to_cancel.copy()
    ):
        # Initial sleep after scancel to give time for job to be cancelled.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)

        job_states = await asyncio.gather(
            *(run_sacct(cluster_to_remote[job.cluster], job.job_id) for job in to_cancel)
        )
        logger.debug(f"Job states: {job_states}")

        for job, job_state in zip(to_cancel, job_states):
            logger.info(f"Job {job.job_id} on cluster {job.cluster} is in state: {job_state}")
            if job_state.startswith("CANCELLED by"):
                job_state = "CANCELLED"  # just to avoid confusing users.
            if job_state == "FAILED":
                # Cheat slightly, but it's fine because this is usually just one of the job
                # steps that is marked "FAILED" in sacct on some clusters, while the others are
                # marked "CANCELLED". With "FAILED" in red, users might get a bit worried.
                job_state = "CANCELLED"
            job_to_state[job] = job_state
            if job_state.startswith(("CANCELLED", "COMPLETED")):
                console.print(f"Job {job.job_id} on cluster {job.cluster} is now {job_state}.")
                to_cancel.remove(job)
                # TODO: Do we remove the jobs from the table if they failed?
                # Also remove from `cluster_to_jobid` so the ctrl+c handler below doesn't
                # try to cancel it again.
                # cluster_and_jobid_to_jobstate.pop((cluster, job_id))
    console.print(
        f"Successfully cancelled all other jobs except for job {first_running_job.job_id} on "
        f"cluster {first_running_job.cluster}."
    )
