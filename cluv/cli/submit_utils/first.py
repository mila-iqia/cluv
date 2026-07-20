import asyncio
import dataclasses
import logging
import shlex

from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct
from cluv.utils import console

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class JobHandle:
    cluster: str
    job_id: int
    state: str


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
    cluster_and_jobid_to_jobstate: dict[tuple[str, int], str],
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> JobHandle | None:
    """Watch the jobs with sacct until one of them starts (or completes)."""

    first_running_job: JobHandle | None = None
    wait_time = 1

    to_query = list(cluster_and_jobid_to_jobstate.keys())

    while first_running_job is None and to_query:
        # Initial sleep after sbatch to give time for job to appear in sacct.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)

        job_states = await asyncio.gather(
            *(run_sacct(cluster_to_remote[cluster], job_id) for cluster, job_id in to_query)
        )

        for (cluster, job_id), job_state in zip(to_query.copy(), job_states):
            if (previous_state := cluster_and_jobid_to_jobstate[(cluster, job_id)]) != job_state:
                console.print(
                    f"Job {job_id} on cluster {cluster}: {previous_state} -> {job_state}"
                )
            cluster_and_jobid_to_jobstate[(cluster, job_id)] = job_state
            if job_state.startswith(("RUNNING", "COMPLETED")):
                return JobHandle(job_id=job_id, cluster=cluster, state=job_state)
            if job_state in FAILED_JOB_STATES:
                to_query.remove((cluster, job_id))
    # If all failed, `cluster_and_jobid_to_jobstate` is empty.
    assert not to_query
    return None


async def wait_for_jobs_to_cancel(
    cluster_and_jobid_to_jobstate: dict[tuple[str, int], str],
    first_running_job: JobHandle,
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> JobHandle | None:
    start_wait_time = 5
    to_cancel = list(cluster_and_jobid_to_jobstate.keys())
    to_cancel.remove((first_running_job.cluster, first_running_job.job_id))

    job_states = await asyncio.gather(
        *(run_sacct(cluster_to_remote[cluster], job_id) for cluster, job_id in to_cancel)
    )
    for (cluster, job_id), job_state in zip(to_cancel, job_states):
        logger.info(f"Job {job_id} on cluster {cluster} state: {job_state}")
        job_state = clean_job_state(job_state)
        cluster_and_jobid_to_jobstate[(cluster, job_id)] = job_state

    to_cancel = [
        (cluster, job_id)
        for (cluster, job_id), job_state in zip(to_cancel, job_states)
        if not job_state.startswith(("CANCELLED", "COMPLETED"))
    ]

    logger.info(f"Need to cancel the following jobs: {to_cancel}")

    await asyncio.gather(
        *[
            cancel_job(cluster_to_remote[cluster], job_id, print=True)
            for cluster, job_id in to_cancel
        ]
    )

    wait_time = min(start_wait_time, max_wait_time_seconds)

    while not all(
        cluster_and_jobid_to_jobstate[cluster_jobid].startswith(
            tuple(["CANCELLED"] + FAILED_JOB_STATES)
        )
        for cluster_jobid in to_cancel.copy()
    ):
        # Initial sleep after scancel to give time for job to be cancelled.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)

        job_states = await asyncio.gather(
            *(run_sacct(cluster_to_remote[cluster], job_id) for cluster, job_id in to_cancel)
        )
        logger.debug(f"Job states: {job_states}")

        for (cluster, job_id), job_state in zip(to_cancel, job_states):
            logger.info(f"Job {job_id} on cluster {cluster} is in state: {job_state}")
            if job_state.startswith("CANCELLED by"):
                job_state = "CANCELLED"  # just to avoid confusing users.
            if job_state == "FAILED":
                # Cheat slightly, but it's fine because this is usually just one of the job
                # steps that is marked "FAILED" in sacct on some clusters, while the others are
                # marked "CANCELLED". With "FAILED" in red, users might get a bit worried.
                job_state = "CANCELLED"
            cluster_and_jobid_to_jobstate[(cluster, job_id)] = job_state
            if job_state.startswith(("CANCELLED", "COMPLETED")):
                console.print(f"Job {job_id} on cluster {cluster} is now {job_state}.")
                to_cancel.remove((cluster, job_id))
                # TODO: Do we remove the jobs from the table if they failed?
                # Also remove from `cluster_to_jobid` so the ctrl+c handler below doesn't
                # try to cancel it again.
                # cluster_and_jobid_to_jobstate.pop((cluster, job_id))
    console.print(
        f"Successfully cancelled all other jobs except for job {first_running_job.job_id} on "
        f"cluster {first_running_job.cluster}."
    )
