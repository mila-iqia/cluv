import asyncio
from pathlib import Path
from typing import Literal

from cluv.cache import Job
from cluv.cli import login
from cluv.cli.submit import Submission, ensure_clean_git_state
from cluv.cli.sync import get_active_remotes
from cluv.remote import Remote
from cluv.utils import console, current_cluster

JobId = str
JobState = str


async def submit(
    cluster: str,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool,
    chunking: int | None,
) -> Job | None:
    """Submit a job to the given cluster (or all clusters if `cluster=="first"`),
    and return the Job object if successful.

    Returns None if the submission failed.
    """
    ensure_clean_git_state(autocommit=autocommit)

    cluster_to_remote = await get_cluster_to_remote(cluster)
    clusters = list(cluster_to_remote.keys())
    remotes = list(cluster_to_remote.values())

    cluster_submissions = {
        cluster: get_submissions(
            cluster,
            job_script=job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            chunking=chunking,
        )
        for cluster in clusters
    }
    job_state_table: dict[Submission, tuple[Job | JobSubmissionFailed | None, JobState]] = {
        job_submission: (None, "SYNCING")
        for submissions in cluster_submissions.values()
        for job_submission in submissions
    }

    await sync_common_part(remotes)

    _found_running_job = asyncio.Event()

    tasks = {
        cluster: asyncio.create_task(
            submit_to_cluster(
                cluster,
                remote=cluster_to_remote[cluster],
                submissions=submissions,
                jobs_state_table=job_state_table,
            )
        )
        for cluster, submissions in cluster_submissions.items()
    }

    # Wait either for one job to have started, or for all tasks to have completed and all job submissions to have failed.
    while True:
        if all(task.done() for task in tasks.values()) and not any(
            isinstance(job_or_exception, Job)
            for job_or_exception, _state in job_state_table.values()
        ):
            # All tasks are done, but no job has started, then all jobs have failed.
            console.log("All submitted jobs have failed! Exiting.")
            return None

        # Gather all jobs grouped by cluster
        _cluster_to_jobs = {
            cluster: [
                job_or_exception
                for job_or_exception, _state in job_state_table.values()
                if isinstance(job_or_exception, Job) and job_or_exception.cluster == cluster
            ]
            for cluster in clusters
        }
        # for cluster in clusters:

        await asyncio.sleep(1)


class JobSubmissionFailed(Exception):
    """Raised when a job submission fails."""


async def submit_to_cluster(
    cluster: str,
    remote: Remote | None,
    submissions: list[Submission],
    jobs_state_table: dict[Submission, tuple[Job | JobSubmissionFailed | None, JobState]],
) -> None:
    """Submit a job to the given cluster, and return the Job object if successful.

    Returns None if the submission failed.
    """
    await sync_per_cluster_part([remote])
    for submission in submissions:
        jobs_state_table[submission] = (None, "SUBMITTING")

    jobs_or_exceptions = await asyncio.gather(
        *(submit_job(submission) for submission in submissions),
        return_exceptions=True,
    )

    assert len(jobs_or_exceptions) == len(submissions)
    for submission, job_or_exception in zip(submissions, jobs_or_exceptions):
        if isinstance(job_or_exception, Job):
            jobs_state_table[submission] = (job_or_exception, "PENDING")
        elif isinstance(job_or_exception, JobSubmissionFailed):
            # job submission failed.
            # TODO: Redirect / Capture the stdout / stderr during syncing and job submission and display
            # a link to the file (in /tmp perhaps) instead.
            jobs_state_table[submission] = (
                job_or_exception,
                "FAILED: [link]<failure_stderr>[/link]",
            )
        else:
            raise job_or_exception


def get_submissions(
    cluster: str,
    *,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    chunking: int | None,
) -> list[Submission]:
    """Expand the possible job configurations for a cluster. Returns a list of `Submission` objects.

    Does *not* do the actual job submission with `sbatch`.
    """


async def submit_job(submission: Submission) -> Job:
    """Does the actual sbatch call.

    Raise a `JobSubmissionFailed` if the job submission fails for some reason.
    """
    ...


async def new_sync(clusters: list[str] | None = None):
    cluster_to_remote = await get_cluster_to_remote(clusters)
    remotes = list(cluster_to_remote.values())
    await sync_common_part(remotes)
    await asyncio.gather(*[sync_per_cluster_part(remote) for remote in remotes])


async def sync_common_part(cluster_remotes: list[Remote | None]): ...


async def sync_per_cluster_part(cluster_remote: Remote | None): ...


async def get_cluster_to_remote(
    cluster: Literal["first"] | str | list[str] | None,
) -> dict[str, Remote | None]:
    cluster_to_remote: dict[str, Remote | None] = {
        remote.hostname: remote for remote in (await get_active_remotes())
    }
    if here := current_cluster():
        cluster_to_remote[here] = None
    if cluster == "first" or cluster is None:
        return cluster_to_remote

    clusters = [cluster] if isinstance(cluster, str) else cluster
    missing_clusters = [c for c in clusters if c not in cluster_to_remote]
    if missing_clusters:
        remotes = await login(missing_clusters)
        assert remotes
        for remote in remotes:
            cluster_to_remote[remote.hostname] = remote

    return {cluster: cluster_to_remote[cluster] for cluster in clusters}
