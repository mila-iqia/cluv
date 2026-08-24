import asyncio
import dataclasses
import datetime
import itertools
import logging
import shlex
from pathlib import Path
from typing import Literal

from cluv.cli import login
from cluv.cli.submit import (
    ensure_clean_git_state,
)
from cluv.cli.sync import get_active_remotes
from cluv.config import SbatchArgs, get_cluv_config
from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, run_saccts
from cluv.utils import console, current_cluster

logger = logging.getLogger(__name__)
JobId = str
JobState = str


class JobSubmissionFailed(Exception):
    """Raised when a job submission fails."""


@dataclasses.dataclass(frozen=True, unsafe_hash=True, kw_only=True)
class Submission:
    """One job to submit: on which cluster, over which connection, and with which sbatch flags."""

    cluster: str

    remote: Remote | None
    """Remote used to run `sbatch`, or `None` to run it on the current cluster."""

    job_script: Path

    sbatch_args: SbatchArgs
    """The sbatch flags from the config to use, i.e. one of the cluster's allocations."""

    program_args: list[str]

    sbatch_command: str

    num_chunks: int | None


@dataclasses.dataclass(frozen=True, unsafe_hash=True, kw_only=True)
class Job(Submission):
    """A Job on a Slurm cluster. This object is returned by `cluv submit`."""

    job_id: int
    submitted_at: datetime.datetime


async def submit(
    cluster: str,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool,
    chunking: int | None,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit a job to the given cluster (or all clusters if `cluster=="first"`),
    and return the Job object if successful.

    Returns None if the submission failed.
    """
    git_commit = ensure_clean_git_state(autocommit=autocommit)

    cluster_to_remote = await get_cluster_to_remote(cluster)
    clusters = list(cluster_to_remote.keys())
    remotes = list(cluster_to_remote.values())

    cluster_submissions = {
        cluster: get_submissions(
            cluster,
            remote,
            job_script=job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            chunking=chunking,
            git_commit=git_commit,
        )
        for cluster, remote in cluster_to_remote.items()
    }
    job_state_table: dict[Submission, tuple[Job | JobSubmissionFailed | None, JobState]] = {
        job_submission: (None, "SYNCING")
        for submissions in cluster_submissions.values()
        for job_submission in submissions
    }

    if not _skip_sync:
        await sync_common_part(remotes)

    found_running_job = asyncio.Event()
    first_running_job: Job | None = None

    tasks = {
        cluster: asyncio.create_task(
            submit_to_cluster(
                cluster,
                remote=cluster_to_remote[cluster],
                submissions=submissions,
                jobs_state_table=job_state_table,
                found_running_job=found_running_job,
                _skip_sync=_skip_sync,
            )
        )
        for cluster, submissions in cluster_submissions.items()
    }

    delay = 1

    # Wait either for one job to have started, or for all tasks to have completed and all job submissions to have failed.
    while first_running_job is None:
        if all(task.done() for task in tasks.values()) and not any(
            isinstance(job_or_exception, Job)
            for job_or_exception, _state in job_state_table.values()
        ):
            # All tasks are done, but no job has started, then all job submissions have failed.
            console.log("All job submissions have failed! Exiting.")
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
        job_states = await asyncio.gather(
            *(
                run_saccts(cluster_to_remote[cluster], [job.job_id for job in cluster_jobs])
                for cluster, cluster_jobs in _cluster_to_jobs.items()
            )
        )

        all_jobs_failed_after_submission = True
        for job, job_state in zip(
            itertools.chain.from_iterable(_cluster_to_jobs.values()),
            itertools.chain.from_iterable(job_states),
        ):
            cluster = job.cluster
            # TODO: Update some table with the new state.
            submission_for_job = next(
                submission
                for submission in itertools.chain.from_iterable(cluster_submissions.values())
                if submission.cluster == cluster
                and submission.sbatch_command == job.sbatch_command
            )
            job_state_table[submission_for_job] = (job, job_state)

            logger.debug(f"Job {job.job_id} on cluster {job.cluster} is in state {job_state}.")

            if not job_state.startswith(tuple(FAILED_JOB_STATES)):
                all_jobs_failed_after_submission = False

            if job_state.startswith(("RUNNING", "COMPLETED")):
                console.log(
                    f"Job {job.job_id} on cluster {job.cluster} has started with state: {job_state}"
                )
                first_running_job = job
                found_running_job.set()
                break

        if all_jobs_failed_after_submission:
            console.log("All submitted jobs have failed! Exiting.")
            return None

        delay = min(delay * 2, 60)
        logger.debug(f"Waiting for {delay} seconds before checking job states again...")
        await asyncio.sleep(delay)

    assert first_running_job is not None
    assert found_running_job.is_set()

    jobs_to_cancel = [
        job_or_exception
        for job_or_exception, _state in job_state_table.values()
        if isinstance(job_or_exception, Job) and job_or_exception != first_running_job
    ]
    await wait_for_jobs_to_cancel(
        jobs_to_cancel,
        job_state_table=job_state_table,
    )

    await asyncio.sleep(1)


async def wait_for_jobs_to_cancel(
    jobs_to_cancel: list[Job],
    job_state_table: dict[Submission, tuple[Job | JobSubmissionFailed | None, JobState]],
    max_wait_time_seconds: int = 60,
) -> None:
    """Wait for all jobs except the first running job to be cancelled or completed."""
    jobs_to_watch = jobs_to_cancel.copy()

    # TODO: Run scancel for all those jobs, then wait for them to be cancelled.
    await run_scancel(jobs_to_watch)
    cluster_to_jobs = dict(
        [
            (remote, list(cluster_jobs))
            for remote, cluster_jobs in itertools.groupby(
                jobs_to_watch, key=lambda job: job.remote
            )
        ]
    )
    delay = 1
    while len(jobs_to_watch) > 0:
        job_states = await asyncio.gather(
            *(
                run_saccts(cluster_remote, [job.job_id for job in cluster_jobs])
                for cluster_remote, cluster_jobs in cluster_to_jobs.items()
            )
        )
        for jobs, job_states in zip(cluster_to_jobs.values(), job_states):
            for job, job_state in zip(jobs, job_states):
                submission_for_job = next(
                    submission
                    for submission in job_state_table.keys()
                    if submission.cluster == job.cluster
                    and submission.sbatch_command == job.sbatch_command
                )
                job_state_table[submission_for_job] = (job, job_state)
                logger.debug(f"Job {job.job_id} on cluster {job.cluster} is in state {job_state}.")

                if job_state.startswith(("CANCELLED", "COMPLETED")):
                    jobs_to_watch.remove(job)
                    logger.debug(
                        f"Job {job.job_id} on cluster {job.cluster} is in state {job_state}."
                    )
        delay = min(delay * 2, max_wait_time_seconds)
        await asyncio.sleep(delay)


async def run_scancel(jobs: list[Job]):
    if not jobs:
        return
    cluster_jobs = {
        remote: list(cluster_jobs)
        for remote, cluster_jobs in itertools.groupby(jobs, key=lambda job: job.remote)
    }

    async def cancel(remote: Remote | None, jobs: list[Job]):
        job_ids = [job.job_id for job in jobs]

        scancel_command = f"scancel {' '.join(map(str, job_ids))}"
        if remote:
            await remote.get_output(scancel_command, hide=True)
            if print:
                console.print(
                    f"Cancelled jobs {', '.join(map(str, job_ids))} on cluster {remote.hostname}."
                )
        else:
            await run(tuple(shlex.split(scancel_command)), hide=True)
            if print:
                console.print(
                    f"Cancelled jobs {', '.join(map(str, job_ids))} on the current cluster."
                )

    await asyncio.gather(
        *(
            cancel(cluster_remote, cluster_jobs)
            for cluster_remote, cluster_jobs in cluster_jobs.items()
        )
    )


async def submit_to_cluster(
    cluster: str,
    remote: Remote | None,
    submissions: list[Submission],
    jobs_state_table: dict[Submission, tuple[Job | JobSubmissionFailed | None, JobState]],
    found_running_job: asyncio.Event,
    _skip_sync: bool = False,
) -> None:
    """Submit a job to the given cluster, and return the Job object if successful.

    Returns None if the submission failed.
    """
    if not _skip_sync:
        await sync_per_cluster_part(remote)

    for submission in submissions:
        jobs_state_table[submission] = (None, "SUBMITTING")

    if found_running_job.is_set():
        # If a job has already started on another cluster, we don't need to submit more jobs.
        console.log(
            f"Skipping submission of jobs to cluster {cluster} because a job "
            f"has already started on another cluster."
        )
        for submission in submissions:
            jobs_state_table[submission] = (None, "SKIPPED")
        return

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
    remote: Remote | None,
    *,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    chunking: int | None,
    git_commit: str,
) -> list[Submission]:
    """Expand the possible job configurations for a cluster. Returns a list of `Submission` objects.

    Does *not* do the actual job submission with `sbatch`.
    """
    submissions: list[Submission] = []
    job_resources_options = get_cluv_config().get_cluster_config(cluster).sbatch_args
    config = get_cluv_config()
    if job_script is None:
        _job_script_str = config.job_script_path
        if _job_script_str is None:
            raise ValueError(
                f"No job script specified for cluster {cluster}, and no default job script path set in the config."
            )
        job_script = Path(_job_script_str)

    for job_resources in job_resources_options:
        job_sbatch_args: SbatchArgs = merge_sbatch_args(
            from_config=job_resources, from_cli=sbatch_args
        )
        num_chunks, job_sbatch_args = apply_chunking(
            job_sbatch_args, job_script=job_script, chunking=chunking
        )
        sbatch_command = get_sbatch_command(
            cluster,
            job_script=job_script,
            sbatch_args=job_sbatch_args,
            program_args=program_args,
            git_commit=git_commit,
        )
        submissions.append(
            Submission(
                cluster=cluster,
                remote=remote,
                job_script=job_script,
                sbatch_args=job_sbatch_args,
                program_args=program_args,
                sbatch_command=sbatch_command,
                num_chunks=num_chunks,
            )
        )
    return submissions


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: SbatchArgs,
    program_args: list[str],
    git_commit: str,
) -> str:
    raise NotImplementedError("TODO")


def apply_chunking(
    sbatch_args: SbatchArgs, job_script: Path | None, chunking: int | None
) -> tuple[int | None, SbatchArgs]:
    raise NotImplementedError("TODO")


def merge_sbatch_args(from_config: SbatchArgs, from_cli: list[str]) -> SbatchArgs:
    """Merge the sbatch args from the config and from the CLI, with CLI args taking precedence."""
    raise NotImplementedError("TODO")


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


async def sync_common_part(cluster_remotes: list[Remote | None]):
    raise NotImplementedError(
        "TODO: Do the `git push`, `pull_datasets`, etc, the sync steps that are common to all the clusters."
    )


async def sync_per_cluster_part(cluster_remote: Remote | None):
    raise NotImplementedError("TODO: Do the sync steps that are specific to each cluster.")


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
