from __future__ import annotations

import asyncio
import datetime
import functools
import logging
import os
import shlex
import subprocess
import sys
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import NamedTuple

import rich.syntax
import rich.table
import rich.text
from rich.live import Live

from cluv.cache import Job, save_job
from cluv.cli.submit_utils.chunking import chunking_update_sbatch_args, get_n_chunks
from cluv.cli.submit_utils.first import cancel_job
from cluv.cli.sync import (
    get_active_remotes,
    prepare_sync,
    print_new_runs,
    sync,
    sync_task_function,
)
from cluv.config import SbatchArgs, find_pyproject, get_cluv_config
from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct
from cluv.utils import console, console_lock, current_cluster

logger = logging.getLogger(__name__)

__all__ = ["submit"]
display_commands = ContextVar("display_commands", default=True)
raise_on_command_error = ContextVar("raise_on_command_error", default=False)


@dataclass(frozen=True)
class ResolvedSbatchArgs:
    """The resolved sbatch arguments after merging the config and the command-line args, and the
    external information we can infer from them, such as the number of chunks for chunked jobs
    when calling get_sbatch_command()."""

    sbatch_args: list[str]
    n_chunks: int | None = None


def sbatch_args_from_dict(d: SbatchArgs) -> list[str]:
    """Convert a dict of sbatch options to a list of command-line flags.

    Key-to-flag conversion:

    - multi-char key + non-empty string value → ``--key=value``
    - single-char key + non-empty string value → ``-k value`` (two separate args)
    - any key + ``True`` → bare flag (``--key`` or ``-k``)
    - any key + ``""`` or ``False`` → omitted entirely

    >>> sbatch_args_from_dict({"time": "2:00:00", "gpus": "1"})
    ['--time=2:00:00', '--gpus=1']
    >>> sbatch_args_from_dict({"exclusive": True})
    ['--exclusive']
    >>> sbatch_args_from_dict({"N": "2"})
    ['-N', '2']
    >>> sbatch_args_from_dict({"gpus": "", "requeue": False})
    []
    >>> sbatch_args_from_dict({"n": True})
    ['-n']
    """
    flags: list[str] = []
    for key, value in d.items():
        if value == "" or value is False:
            continue
        is_short_flag = len(key) == 1
        if value is True:
            flags.append(f"-{key}" if is_short_flag else f"--{key}")
        else:
            if is_short_flag:
                flags.extend([f"-{key}", str(value)])
            else:
                flags.append(f"--{key}={value}")
    return flags


class Submission(NamedTuple):
    """One job to submit: on which cluster, over which connection, and with which sbatch flags."""

    remote: Remote | None
    """Remote used to run `sbatch`, or `None` to run it on the current cluster."""

    job_script: Path

    sbatch_args: list[str]
    """The sbatch flags from the config to use, i.e. one of the cluster's allocations."""

    program_args: list[str]

    @property
    def cluster(self) -> str:
        """The cluster on which this job will be submitted."""
        if self.remote:
            return self.remote.hostname
        this_cluster = current_cluster()
        assert this_cluster
        return this_cluster


async def submit(
    cluster: str,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool = False,
    chunking: int | None = None,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets `GIT_COMMIT` and any
    environment variables configured in `[tool.cluv.env]` / `[tool.cluv.clusters.<name>.env]`,
    then calls `sbatch` on the remote.

    `sbatch_args` are forwarded as flags to `sbatch`; `program_args` are passed to
    the job script.

    When more than one allocation is configured for the target cluster (a list of flag sets in
    `[tool.cluv.clusters.<name>].sbatch_args`), one job is submitted per allocation and only the
    first one to start is kept, the others being cancelled.

    Parameters:
        cluster: SSH hostname of the target cluster. Can be set to "first" to launch the job on all clusters and keep only the first one to starts.
        job_script: Path to the job script to submit, relative to the project root.
            When omitted, uses the configured default for the target cluster.
        sbatch_args: List of additional flags to pass to `sbatch`.
        program_args: List of arguments to pass to the job script, for example `["python", "main.py"]`.
        autocommit: If True, automatically create a local commit with tracked changes before submitting.
        chunking: Duration in hours of each chunk when splitting the job into multiple consecutive
            short jobs. When None, chunking is disabled.
        _skip_sync: If True, skip the synchronization step before submitting.

    Returns:
        The job ID of the submitted job or None if the sbatch command fails.

    Examples:

    ```python
    submit(
        "mila",
        "scripts/job.sh",
        sbatch_args=["--time=00:00:10"],
        program_args=["python", "--version"],
    )
    ```
    """

    # Check git is clean locally (untracked files are fine) and capture current commit hash.
    git_commit = ensure_clean_git_state(
        autocommit=autocommit,
        submit_command=build_submit_command(
            cluster,
            job_script
            or (get_job_script_path_from_config(cluster) if cluster != "first" else "")
            or "<job_script>",
            sbatch_args,
            program_args,
        ),
    )

    here = current_cluster()

    if cluster == "first":
        job = await submit_first(
            job_script, sbatch_args, program_args, git_commit, chunking, _skip_sync=_skip_sync
        )
        if job:
            save_job(job)
        return job

    if job_script is None:
        job_script_from_config = get_job_script_path_from_config(cluster)
        job_script = _check_job_script_exists_locally(job_script_from_config, cluster)
    else:
        job_script = _check_job_script_exists_locally(job_script, cluster)

    if cluster != here:
        # The sbatch command will be run over SSH.
        if _skip_sync:
            remote = await Remote.connect(hostname=cluster)
        else:
            remote = (await sync(clusters=[cluster]))[0]
    else:
        # Submitting to the current cluster. The sbatch command will run locally.
        remote = None

    job_resources_options = get_cluv_config().get_cluster_config(cluster).sbatch_args
    if len(job_resources_options) > 1:
        # We can submit the job using different job configurations, for example
        # with different accounts, or different GPU models, etc.
        # Submit one job per allocation and keep the first one that starts.
        job = await submit_and_keep_first(
            [
                Submission(
                    remote=remote,
                    job_script=job_script,
                    sbatch_args=sbatch_args_from_dict(job_resources_config) + sbatch_args,
                    program_args=program_args,
                )
                for job_resources_config in job_resources_options
            ],
            git_commit=git_commit,
            chunking=chunking,
        )
        if job:
            save_job(job)
        return job

    sbatch_args_from_config = job_resources_options[0]
    sbatch_args = sbatch_args_from_dict(sbatch_args_from_config) + sbatch_args
    result, job = await sbatch(
        remote=remote,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        git_commit=git_commit,
        chunking=chunking,
    )

    if result.returncode != 0 or job is None:
        console.print(f"[red] Error during sbatch : {result.stderr}[/red]")
    else:
        save_job(job)

        console.log(
            f"Successfully submitted job {job.job_id} on the {cluster} cluster.\n"
            f"Use `ssh {cluster} sacct -j {job.job_id}` to view its status, and `cluv sync {cluster}` to "
            f"fetch results once it is complete."
        )

    return job


async def submit_first(
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    chunking: int | None = None,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit the job on all clusters (and on every allocation of each cluster), and wait until one
    of them starts. Once one starts, cancel the others.

    Each cluster syncs and submits on its own: a cluster's job is submitted as soon as *that* cluster
    is done syncing, without waiting for the other (possibly much slower) clusters. Clusters join the
    race as they become ready, and the ones that are still syncing when a job starts running give up
    instead of submitting.
    """
    # The steps that are shared by all the clusters (`git push`, pulling the datasets) still have to
    # happen before any cluster is synced.
    if not _skip_sync:
        remotes = await prepare_sync()
    else:
        remotes = await get_active_remotes()

    this_cluster = current_cluster()
    # `prepare_sync` / `get_active_remotes` don't return a Remote for the current cluster.
    assert not any(remote.hostname == this_cluster for remote in remotes)

    cluster_to_remote: dict[str, Remote | None] = {remote.hostname: remote for remote in remotes}
    if this_cluster is not None:
        # We are also on a Slurm cluster, so consider this as an option as well. `remote=None` means
        # that the commands are run here instead of over SSH.
        cluster_to_remote[this_cluster] = None

    race = {
        cluster: ClusterInRace(
            remote=remote,
            job_script=_check_job_script_exists_locally(
                job_script or get_job_script_path_from_config(cluster), cluster
            ),
        )
        for cluster, remote in cluster_to_remote.items()
    }

    max_wait_time_seconds = 5
    race_is_over = asyncio.Event()
    """Set once a job is running, so the clusters that are still syncing don't submit anything."""
    output_lock = asyncio.Lock()
    """Keeps each command and its output together in the console instead of interleaving them."""

    async def sync_then_submit(cluster: str) -> Job | None:
        """Syncs one cluster, then submits every allocation configured for it and keeps whichever
        job is kept (the only one, or the first of its allocations to start)."""
        # Tasks get their own copy of the context, so this doesn't affect the caller.
        console_lock.set(output_lock)
        entry = race[cluster]
        if entry.remote is not None and not _skip_sync:
            entry.status = "Syncing..."
            new_runs = await sync_task_function(
                report_progress=functools.partial(_report_sync_progress, entry),
                remote=entry.remote,
            )
            print_new_runs(cluster, new_runs)
        if race_is_over.is_set():
            entry.status = "Not submitted (another job already started)"
            return None
        entry.status = "Submitting..."

        # One submission per allocation configured for this cluster.
        allocations = get_cluv_config().get_cluster_config(cluster).sbatch_args
        cluster_submissions = [
            Submission(
                remote=entry.remote,
                job_script=entry.job_script,
                sbatch_args=sbatch_args_from_dict(allocation) + sbatch_args,
                program_args=program_args,
            )
            for allocation in allocations
        ]
        # Submit in a separate task, so that giving up on this cluster while `sbatch` is in flight
        # doesn't lose the job id (which we need in order to cancel that job).
        if len(cluster_submissions) == 1:
            entry.sbatch_task = asyncio.create_task(
                _submit_one(cluster, cluster_submissions[0], git_commit, chunking),
                name=f"sbatch-{cluster}",
            )
        else:
            # More than one allocation for this cluster: race them against each other the same way
            # `submit` races allocations for a single specified cluster, and keep only the winner.
            entry.sbatch_task = asyncio.create_task(
                submit_and_keep_first(
                    cluster_submissions, git_commit=git_commit, chunking=chunking
                ),
                name=f"sbatch-{cluster}",
            )
        return await asyncio.shield(entry.sbatch_task)

    submissions = {
        cluster: asyncio.create_task(sync_then_submit(cluster), name=f"submit-{cluster}")
        for cluster in race
    }

    # Wait for a job to start on a cluster.
    # If the wait is interrupted, cancel all jobs.
    first_running_job: JobHandle | None = None
    title = "Syncing and submitting the job on the clusters..."

    try:
        with Live(
            get_renderable=lambda: make_race_table(race, title),
            console=console,
            refresh_per_second=1,
        ) as live:
            first_running_job = await wait_for_running_job(
                race, submissions, max_wait_time_seconds
            )
            live.update(make_race_table(race, title), refresh=True)  # probably not necessary.
            if not first_running_job:
                console.log("No job could be submitted, or all submitted jobs failed! Exiting.")
                return None

            console.log(
                f"Job {first_running_job.job_id} on cluster {first_running_job.cluster} is "
                f"{first_running_job.state}. Cancelling the other jobs...\n",
            )
            title = "Waiting for jobs to cancel..."
            race_is_over.set()
            await stop_pending_submissions(race, submissions)
            await wait_for_jobs_to_cancel(race, first_running_job, max_wait_time_seconds)
            live.update(make_race_table(race, title), refresh=True)  # probably not necessary.

        console.print(
            f"Successfully cancelled all other jobs except for job {first_running_job.job_id} "
            f"on cluster {first_running_job.cluster}, which is {first_running_job.state}."
        )
        if first_running_job.state.startswith("RUNNING"):
            console.print(
                f"Use `ssh {first_running_job.cluster} sacct -j {first_running_job.job_id}` to view its status."
            )
            console.print(
                f"Once completed, run `cluv sync {first_running_job.cluster}` to fetch its results."
            )

    except (KeyboardInterrupt, asyncio.CancelledError, Exception):
        console.log("Interrupted by user. Cancelling all jobs...")
        race_is_over.set()
        await stop_pending_submissions(race, submissions)
        await asyncio.gather(
            *[
                cancel_job(entry.remote, entry.job.job_id, print=True)
                for cluster, entry in race.items()
                if entry.job is not None
                and (first_running_job is None or cluster != first_running_job.cluster)
            ]
        )
        return None

    winner = race[first_running_job.cluster]
    assert winner.job is not None
    return winner.job


async def _submit_one(
    cluster: str, submission: Submission, git_commit: str, chunking: int | None
) -> Job | None:
    """Submit a single allocation, logging (but not raising) if it fails."""
    result, job = await sbatch(
        submission.remote,
        job_script=submission.job_script,
        sbatch_args=submission.sbatch_args,
        program_args=submission.program_args,
        git_commit=git_commit,
        chunking=chunking,
    )
    if result.returncode != 0 or job is None:
        console.log(f"[red]Error during sbatch on {cluster}: {result.stderr.strip()}[/red]")
        return None
    return job


async def wait_for_running_job_by_cluster(
    job_to_state: dict[Job, str],
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> tuple[Job, str] | None:
    """Watch the jobs with sacct until one of them starts (or completes).

    Used by `submit_and_keep_first`, which submits all its jobs up front (unlike `submit_first`'s
    own race, whose clusters join progressively as they finish syncing).

    Returns the first job that starts with its state, or None if all jobs fail before any start.
    """
    first_running_job: Job | None = None
    wait_time = 1

    to_query = list(job_to_state.keys())

    while first_running_job is None and to_query:
        # Initial sleep after sbatch to give time for job to appear in sacct.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)

        job_states = await asyncio.gather(
            *(run_sacct(cluster_to_remote[job.cluster], job.job_id) for job in to_query)
        )

        for job, job_state in zip(to_query.copy(), job_states):
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


async def wait_for_jobs_to_cancel_by_cluster(
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
    console.print(
        f"Successfully cancelled all other jobs except for job {first_running_job.job_id} on "
        f"cluster {first_running_job.cluster}."
    )


async def submit_and_keep_first(
    submissions: list[Submission],
    git_commit: str,
    chunking: int | None = None,
) -> Job | None:
    """Submit all the given jobs, wait until one of them starts, then cancel the others."""

    sbatch_results = await asyncio.gather(
        *[
            sbatch(
                submission.remote,
                job_script=submission.job_script,
                sbatch_args=submission.sbatch_args,
                program_args=submission.program_args,
                git_commit=git_commit,
                chunking=chunking,
            )
            for submission in submissions
        ],
        return_exceptions=True,
    )

    # `sacct` and `scancel` are run on the cluster of the job, whatever its allocation.
    cluster_to_remote = {submission.cluster: submission.remote for submission in submissions}
    # Only show the allocation of each job when some cluster has more than one.
    show_allocations = len(submissions) > len(cluster_to_remote)

    job_to_state: dict[Job, str] = {}
    table = rich.table.Table(
        "Cluster",
        *(["sbatch arguments"] if show_allocations else []),
        "Result",
        title="Jobs submitted on the clusters",
    )

    for submission, sbatch_result in zip(submissions, sbatch_results):
        # Reconstruct the sbatch command that was used, just to display it (not great).
        sbatch_command, _sbatch_args = get_sbatch_command(
            cluster=submission.cluster,
            job_script=submission.job_script,
            sbatch_args=submission.sbatch_args,
            program_args=submission.program_args,
            git_commit=git_commit,
            chunking=chunking,
        )
        if isinstance(sbatch_result, BaseException):
            output_text = rich.text.Text(f"Error: {sbatch_result}", style="red")
        else:
            result, job = sbatch_result
            if result.returncode != 0 or job is None:
                output_text = rich.text.Text(f"Error: {result.stderr.strip()}", style="red")
            else:
                job_to_state[job] = "UNKNOWN"
                output_text = rich.text.Text(f"Job ID: {job.job_id}", style="green")
        table.add_row(
            submission.cluster,
            *([shlex.join(submission.sbatch_args)] if show_allocations else []),
            rich.console.Group(
                rich.syntax.Syntax(sbatch_command, lexer="sh", word_wrap=True),
                output_text,
            ),
            end_section=True,
        )
    console.print(table)

    if not job_to_state:
        console.print("No job submitted on clusters. See errors above.")
        return None

    # Wait for a job to start on a cluster.
    # If the wait is interrupted, cancel all jobs.
    first_running_job: Job | None = None

    max_wait_time_seconds = 5

    cancelling = False

    def make_table() -> rich.table.Table:
        table = rich.table.Table(
            "Cluster",
            "Job ID",
            "Status",
            title="Waiting for a job to start..."
            if not cancelling
            else "Waiting for jobs to cancel...",
        )
        for job, state in job_to_state.items():
            table.add_row(
                job.cluster,
                str(job.job_id),
                rich.text.Text(
                    state,
                    style="green"
                    if state.startswith(("RUNNING", "COMPLETED", "CANCELLED"))
                    else "yellow"
                    if state.startswith(("PENDING", "UNKNOWN"))
                    else "red",
                ),
            )
        return table

    job_to_state = {job: "UNKNOWN" for job in job_to_state}
    cluster_to_remote = {submission.cluster: submission.remote for submission in submissions}

    try:
        with Live(get_renderable=make_table, console=console, refresh_per_second=1) as live:
            wait_result = await wait_for_running_job_by_cluster(
                job_to_state, cluster_to_remote, max_wait_time_seconds
            )
            live.update(make_table(), refresh=True)  # probably not entirely necessary.
            if not wait_result:
                console.log("All submitted jobs have failed! Exiting.")
                return None

            first_running_job, first_state = wait_result

            console.log(
                f"Job {first_running_job.job_id} on cluster {first_running_job.cluster} is {first_state}. "
                f"Cancelling the other jobs...\n",
            )
            cancelling = True
            await wait_for_jobs_to_cancel_by_cluster(
                job_to_state,
                first_running_job,
                cluster_to_remote,
                max_wait_time_seconds,
            )
            live.update(make_table(), refresh=True)  # probably not entirely necessary.

    except (KeyboardInterrupt, asyncio.CancelledError, Exception):
        console.log("Interrupted by user. Cancelling all jobs...")
        to_cancel = list(job_to_state.keys())
        if first_running_job:
            to_cancel.remove(first_running_job)
        await asyncio.gather(
            *[
                cancel_job(cluster_to_remote[job.cluster], job.job_id, print=True)
                for job in to_cancel
            ]
        )
        return None

    return first_running_job


@dataclass(frozen=True)
class JobHandle:
    """Lightweight reference to a submitted job, used while it is racing against the others."""

    cluster: str
    job_id: int
    state: str


@dataclass
class ClusterInRace:
    """State of one of the clusters taking part in a `cluv submit first` race."""

    remote: Remote | None
    """Connection to that cluster. `None` when it is the cluster we are currently on."""

    job_script: Path
    """The job script to submit on that cluster."""

    status: str = "Waiting..."
    """What that cluster is currently doing. Displayed until its job shows up in `sacct`."""

    sbatch_task: asyncio.Task[Job | None] | None = None
    """The submission(s) for this cluster, kept apart so giving up on a slow sync doesn't lose the
    job (either a single `_submit_one` call, or a `submit_and_keep_first` race across allocations)."""

    job: Job | None = None
    """The job kept for that cluster (the only one, or the winner of its allocations)."""

    job_state: str | None = None
    """Last job state seen in `sacct` for that job."""


def _report_sync_progress(
    entry: ClusterInRace, progress: int, total: int, info: str | None = None
) -> None:
    """`ReportProgressFn` that shows the sync progress of a cluster in the race table."""
    entry.status = f"Syncing ({progress}/{total})" + (f": {info}" if info else "")


def make_race_table(race: dict[str, ClusterInRace], title: str) -> rich.table.Table:
    """Table showing what each cluster of the race is doing, and the state of its job (if any)."""
    table = rich.table.Table("Cluster", "Job ID", "Status", title=title)
    for cluster, entry in race.items():
        table.add_row(
            cluster,
            str(entry.job.job_id) if entry.job else "",
            rich.text.Text(entry.job_state, style=_job_state_style(entry.job_state))
            if entry.job_state
            else rich.text.Text(entry.status, style="dim"),
        )
    return table


def _job_state_style(job_state: str) -> str:
    if job_state.startswith(("RUNNING", "COMPLETED", "CANCELLED")):
        return "green"
    if job_state.startswith(("PENDING", "UNKNOWN")):
        return "yellow"
    return "red"


def _submitted_job(
    cluster: str, entry: ClusterInRace, submission: asyncio.Task[Job | None]
) -> Job | None:
    """Job kept by a finished `sync_then_submit` task, or `None` if it didn't submit.

    `_submit_one` / `submit_and_keep_first` already log the reason on a failed or losing submission,
    so there's nothing left to report here beyond an unexpected exception.
    """
    try:
        return submission.result()
    except asyncio.CancelledError:
        entry.status = "Cancelled"
        return None
    except Exception as err:
        entry.status = "Failed"
        console.log(f"[red]Unable to sync with or submit the job on {cluster}: {err}[/red]")
        return None


async def stop_pending_submissions(
    race: dict[str, ClusterInRace], submissions: dict[str, asyncio.Task[Job | None]]
) -> None:
    """Stops the clusters that are still syncing, now that the race is over.

    Giving up on a sync that is taking forever is the whole point of `submit first`. An `sbatch` that
    is already in flight is left to finish though, so that we learn the id of the job it queues and
    can cancel it, instead of leaving it there for the scheduler to eventually start.
    """
    for cluster, submission in submissions.items():
        if not submission.done():
            race[cluster].status = "Cancelled"
            submission.cancel()
    await asyncio.gather(*submissions.values(), return_exceptions=True)

    late_sbatch_tasks = {
        cluster: entry.sbatch_task
        for cluster, entry in race.items()
        if entry.job is None and entry.sbatch_task is not None
    }
    results = await asyncio.gather(*late_sbatch_tasks.values(), return_exceptions=True)
    for cluster, result in zip(late_sbatch_tasks, results):
        if isinstance(result, BaseException) or result is None:
            continue
        race[cluster].job = result
        race[cluster].job_state = "UNKNOWN"
        console.log(f"Job {result.job_id} was submitted on {cluster} just as the race ended.")


async def wait_for_running_job(
    race: dict[str, ClusterInRace],
    submissions: dict[str, asyncio.Task[Job | None]],
    max_wait_time_seconds: int = 60,
) -> JobHandle | None:
    """Watch the jobs with sacct until one of them starts (or completes).

    Clusters join the race as their `sync_then_submit` task completes, so the jobs that are already
    submitted are watched while the remaining clusters are still syncing.
    """
    pending = dict(submissions)
    to_query: list[tuple[str, int]] = []
    wait_time = 1

    while True:
        # Let the clusters that are done syncing and submitting join the race.
        for cluster in [cluster for cluster, task in pending.items() if task.done()]:
            entry = race[cluster]
            if job := _submitted_job(cluster, entry, pending.pop(cluster)):
                entry.job = job
                entry.job_state = "UNKNOWN"
                to_query.append((cluster, job.job_id))
                console.log(f"Submitted job {job.job_id} on the {cluster} cluster.")

        if not to_query:
            if not pending:
                # Every cluster either failed to submit, or its job failed.
                return None
            # Nothing was submitted (yet): wait for the next cluster to be done syncing.
            await asyncio.wait(pending.values(), return_when=asyncio.FIRST_COMPLETED)
            continue

        # Initial sleep after sbatch to give time for job to appear in sacct.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)

        job_states = await asyncio.gather(
            *(run_sacct(race[cluster].remote, job_id) for cluster, job_id in to_query)
        )

        for (cluster, job_id), job_state in zip(to_query.copy(), job_states):
            entry = race[cluster]
            if entry.job_state != job_state:
                console.print(
                    f"Job {job_id} on cluster {cluster}: {entry.job_state} -> {job_state}"
                )
            entry.job_state = job_state
            if job_state.startswith(("RUNNING", "COMPLETED")):
                return JobHandle(job_id=job_id, cluster=cluster, state=job_state)
            if job_state in FAILED_JOB_STATES:
                to_query.remove((cluster, job_id))


async def wait_for_jobs_to_cancel(
    race: dict[str, ClusterInRace],
    first_running_job: JobHandle,
    max_wait_time_seconds: int = 60,
) -> None:
    start_wait_time = 5
    to_cancel: list[tuple[str, int]] = [
        (cluster, entry.job.job_id)
        for cluster, entry in race.items()
        if entry.job is not None and cluster != first_running_job.cluster
    ]

    job_states = await asyncio.gather(
        *(run_sacct(race[cluster].remote, job_id) for cluster, job_id in to_cancel)
    )
    for (cluster, job_id), job_state in zip(to_cancel, job_states):
        logger.info(f"Job {job_id} on cluster {cluster} state: {job_state}")
        race[cluster].job_state = clean_job_state(job_state)

    to_cancel = [
        (cluster, job_id)
        for (cluster, job_id), job_state in zip(to_cancel, job_states)
        if not job_state.startswith(("CANCELLED", "COMPLETED"))
    ]

    logger.info(f"Need to cancel the following jobs: {to_cancel}")

    await asyncio.gather(
        *[cancel_job(race[cluster].remote, job_id, print=True) for cluster, job_id in to_cancel]
    )

    wait_time = min(start_wait_time, max_wait_time_seconds)

    while to_cancel:
        # Initial sleep after scancel to give time for job to be cancelled.
        await asyncio.sleep(wait_time)
        wait_time = min(wait_time * 2, max_wait_time_seconds)

        job_states = await asyncio.gather(
            *(run_sacct(race[cluster].remote, job_id) for cluster, job_id in to_cancel)
        )
        logger.debug(f"Job states: {job_states}")

        for (cluster, job_id), job_state in zip(to_cancel.copy(), job_states):
            logger.info(f"Job {job_id} on cluster {cluster} is in state: {job_state}")
            job_state = clean_job_state(job_state)  # just to avoid confusing users.
            if job_state == "FAILED":
                # Cheat slightly, but it's fine because this is usually just one of the job
                # steps that is marked "FAILED" in sacct on some clusters, while the others are
                # marked "CANCELLED". With "FAILED" in red, users might get a bit worried.
                job_state = "CANCELLED"
            race[cluster].job_state = job_state
            if job_state.startswith(("CANCELLED", "COMPLETED")) or job_state in FAILED_JOB_STATES:
                console.print(f"Job {job_id} on cluster {cluster} is now {job_state}.")
                to_cancel.remove((cluster, job_id))


def build_submit_command(
    cluster: str,
    job_script: str | Path | PurePosixPath,
    sbatch_args: list[str],
    program_args: list[str],
) -> str:
    """Build the local `cluv submit` command line used to launch the job."""
    command_parts = ["cluv", "submit"]
    command_parts.extend([cluster, str(job_script), *sbatch_args])
    if program_args:
        command_parts.extend(["--", *program_args])
    return shlex.join(command_parts)


def create_submit_commit(submit_command: str) -> None:
    """Create a commit with tracked changes and include the launched job command in the body."""
    try:
        subprocess.run(["git", "add", "-u"], check=True, capture_output=True, text=True)
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                "cluv submit: auto-commit tracked changes",
                "-m",
                f"Launched job command:\n\n{submit_command}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as err:
        error_text = (err.stderr or err.stdout or str(err)).strip()
        console.print(
            "[red]Failed to create automatic submit commit before job submission:[/red] "
            f"{error_text}"
        )
        raise


def ensure_clean_git_state(autocommit: bool = False, submit_command: str | None = None) -> str:
    """
    Check git is clean locally and return the current commit hash.
    """
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines:
        if autocommit:
            if submit_command is None:
                raise ValueError("submit_command is required when autocommit=True")
            create_submit_commit(submit_command)
        elif not (os.environ.get("SKIP_CLEAN_GIT_CHECK", "0") == "1"):
            console.print(
                "Working directory is dirty. Please commit your changes before submitting, "
                "or use `--autocommit` (`hydra.launcher.autocommit=True` when using Hydra).",
                style="red",
            )
            sys.exit(1)

    # In GitHub Actions PR jobs we can be on a detached merge commit that doesn't exist on
    # the synced remote checkout. Prefer the branch tip commit in that case.
    current_branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
    ).strip()
    if current_branch == "HEAD" and os.environ.get("GITHUB_ACTIONS"):
        github_head_ref = os.environ.get("GITHUB_HEAD_REF", "").strip()
        if github_head_ref:
            remote_head_ref = f"origin/{github_head_ref}"
            remote_head_result = subprocess.run(
                ["git", "rev-parse", "--verify", remote_head_ref],
                capture_output=True,
                text=True,
            )
            if remote_head_result.returncode == 0:
                return remote_head_result.stdout.strip()
            console.log(
                f"Could not resolve {remote_head_ref}. Falling back to local HEAD commit.",
                style="yellow",
            )

    # Capture current commit hash.
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def get_job_script_path_from_config(cluster: str) -> Path | PurePosixPath | None:
    job_script_path = get_cluv_config().get_cluster_config(cluster).job_script_path
    if cluster == current_cluster() and job_script_path is not None:
        # Resolve the path to the job script on the local machine.
        job_script_path = Path(os.path.expandvars(job_script_path))
        return job_script_path
    return job_script_path


def _check_job_script_not_none(
    job_script: Path | PurePosixPath | None | None, cluster: str
) -> Path | PurePosixPath:
    if job_script is None:
        raise ValueError(
            f"No job script was provided and no [tool.cluv] job_script_path is configured for {cluster}."
        )
    return job_script


def _check_job_script_exists_locally(
    job_script: Path | PurePosixPath | None, cluster: str
) -> Path:
    job_script = _check_job_script_not_none(job_script, cluster)
    job_script = Path(os.path.expandvars(job_script))
    if not job_script.exists():
        raise ValueError(
            f"The configured job_script value ({job_script}) does not exist on this machine.\n"
            f"The job script, even though it can be customized per cluster, needs to exist on "
            f"the local machine, because we need to read its header to infer the values of "
            f"sbatch arguments."
        )
    return job_script


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    chunking: int | None,
) -> tuple[str, ResolvedSbatchArgs]:
    """
    Generate the command to submit the job via sbatch on the remote cluster, with the appropriate
    sbatch_arguments, environment variables and paths set.

    NOTE: `sbatch_args` needs to already contain the sbatch arguments from the cluster config + command-line.
    """
    # Resolve remote job script path.
    local_job_script = job_script
    local_project_dir = find_pyproject().parent
    if not local_job_script.is_absolute():
        local_job_script = local_project_dir / local_job_script
    job_script_relative_path = local_job_script.relative_to(local_project_dir)

    # The project either has a project_dir set, or it is assumed to be under $HOME.
    remote_project_dir = get_cluv_config().get_cluster_config(cluster).project_dir or (
        PurePosixPath("$HOME") / local_project_dir.relative_to(Path.home())
    )
    remote_job_script = PurePosixPath(remote_project_dir) / job_script_relative_path

    # Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)
    env_vars: dict[str, str] = {**config.env}
    env_vars.update(cluster_config.env)

    # Prefix the job name with "cluv-" so it is easy to identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    in_job_packing = False
    assert not in_job_packing, "todo"
    # might contain unresolved env vars.
    cluster_results_path = PurePosixPath(cluster_config.results_path)
    n_chunks = None
    # TODO: Use the `get_run_id` function with the placeholder job id %j and task index %t:
    if chunking:
        assert not in_job_packing, "can't do both right now."
        env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%A/slurm-%A_%a.out"
        n_chunks = get_n_chunks(sbatch_args, env_vars, job_script, chunk_size=chunking)
        sbatch_args = chunking_update_sbatch_args(n_chunks, sbatch_args, chunk_size=chunking)
    elif not any("--output" in flag for flag in sbatch_args):
        if in_job_packing:
            env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%j_%t/slurm-%j_%t.out"
        else:
            env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%j/slurm-%j.out"
    output_from_cluv = env_vars.get("SBATCH_OUTPUT")
    if (
        output_from_file := next(
            (
                line
                for line in job_script.read_text().splitlines()
                if line.strip().startswith("#SBATCH") and "--output" in line
            ),
            None,
        )
    ) and output_from_file != output_from_cluv:
        logger.warning(
            UserWarning(
                f"[yellow]⚠️ The job script {job_script} contains an SBATCH --output directive "
                f"which will be overwritten by cluv, to facilitate the syncing of results.\n"
                f"Consider using cluv in your Python script to decide where to store results. "
                f"Take a look a the pytorch example of the Cluv repo for more info.[/yellow]"
            )
        )

    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = shlex.join(sbatch_args)
    program_args_str = shlex.join(program_args)

    sbatch_command = (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={remote_project_dir} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )

    return sbatch_command, ResolvedSbatchArgs(sbatch_args=sbatch_args, n_chunks=n_chunks)


async def sbatch(
    remote: Remote | None,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    chunking: int | None,
) -> tuple[subprocess.CompletedProcess[str], Job | None]:
    """Submit the job via sbatch on the remote cluster, and return the command output and the job info."""
    cluster = remote.hostname if remote else current_cluster()
    # Should be set, since `remote` is None if current_cluster() is the same as the cluster argument
    # to `submit`.
    assert cluster
    sbatch_command, resolved_args = get_sbatch_command(
        cluster=cluster,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        git_commit=git_commit,
        chunking=chunking,
    )

    display = display_commands.get()
    hide = not display
    warn = not raise_on_command_error.get()

    if remote:
        result = await remote.run(sbatch_command, display=display, warn=warn, hide=hide)
    else:
        # Run the sbatch command locally.
        result = await run(
            tuple(shlex.split(sbatch_command)), _display=display, warn=warn, hide=hide
        )

    submit_time = datetime.datetime.now()

    if result.returncode != 0:
        return result, None

    job = Job(
        job_id=int(result.stdout.strip()),
        cluster=cluster,
        job_script=str(job_script),
        git_commit=git_commit,
        sbatch_args=resolved_args.sbatch_args,
        program_args=program_args,
        submitted_at=submit_time.isoformat(),
        n_chunks=resolved_args.n_chunks,
    )

    return result, job
