import asyncio
import contextlib
import dataclasses
import datetime
import itertools
import logging
import os
import re
import shlex
import subprocess
import sys
import traceback
import typing
from contextvars import ContextVar
from pathlib import Path, PurePosixPath
from typing import Generic, TypeVar

import rich.box
import rich.table
import rich.text
from rich.live import Live

from cluv.cache import Job, Submission, get_submission_log_dir, save_job
from cluv.cli.submit_utils.chunking import apply_chunking
from cluv.cli.sync import (
    get_cluster_to_remote,
    sync_common_part,
    sync_per_cluster_part,
)
from cluv.config import ClusterConfig, find_pyproject, get_cluv_config
from cluv.remote import Remote, command_log_files, run
from cluv.sbatch_args import SbatchArgs, sbatch_args_from_list, sbatch_args_to_list
from cluv.slurm import FAILED_JOB_STATES, run_saccts
from cluv.utils import console, gather_dict, group_by_cluster, set_context

logger = logging.getLogger(__name__)

__all__ = ["submit"]

display_commands = ContextVar("display_commands", default=True)
raise_on_command_error = ContextVar("raise_on_command_error", default=False)

JobState = str


class ClusterSyncFailed(Exception):
    """Raised when syncing with a cluster fails."""


class JobSubmissionFailed(Exception):
    """Raised when a job submission fails."""


JobSubmission = TypeVar("JobSubmission", Submission, Job)


@dataclasses.dataclass
class SubmissionProgress(Generic[JobSubmission]):
    """Live, mutable tracking of one `Submission`'s progress into becoming a `Job`.

    Tracks a submission from before it's even synced (``state="SYNCING"``, no `job` yet),
    through submission (``job`` known, state polled from `sacct`), to running and, possibly,
    cancellation.

    A single flat list of these -- covering every submission, on every cluster, for one
    `submit()` call -- is all a live display needs to render the whole picture, from a plain
    `rich.Live` table (see `render_job_table` below) up to, eventually, a table shared across
    several concurrent `submit`/`submit_first` calls (`rich.Live` only supports one live region
    per console, so that would fuse several such lists together instead of replacing this one).
    """

    job: JobSubmission
    log_path: Path
    """Where this submission's `sbatch` output will be written."""
    state: JobState = "SYNCING"
    error: ClusterSyncFailed | JobSubmissionFailed | None = None

    @property
    def cluster(self) -> str:
        return self.job.cluster

    @property
    def job_id(self) -> int | None:
        return self.job.job_id if isinstance(self.job, Job) else None


def has_job(submission_progress: SubmissionProgress) -> typing.TypeGuard[SubmissionProgress[Job]]:
    return isinstance(submission_progress.job, Job)


def _state_style(state: JobState) -> str:
    if state.startswith(("RUNNING", "COMPLETED", "CANCELLED")):
        return "green"
    if state.startswith(("SYNCING", "SUBMITTING", "PENDING", "UNKNOWN")):
        return "yellow"
    return "red"


def _short_command(submission: Submission) -> str:
    """A compact stand-in for `submission.sbatch_command`, e.g.
    ``bash --login -c '(...) --time=01:00:00 -- python main.py --lr=0.1'``.

    The full command repeats a lot of boilerplate (env vars, `--chdir=`, the job script path)
    across every cluster and allocation in one `submit()` call, none of which is usually what
    someone glancing at the table wants to see -- `(...)` stands in for all of that. What's
    shown instead is exactly the two things that vary and actually matter: the sbatch flags
    (resources requested) and the program args (what's actually being run).
    """
    sbatch_flags = shlex.join(sbatch_args_to_list(submission.sbatch_args))
    program_args_str = shlex.join(submission.program_args)
    return f"bash --login -c '(...) {sbatch_flags} -- {program_args_str}'"


def _command_and_log_cell(row: SubmissionProgress) -> rich.text.Text:
    """The command cell: the (short) command, with the log path as a clickable link (in
    terminals that support it) on its own line below."""
    cell = rich.text.Text(_short_command(row.job))
    cell.append("\nlog: ", style="dim")
    cell.append(str(row.log_path), style=f"dim link file://{row.log_path}")
    return cell


def render_job_table(
    cluster_to_job_submissions: dict[str, list[SubmissionProgress]], *, cancelling: bool = False
) -> rich.table.Table:
    """Render the current state of every submission as a single table.

    A plain `rich.Live` for now; the natural place to plug in a registry that fuses several
    concurrent `submit`/`submit_first` calls' rows into one shared live region later on.
    """
    title = "Waiting for jobs to cancel..." if cancelling else "Submitting jobs..."
    table = rich.table.Table(
        "Cluster",
        "Job ID",
        "Status",
        "Command",
        title=title,
        box=rich.box.ROUNDED,
        show_lines=True,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
        expand=True,
    )
    for cluster_name, cluster_jobs in cluster_to_job_submissions.items():
        for job_row in cluster_jobs:
            assert job_row.cluster == cluster_name
            table.add_row(
                job_row.cluster,
                str(job_row.job_id) if job_row.job_id is not None else "-",
                rich.text.Text(job_row.state, style=_state_style(job_row.state)),
                _command_and_log_cell(job_row),
            )
    return table


async def submit(
    cluster: str,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool = False,
    chunking: int | None = None,
    _skip_sync: bool = False,
    sync_datasets: bool = True,
    parsable: bool = False,
) -> Job | None:
    """Submit a job to the given cluster (or all clusters if `cluster=="first"`),
    and return the Job object if successful.

    If `parsable` is True, print only the job ID (or '<cluster>:<job_id>' when `cluster`
    is 'first') to stdout, for programmatic use, instead of the usual human-readable summary.
    Everything else (logs, the live jobs table, command outputs) is silenced, as with `--quiet`.

    Returns None if the submission failed.
    """
    # `--parsable` promises that stdout carries nothing but the job id. `--quiet` already suppresses
    # everything the submission writes through the shared console (and the raw command outputs in
    # `cluv.remote.run`, which check `console.quiet` too), so it is all this needs to do.
    if parsable:
        console.quiet = True
    submit_command = build_submit_command(
        cluster=cluster, job_script=job_script, sbatch_args=sbatch_args, program_args=program_args
    )
    git_commit = ensure_clean_git_state(autocommit=autocommit, submit_command=submit_command)
    cluster_to_remote = await get_cluster_to_remote(cluster)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = get_submission_log_dir()

    # Dict from cluster_name to *potential* job submissions.
    # They get actually converted into `Jobs` later when we actually sbatch them.
    _cluster_to_submissions = {
        cluster_name: get_submissions(
            cluster=cluster_name,
            remote=remote,
            job_script=job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            chunking=chunking,
            git_commit=git_commit,
        )
        for cluster_name, remote in cluster_to_remote.items()
    }

    # Wrap the submissions in a mutable dataclass where we will modify the `state` and maybe set the `job` fields.
    cluster_to_job_submissions = {
        cluster_name: [
            SubmissionProgress(
                job=submission, log_path=log_dir / f"{timestamp}_{cluster_name}_{i}.txt"
            )
            for i, submission in enumerate(cluster_jobs)
        ]
        for cluster_name, cluster_jobs in _cluster_to_submissions.items()
    }
    # Create the log files right away, so the links in the jobs table always point somewhere.
    all_log_paths = tuple(
        row.log_path for rows in cluster_to_job_submissions.values() for row in rows
    )
    for rows in cluster_to_job_submissions.values():
        for row in rows:
            row.log_path.write_text(
                f"cluster: {row.cluster}\ncommand: {row.job.sbatch_command}\n\n--- sync ---\n"
            )

    if not _skip_sync:
        remotes = [r for r in cluster_to_remote.values() if r]
        with logging_commands_to(all_log_paths):
            await sync_common_part(remotes, sync_datasets=sync_datasets)

    found_running_job = asyncio.Event()

    cancelling = False

    def _render() -> rich.table.Table:
        return render_job_table(cluster_to_job_submissions, cancelling=cancelling)

    try:
        with Live(get_renderable=_render, console=console, refresh_per_second=1) as live:
            winning_job = await wait_for_first_running_job(
                cluster_to_job_submissions,
                cluster_to_remote=cluster_to_remote,
                found_running_job=found_running_job,
                _skip_sync=_skip_sync,
                sync_datasets=sync_datasets,
            )
            if winning_job is None:
                console.log("All job submissions have failed! Exiting.")
                return None

            for _cluster, cluster_jobs in cluster_to_job_submissions.items():
                for job in cluster_jobs:
                    if job is not winning_job and not has_job(job) and job.state == "SYNCING":
                        job.state = "SKIPPED"

            cancelling = True

            jobs_to_cancel = [
                job_submission
                for _cluster, cluster_jobs in cluster_to_job_submissions.items()
                for job_submission in cluster_jobs
                if job_submission is not winning_job and has_job(job_submission)
            ]
            if jobs_to_cancel:
                console.log("Jobs to cancel:")
                for job in jobs_to_cancel:
                    console.log(f"  {job.job_id} on {job.cluster}")
            await wait_for_jobs_to_cancel(jobs_to_cancel, cluster_to_remote)
            live.refresh()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # The user stopped `cluv submit` while jobs were still in flight -- cancel everything.
        console.log("Interrupted by user. Cancelling all submitted jobs...")
        all_jobs = list(itertools.chain.from_iterable(cluster_to_job_submissions.values()))
        await run_scancel(all_jobs)
        raise

    job = winning_job.job
    assert job is not None

    if parsable:
        print(f"{job.cluster}:{job.job_id}" if cluster == "first" else job.job_id)
    else:
        console.print(
            f"Successfully submitted job {job.job_id} on cluster {job.cluster}.\n"
            f"Use `ssh {job.cluster} sacct -j {job.job_id}` to view its status, and `cluv sync"
            f" {job.cluster}` to fetch results once it is complete.",
            style="green",
        )

    save_job(job)
    return job


async def wait_for_first_running_job(
    cluster_to_job_submissions: dict[str, list[SubmissionProgress[Submission]]],
    *,
    cluster_to_remote: dict[str, Remote | None],
    found_running_job: asyncio.Event,
    _skip_sync: bool,
    sync_datasets: bool,
    initial_delay: int = 10,
    max_wait_time_seconds: int = 60,
) -> SubmissionProgress[Job] | None:
    """Poll `sacct` on each cluster until one submitted job starts running, or every submission has failed.

    Mutates the job submissions in place with the latest known job id / state, so a live display
    can render them at any point during this wait. Sets `found_running_job` the moment a job starts,
    so that clusters which haven't submitted their own jobs yet can skip doing so.

    Returns the submission for the job that started, or None if every submission ended up failing.
    """
    # Need one remote for each cluster that has job submissions.
    assert set(cluster_to_remote.keys()) >= set(cluster_to_job_submissions.keys())
    all_submissions = list(itertools.chain.from_iterable(cluster_to_job_submissions.values()))

    # Errors are reported (and stored on the submissions) by `sync_and_submit_jobs_to_cluster`.
    sync_and_submit = asyncio.gather(
        *(
            sync_and_submit_jobs_to_cluster(
                cluster_name,
                remote=cluster_to_remote[cluster_name],
                job_submissions=cluster_job_submissions,
                found_running_job=found_running_job,
                _skip_sync=_skip_sync,
                sync_datasets=sync_datasets,
            )
            for cluster_name, cluster_job_submissions in cluster_to_job_submissions.items()
        ),
        return_exceptions=True,
    )
    delay = initial_delay
    try:
        while True:
            # Read both before awaiting, so no job can get submitted in between.
            submitted_everywhere = sync_and_submit.done()
            queued_jobs = [s for s in all_submissions if has_job(s)]

            await asyncio.gather(
                *(
                    update_job_states_with_sacct(cluster_to_remote[cluster], cluster_jobs)
                    for cluster, cluster_jobs in group_by_cluster(queued_jobs).items()
                )
            )
            logger.debug("Job states: %s", {(j.cluster, j.job_id): j.state for j in queued_jobs})

            if started_jobs := (
                [j for j in queued_jobs if j.state == "COMPLETED"]
                or [j for j in queued_jobs if j.state == "RUNNING"]
            ):
                found_running_job.set()
                logger.debug(f"Found {len(started_jobs)} running (or completed) jobs.")
                return started_jobs[0]

            if submitted_everywhere:
                if all(j.state.startswith(tuple(FAILED_JOB_STATES)) for j in queued_jobs):
                    console.print(
                        f"Submitted {len(all_submissions)} jobs but none succeeded!", style="red"
                    )
                    return None
                # Skip the wait if we'd only be waiting on one job to start.
                pending_jobs = [j for j in queued_jobs if j.state == "PENDING"]
                if len(pending_jobs) == 1:
                    console.log("Only one job pending. Skipping wait for a running job.")
                    return pending_jobs[0]

            await asyncio.sleep(delay)
            delay = min(delay * 2, max_wait_time_seconds)
    finally:
        sync_and_submit.cancel()


async def update_job_states_with_sacct(
    remote: Remote | None, jobs: list[SubmissionProgress[Job]]
) -> None:
    job_states = await run_saccts(remote, [job.job.job_id for job in jobs])
    for job, state in zip(jobs, job_states):
        job.state = state.strip().split()[0]  # keep only the first word of the state?


async def wait_for_jobs_to_cancel(
    job_submissions: list[SubmissionProgress[Job]],
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
    initial_delay: int = 1,
) -> None:
    """Cancel every (already-submitted) job in `rows`, and wait until they're all done."""
    cluster_to_job_submissions = group_by_cluster(job_submissions)

    assert all(job.job_id is not None for job in job_submissions)
    cluster_to_job_ids = {
        cluster: [job.job_id for job in cluster_jobs if job.job_id is not None]
        for cluster, cluster_jobs in cluster_to_job_submissions.items()
    }
    delay = initial_delay

    while not all(
        job.state.startswith(("CANCELLED", "COMPLETED", "FAILED")) for job in job_submissions
    ):
        for job in job_submissions:
            try:
                await run_scancel([job])
            except Exception as err:
                logging.debug(f"Error running scancel for job {job.job_id}: {err}")
        job_states = await gather_dict(
            {
                cluster: run_saccts(cluster_to_remote[cluster], cluster_job_ids)
                for cluster, cluster_job_ids in cluster_to_job_ids.items()
            }
        )
        for cluster, cluster_jobs in cluster_to_job_submissions.items():
            for job, state in zip(cluster_jobs, job_states[cluster]):
                job.state = state.strip().split()[0]  # keep only the first word of the state.

        await asyncio.sleep(delay)
        delay = min(delay * 2, max_wait_time_seconds)
    console.log(f"Cancelled {len(job_submissions)} job submission(s).")
    for job_submission in job_submissions:
        console.log(
            f"Job {job_submission.job_id} on custer {job_submission.cluster} that was in state: {job_submission.state}"
        )


async def run_scancel(jobs: list[SubmissionProgress]) -> None:
    """Cancel the (already-submitted) jobs behind `rows`, grouped by remote."""
    if not jobs:
        return
    by_remote: dict[Remote | None, list[SubmissionProgress]] = {}
    for job in jobs:
        if job.job_id is not None:
            by_remote.setdefault(job.job.remote, []).append(job)

    async def cancel(remote: Remote | None, cluster_rows: list[SubmissionProgress]) -> None:
        job_ids = [job.job_id for job in cluster_rows if job.job_id is not None]
        scancel_command = f"scancel {' '.join(map(str, job_ids))}"
        if remote is not None:
            await remote.get_output(scancel_command, hide=True)
        else:
            await run(tuple(shlex.split(scancel_command)), hide=True)

    await asyncio.gather(
        *(
            cancel(remote, cluster_rows)
            for remote, cluster_rows in by_remote.items()
            if cluster_rows
        ),
        return_exceptions=True,  # so we don't stop all the cancels if one fails.
    )


async def sync_and_submit_jobs_to_cluster(
    cluster: str,
    remote: Remote | None,
    job_submissions: list[SubmissionProgress[Submission]],
    found_running_job: asyncio.Event,
    _skip_sync: bool = False,
    sync_datasets: bool = True,
) -> list[SubmissionProgress[Job]]:
    """Sync then submit every submission for one cluster, in parallel.

    NOTE: This modifies the job submissions in-place.

    Returns the job submissions that were successfully submitted and now have a job id.
    """
    if found_running_job.is_set():
        # If a job has already started on another cluster, we don't need to submit more jobs.
        # NOTE: (@lebrice) I'm not sure if this case can be encountered in practice, given my limited
        # knowledge of how asyncio actually works.
        console.log(
            f"Skipping syncing with cluster {cluster} because a job "
            f"has already started on another cluster."
        )
        for job_submission in job_submissions:
            job_submission.state = "SKIPPED"
        return []

    if not _skip_sync:
        try:
            with logging_commands_to(tuple(row.log_path for row in job_submissions)):
                await sync_per_cluster_part(remote, sync_datasets=sync_datasets)
        except Exception as exc:
            console.log(f"Failed to sync with cluster {cluster}: {exc}")
            for job_submission in job_submissions:
                # todo: Not sure if this is the right way to do this.
                job_submission.error = ClusterSyncFailed().with_traceback(exc.__traceback__)
                job_submission.state = "FAILED (unable to sync)"
            raise ClusterSyncFailed() from exc

    if found_running_job.is_set():
        # If a job has already started on another cluster, we don't need to submit more jobs.
        console.log(
            f"Skipping submission of jobs to cluster {cluster} because a job "
            f"has already started on another cluster."
        )
        for job_submission in job_submissions:
            job_submission.state = "SKIPPED"
        return []

    for job_submission in job_submissions:
        job_submission.state = "SUBMITTING"

    results = await asyncio.gather(
        *(submit_job(row.job, row.log_path) for row in job_submissions),
        return_exceptions=True,
    )

    assert len(results) == len(job_submissions)
    successful_job_submissions: list[SubmissionProgress[Job]] = []
    for job_submission, result in zip(job_submissions, results):
        if isinstance(result, Job):
            job_submission.job = result
            job_submission.state = "PENDING"
            assert has_job(job_submission)
            successful_job_submissions.append(job_submission)
        elif isinstance(result, JobSubmissionFailed):
            job_submission.error = result
            job_submission.state = "FAILED"
            console.log(f"[red]{result}[/red]")
        else:
            assert isinstance(result, BaseException)
            logger.error(f"Unexpected exception: {result}")
            raise result
    return successful_job_submissions


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
    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)
    job_resources_options = cluster_config.sbatch_args

    if job_script is None:
        if cluster_config.job_script_path is None:
            raise ValueError(
                f"No job script specified for cluster {cluster!r}, and no default job script "
                f"path set in the config."
            )
        job_script = Path(os.path.expandvars(str(cluster_config.job_script_path)))
    if not job_script.exists():
        raise ValueError(
            f"The job script ({job_script}) does not exist on this machine. Even though it "
            f"can be customized per cluster, it needs to exist locally, since cluv needs to "
            f"read its header to infer sbatch defaults."
        )

    job_env_vars = get_job_env_vars(
        cluster=cluster, git_commit=git_commit, cluster_config=cluster_config
    )
    project_dir_on_cluster = cluster_config.project_dir
    cluster_job_script_path = get_cluster_job_script_path(
        local_job_script_path=job_script, cluster=cluster, cluster_config=cluster_config
    )
    for job_resources in job_resources_options:
        job_resources = merge_sbatch_args(from_config=job_resources, from_cli=sbatch_args)
        n_chunks, job_resources = apply_chunking(
            job_resources, job_script=job_script, chunking=chunking, env_vars=job_env_vars
        )
        job_resources = add_cluv_sbatch_args(
            job_resources, job_script=job_script, cluster=cluster, cluster_config=cluster_config
        )
        sbatch_command = get_sbatch_command(
            env_vars=job_env_vars,
            job_script=cluster_job_script_path,
            sbatch_args=job_resources,
            program_args=program_args,
            project_dir_on_cluster=project_dir_on_cluster,
        )
        submissions.append(
            Submission(
                cluster=cluster,
                remote=remote,
                job_script=job_script,
                sbatch_args=job_resources,
                program_args=program_args,
                sbatch_command=sbatch_command,
                n_chunks=n_chunks,
                git_commit=git_commit,
            )
        )
    return submissions


def get_cluster_job_script_path(
    local_job_script_path: Path, cluster: str, cluster_config: ClusterConfig
) -> PurePosixPath:
    if cluster_config.job_script_path:
        return cluster_config.job_script_path

    local_project_dir = find_pyproject().parent
    cluster_project_dir = cluster_config.project_dir
    if cluster_project_dir is None:
        if not local_project_dir.is_relative_to(Path.home()):
            raise RuntimeError(
                f"Project path is not set for cluster {cluster!r} in the Cluv config, and the "
                f"project root ({local_project_dir}) is not under $HOME. "
                f"Please set `project_dir` in the Cluv config section of pyproject.toml for that cluster."
            )
        cluster_project_dir = PurePosixPath("$HOME") / local_project_dir.relative_to(Path.home())

    if not local_job_script_path.is_absolute():
        local_job_script_path = local_job_script_path.resolve()

    if not local_job_script_path.is_relative_to(local_project_dir):
        raise RuntimeError("The job script should be relative to the local project root.")

    local_job_script_relative_path = local_job_script_path.relative_to(local_project_dir)
    return cluster_project_dir / local_job_script_relative_path


def merge_sbatch_args(from_config: SbatchArgs, from_cli: list[str]) -> SbatchArgs:
    """Merge the sbatch args from the config and from the CLI, with CLI args taking precedence.

    `-t` is normalized to `time` (its long-flag alias) as it's merged in, so `--time=1:00:00
    -t=2:00:00` -- config or CLI, either order -- resolves to a single `time` value (the last
    one written) instead of leaving two separate keys for what's really the same sbatch option.
    """
    sbatch_args_from_config = sbatch_args_to_list(from_config)
    return sbatch_args_from_list(sbatch_args_from_config + from_cli)


# Characters the cluster's login shell would act on in a value that reaches it unquoted. `$` is
# deliberately *not* in here: `$SCRATCH/logs/x` has to reach that shell intact, so that it is the
# one to expand it.
_UNSAFE_PATH_CHARS = re.compile(r"""[\s'"`;&|<>()\\]""")


def check_path_is_safe_to_interpolate(path: PurePosixPath | str, setting: str) -> None:
    """Raise a `ValueError` if `path` can't be interpolated into the sbatch command as-is.

    Values that may contain environment variables are written into the command *unquoted*, so that
    the cluster's login shell is the one that expands them (see `get_sbatch_command`). Escaping them
    with `shlex.quote` first would stop exactly that, so anything else that shell treats specially
    has to be rejected up front instead: a space would split the value into two `sbatch` arguments,
    and a `;` would end the command and start another one.

    >>> check_path_is_safe_to_interpolate("$SCRATCH/logs/imagenet", "results_path")
    >>> check_path_is_safe_to_interpolate("/home/me/my logs", "results_path")
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: The results_path '/home/me/my logs' contains a ' ', ...
    """
    if match := _UNSAFE_PATH_CHARS.search(str(path)):
        raise ValueError(
            f"The {setting} {str(path)!r} contains a {match.group()!r}, which cluv can't pass to "
            f"sbatch safely: it goes into a `bash --login -c ...` command unquoted, so that the "
            f"cluster's login shell expands variables like $SCRATCH in it. Please use a value "
            f"without whitespace or shell metacharacters."
        )


def get_job_env_vars(
    cluster: str, git_commit: str, cluster_config: ClusterConfig
) -> dict[str, str]:
    env_vars = cluster_config.env.copy()
    env_vars["GIT_COMMIT"] = git_commit
    # Tell the job which cluster config it is running under, so that `cluv.job` / `cluv.config`
    # resolve the same `[tool.cluv.clusters.<name>]` section that we used to submit it. The cluster
    # can't always be identified from inside the job: a job submitted to `trillium-gpu` reports
    # `CC_CLUSTER=trillium`, and Killarney/Vulcan only set `CC_CLUSTER` in a login shell.
    env_vars["CLUV_CLUSTER"] = cluster
    return env_vars


def add_cluv_sbatch_args(
    sbatch_args: SbatchArgs,
    job_script: Path,
    cluster: str,
    cluster_config: ClusterConfig,
) -> SbatchArgs:
    """
    - Add the --output flag (So that outputs are created in the `results_path` for the run prescribed by Cluv)
    - Add the --job-name flag (So that we can identify the cluv jobs later)
    - Add the --export=ALL flag (since trillium and trillium-gpu apparently have `--export=None` as default).
    - Add the --chdir flag to move to the project folder when running the command.

    Returns a new dict; the one passed in is left alone.
    """
    sbatch_args = sbatch_args.copy()

    base_name = sbatch_args.get("job-name") or Path(job_script).stem
    sbatch_args["job-name"] = f"cluv-{base_name}"

    if "output" not in sbatch_args and (
        _header_output := next(
            (
                line
                for line in job_script.read_text().splitlines()
                if line.strip().startswith("#SBATCH") and "--output" in line
            ),
            None,
        )
    ):
        logger.warning(
            f"[yellow]The job script {job_script} sets {_header_output.strip()!r}, which "
            f"will be overridden by cluv's --output so that results can be synced "
            f"back. Consider using cluv in your Python script to decide where to store "
            f"results instead.[/yellow]"
        )

    # Chunked (job array) jobs need `%A`/`%a` (array job id / task id) instead of `%j`.
    if "array" in sbatch_args:
        sbatch_args["output"] = str(cluster_config.results_path / f"{cluster}_%A/slurm-%A_%a.out")
    else:
        sbatch_args["output"] = str(cluster_config.results_path / f"{cluster}_%j/slurm-%j.out")

    # NOTE: `cluster_config.project_dir` already falls back to the global `project_dir`, so the
    # `$HOME/<project>` default below is only used when neither is set.
    local_project_dir = find_pyproject().parent
    remote_project_dir = cluster_config.project_dir or (
        PurePosixPath("$HOME") / local_project_dir.relative_to(Path.home())
    )
    sbatch_args["chdir"] = str(remote_project_dir)
    # Some clusters (trillium, trillium-gpu) have a wrapper around `sbatch` that sets `--export=NONE` by default,
    # which would discard the environment variables.
    sbatch_args["export"] = "ALL"
    return sbatch_args


def get_sbatch_command(
    job_script: PurePosixPath,
    sbatch_args: SbatchArgs,
    program_args: list[str],
    env_vars: dict[str, str],
    project_dir_on_cluster: PurePosixPath | None = None,
) -> str:
    """Generate the command to submit the job via `sbatch` on the cluster."""
    if job_script.is_absolute():
        raise RuntimeError(
            f"The job script path {str(job_script)!r} is an absolute path on this machine, but it "
            f"has to be the path of the script *on the cluster* (relative to the project there, or "
            f"starting with a variable like $HOME that the cluster's shell expands). This is what "
            f"`get_cluster_job_script_path` returns."
        )

    sbatch_flags = sbatch_args_to_list(sbatch_args)
    env_vars_prefix = " ".join(f"{k}={v}" for k, v in env_vars.items())

    # These are interpolated unquoted, so the cluster's login shell expands any env vars in them
    # (`$SCRATCH`, `$HOME`); they can't be `shlex`-escaped first, since quoting would both stop
    # that expansion and close the surrounding single-quoted string.
    check_path_is_safe_to_interpolate(job_script, "job_script")
    if isinstance(chdir := sbatch_args.get("chdir"), str):
        check_path_is_safe_to_interpolate(chdir, "chdir")
    if isinstance(output := sbatch_args.get("output"), str):
        check_path_is_safe_to_interpolate(output, "output")
    for name, value in env_vars.items():
        # Same deal: `UV_CACHE_DIR=$SCRATCH/.cache/uv` has to stay unquoted to be expanded on the
        # cluster, so a value with a space in it would make the login shell read the second word
        # as the command to run, and `sbatch` would never be reached.
        check_path_is_safe_to_interpolate(value, f"{name} environment variable")

    # `program_args` is the one part that is *not* meant to be expanded here: it is whatever the
    # user wrote after `--`, so it gets escaped, and a `$SLURM_TMPDIR` in it survives for the job
    # itself to expand. That only holds together because the whole inner command is quoted in one
    # go below - `shlex.join`'s quotes would otherwise close a hand-written `'...'` around it, and
    # an argument containing a space would break apart (POSIX single quotes don't nest).
    if env_vars_prefix:
        env_vars_prefix += "; "
    cd_command = ""
    if project_dir_on_cluster:
        cd_command = f"cd {project_dir_on_cluster} && "
    inner_command = (
        f"{env_vars_prefix}{cd_command}sbatch --parsable {' '.join(sbatch_flags)} {job_script} "
        f"{shlex.join(program_args)}"
    )
    return f"bash --login -c {shlex.quote(inner_command)}"


async def submit_job(submission: Submission, log_path: Path) -> Job:
    """Does the actual sbatch call, and writes its outcome to `log_path`.

    Raise a `JobSubmissionFailed` if the job submission fails for some reason.
    """
    display = display_commands.get()
    hide = not display
    warn = not raise_on_command_error.get()

    if submission.remote is not None:
        result = await submission.remote.run(
            submission.sbatch_command, display=display, warn=warn, hide=hide
        )
    else:
        result = await run(
            tuple(shlex.split(submission.sbatch_command)), _display=display, warn=warn, hide=hide
        )

    job_id = int(result.stdout.strip()) if result.returncode == 0 else None
    error = None
    if result.returncode != 0:
        error = (
            f"Failed to submit job on cluster {submission.cluster}: "
            f"{result.stderr or result.stdout}"
        )
    write_submission_log(log_path, submission, job_id=job_id, error=error, result=result)

    if error is not None:
        raise JobSubmissionFailed(error)
    assert job_id is not None

    return Job(
        cluster=submission.cluster,
        remote=submission.remote,
        job_script=submission.job_script,
        sbatch_args=submission.sbatch_args,
        program_args=submission.program_args,
        sbatch_command=submission.sbatch_command,
        n_chunks=submission.n_chunks,
        git_commit=submission.git_commit,
        job_id=job_id,
        submitted_at=datetime.datetime.now(),
    )


@contextlib.contextmanager
def logging_commands_to(log_paths: tuple[Path, ...]):
    """Append the commands run in this context (and their outputs) to `log_paths`, as well as
    the traceback of any exception raised."""
    try:
        with set_context(command_log_files, log_paths):
            yield
    except Exception:
        for log_path in log_paths:
            with log_path.open("a") as f:
                f.write(traceback.format_exc())
        raise


def write_submission_log(
    log_path: Path,
    submission: Submission,
    *,
    job_id: int | None,
    error: str | None,
    result: subprocess.CompletedProcess[str],
) -> None:
    """Append the outcome of one `sbatch` call to `log_path`, so it can be found on its own even
    when many submissions' console output is interleaved.
    """
    lines = [
        "",
        "--- submission ---",
        f"status: {'FAILED' if error is not None else 'SUBMITTED'}",
        f"job_id: {job_id if job_id is not None else '-'}",
        f"returncode: {result.returncode}",
        "",
        "--- stdout ---",
        result.stdout,
        "--- stderr ---",
        result.stderr,
    ]
    with log_path.open("a") as f:
        f.write("\n".join(lines))


def build_submit_command(
    cluster: str,
    job_script: str | Path | PurePosixPath | None,
    sbatch_args: list[str],
    program_args: list[str],
) -> str:
    """Build the local `cluv submit` command line used to launch the job."""
    command_parts = ["cluv", "submit", cluster]
    if job_script is not None:
        command_parts.append(str(job_script))
    command_parts.extend(sbatch_args)
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
