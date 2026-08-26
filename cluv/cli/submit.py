import argparse
import asyncio
import dataclasses
import datetime
import logging
import os
import shlex
import subprocess
import sys
from contextvars import ContextVar
from pathlib import Path, PurePosixPath

import rich.box
import rich.table
import rich.text
from rich.live import Live

from cluv.cache import Job, Submission, save_job
from cluv.cli.submit_utils.chunking import apply_chunking
from cluv.cli.sync import get_cluster_to_remote, sync_common_part, sync_per_cluster_part
from cluv.config import SbatchArgs, find_pyproject, get_cluv_config
from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, run_saccts
from cluv.utils import console, group_by_cluster

logger = logging.getLogger(__name__)

__all__ = ["submit"]

display_commands = ContextVar("display_commands", default=True)
raise_on_command_error = ContextVar("raise_on_command_error", default=False)

JobState = str


class JobSubmissionFailed(Exception):
    """Raised when a job submission fails."""


@dataclasses.dataclass
class SubmissionProgress:
    """Live, mutable tracking of one `Submission`'s progress.

    Tracks a submission from before it's even synced (``state="SYNCING"``, no `job` yet),
    through submission (``job`` known, state polled from `sacct`), to running and, possibly,
    cancellation.

    A single flat list of these -- covering every submission, on every cluster, for one
    `submit()` call -- is all a live display needs to render the whole picture, from a plain
    `rich.Live` table (see `render_job_table` below) up to, eventually, a table shared across
    several concurrent `submit`/`submit_first` calls (`rich.Live` only supports one live region
    per console, so that would fuse several such lists together instead of replacing this one).
    """

    submission: Submission
    state: JobState = "SYNCING"
    job: Job | None = None
    error: str | None = None

    @property
    def cluster(self) -> str:
        return self.submission.cluster

    @property
    def job_id(self) -> int | None:
        return self.job.job_id if self.job is not None else None


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
    sbatch_flags = shlex.join(sbatch_args_from_dict(submission.sbatch_args))
    program_args_str = shlex.join(submission.program_args)
    return f"bash --login -c '(...) {sbatch_flags} -- {program_args_str}'"


def render_job_table(
    rows: list[SubmissionProgress], *, cancelling: bool = False
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
    for row in rows:
        table.add_row(
            row.cluster,
            str(row.job_id) if row.job_id is not None else "-",
            rich.text.Text(row.state, style=_state_style(row.state)),
            _short_command(row.submission),
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
) -> Job | None:
    """Submit a job to the given cluster (or all clusters if `cluster=="first"`),
    and return the Job object if successful.

    Returns None if the submission failed.
    """
    submit_command = build_submit_command(
        cluster=cluster, job_script=job_script, sbatch_args=sbatch_args, program_args=program_args
    )
    git_commit = ensure_clean_git_state(autocommit=autocommit, submit_command=submit_command)
    cluster_to_remote = await get_cluster_to_remote(cluster)

    job_submissions = [
        SubmissionProgress(submission=submission)
        for cluster_name, remote in cluster_to_remote.items()
        for submission in get_submissions(
            cluster_name,
            remote,
            job_script=job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            chunking=chunking,
            git_commit=git_commit,
        )
    ]

    if not _skip_sync:
        remotes = [r for r in cluster_to_remote.values() if r]
        await sync_common_part(remotes)

    found_running_job = asyncio.Event()
    tasks = [
        asyncio.create_task(
            submit_to_cluster(
                cluster_name,
                remote,
                job_submissions=[
                    job_submission
                    for job_submission in job_submissions
                    if job_submission.cluster == cluster_name
                ],
                found_running_job=found_running_job,
                _skip_sync=_skip_sync,
            )
        )
        for cluster_name, remote in cluster_to_remote.items()
    ]

    cancelling = False

    def _render() -> rich.table.Table:
        return render_job_table(job_submissions, cancelling=cancelling)

    try:
        with Live(get_renderable=_render, console=console, refresh_per_second=1):
            first_running_row = await wait_for_first_running_job(
                job_submissions, cluster_to_remote, tasks, found_running_job
            )
            if first_running_row is None:
                console.log("All job submissions have failed! Exiting.")
                return None

            cancelling = True
            other_rows = [
                row
                for row in job_submissions
                if row is not first_running_row and row.job_id is not None
            ]
            await wait_for_jobs_to_cancel(other_rows, cluster_to_remote)
    except (KeyboardInterrupt, asyncio.CancelledError):
        # The user stopped `cluv submit` while jobs were still in flight -- cancel everything
        # that got a job id so far instead of leaving them running unattended.
        console.log("Interrupted by user. Cancelling all submitted jobs...")
        submitted_rows = [row for row in job_submissions if row.job_id is not None]
        await run_scancel(submitted_rows)
        raise

    console.print(
        f"Job {first_running_row.job_id} on cluster {first_running_row.cluster} is running.",
        style="green",
    )
    job = first_running_row.job
    assert job is not None
    save_job(job)
    return job


async def wait_for_first_running_job(
    job_submissions: list[SubmissionProgress],
    cluster_to_remote: dict[str, Remote | None],
    tasks: list[asyncio.Task],
    found_running_job: asyncio.Event,
    max_wait_time_seconds: int = 60,
) -> SubmissionProgress | None:
    """Poll `sacct` until one submitted job starts running, or every submission has failed.

    Mutates `rows` in place with the latest known job id / state, so a live display can render
    them at any point during this wait. Sets `found_running_job` the moment a job starts, so
    that clusters which haven't submitted their own jobs yet can skip doing so.

    Returns the row for the job that started, or None if every submission ended up failing.
    """
    delay = 1
    while True:
        submitted = [row for row in job_submissions if row.job_id is not None]
        by_cluster = group_by_cluster(submitted)
        if by_cluster:
            states_per_cluster = await asyncio.gather(
                *(
                    run_saccts(cluster_to_remote[cluster], [row.job_id for row in cluster_rows])
                    for cluster, cluster_rows in by_cluster.items()
                )
            )
            for cluster_rows, states in zip(by_cluster.values(), states_per_cluster):
                for row, state in zip(cluster_rows, states):
                    row.state = state
                    if state.startswith(("RUNNING", "COMPLETED")):
                        found_running_job.set()
                        return row

        all_failed = bool(submitted) and all(
            row.state.startswith(tuple(FAILED_JOB_STATES)) for row in submitted
        )
        if all(task.done() for task in tasks) and (not submitted or all_failed):
            return None

        await asyncio.sleep(delay)
        delay = min(delay * 2, max_wait_time_seconds)


async def wait_for_jobs_to_cancel(
    job_submissions: list[SubmissionProgress],
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> None:
    """Cancel every (already-submitted) job in `rows`, and wait until they're all done."""
    to_cancel = [
        job for job in job_submissions if not job.state.startswith(("CANCELLED", "COMPLETED"))
    ]
    if not to_cancel:
        return

    await run_scancel(to_cancel)

    delay = 1
    while to_cancel:
        by_cluster = group_by_cluster(to_cancel)
        states_per_cluster = await asyncio.gather(
            *(
                run_saccts(cluster_to_remote[cluster], [row.job_id for row in cluster_rows])
                for cluster, cluster_rows in by_cluster.items()
            )
        )
        for cluster_rows, states in zip(by_cluster.values(), states_per_cluster):
            for row, state in zip(cluster_rows, states):
                if state.startswith("CANCELLED") or state == "FAILED":
                    # "CANCELLED by <uid>", and a stray "FAILED" job step on some clusters
                    # (while the rest of the job is "CANCELLED"), both just mean cancelled.
                    state = "CANCELLED"
                row.state = state

        to_cancel = [
            row for row in to_cancel if not row.state.startswith(("CANCELLED", "COMPLETED"))
        ]
        if to_cancel:
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_wait_time_seconds)

    console.log(f"Cancelled {len(job_submissions)} other job submission(s).")


async def run_scancel(rows: list[SubmissionProgress]) -> None:
    """Cancel the (already-submitted) jobs behind `rows`, grouped by remote."""
    if not rows:
        return
    by_remote: dict[Remote | None, list[SubmissionProgress]] = {}
    for row in rows:
        by_remote.setdefault(row.submission.remote, []).append(row)

    async def cancel(remote: Remote | None, cluster_rows: list[SubmissionProgress]) -> None:
        job_ids = [row.job_id for row in cluster_rows]
        scancel_command = f"scancel {' '.join(map(str, job_ids))}"
        if remote is not None:
            await remote.get_output(scancel_command, hide=True)
        else:
            await run(tuple(shlex.split(scancel_command)), hide=True)

    await asyncio.gather(
        *(cancel(remote, cluster_rows) for remote, cluster_rows in by_remote.items())
    )


async def submit_to_cluster(
    cluster: str,
    remote: Remote | None,
    job_submissions: list[SubmissionProgress],
    found_running_job: asyncio.Event,
    _skip_sync: bool = False,
) -> None:
    """Sync then submit every submission for one cluster, in parallel."""
    if not _skip_sync:
        await sync_per_cluster_part(remote)

    if found_running_job.is_set():
        # If a job has already started on another cluster, we don't need to submit more jobs.
        console.log(
            f"Skipping submission of jobs to cluster {cluster} because a job "
            f"has already started on another cluster."
        )
        for row in job_submissions:
            row.state = "SKIPPED"
        return

    for row in job_submissions:
        row.state = "SUBMITTING"

    results = await asyncio.gather(
        *(submit_job(row.submission) for row in job_submissions),
        return_exceptions=True,
    )

    assert len(results) == len(job_submissions)
    for row, result in zip(job_submissions, results):
        if isinstance(result, Job):
            row.job = result
            row.state = "PENDING"
        elif isinstance(result, JobSubmissionFailed):
            row.error = str(result)
            row.state = "FAILED"
            console.log(f"[red]{result}[/red]")
        else:
            assert isinstance(result, BaseException)
            raise result


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

    for job_resources in job_resources_options:
        job_resources = merge_sbatch_args(from_config=job_resources, from_cli=sbatch_args)
        n_chunks, job_resources = apply_chunking(
            job_resources, job_script=job_script, chunking=chunking
        )
        sbatch_command = get_sbatch_command(
            cluster,
            job_script=job_script,
            sbatch_args=job_resources,
            program_args=program_args,
            git_commit=git_commit,
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


def sbatch_args_from_args_list(sbatch_args_list: list[str]) -> SbatchArgs:
    """Convert a list of sbatch flags (from the CLI) to a dict of sbatch options.

    Behaves like argparse, where if the flags are passed multiple times, the last value is kept.
    Aliases for common commands are also kept.

    >>> sbatch_args_from_args_list(["--time=2:00:00", "-t=00:00:30"])
    {'time': '00:00:30'}
    >>> sbatch_args_from_args_list(["--time=2:00:00", "--gpus=1"])
    {'time': '2:00:00', 'gpus': '1'}
    >>> sbatch_args_from_args_list(["--exclusive"])
    {'exclusive': True}
    >>> sbatch_args_from_args_list(["-N", "2"])
    {'nodes': '2'}
    >>> sbatch_args_from_args_list(["-f", "2"])
    {'f': '2'}
    >>> sbatch_args_from_args_list(["--gpus", "--requeue=False"])
    {'gpus': True, 'requeue': 'False'}
    >>> sbatch_args_from_args_list(["-n"])
    {'n': True}
    >>> sbatch_args_from_args_list(["--array=0-3%2", "-a=0-1%1"])
    {'array': '0-3%2', 'a': '0-1%1'}
    """
    # Maybe use argparse, and keep it simple! No need to recreate every single sbatch flag,
    #
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("-c", "--cpus-per-task", dest="cpus-per-task", default=argparse.SUPPRESS)
    parser.add_argument("-t", "--time", dest="time", default=argparse.SUPPRESS)
    parser.add_argument("-N", "--nodes", dest="nodes", default=argparse.SUPPRESS)
    parser.add_argument("-A", "--account", dest="account", default=argparse.SUPPRESS)
    args, unknown = parser.parse_known_args(sbatch_args_list)
    sbatch_args: SbatchArgs = vars(args)

    # First, join any stragglers like ['-f', '2'] into ['-f=2'] so we can parse them consistently.
    # Edge case: ['-f', '-g'] stays the same.
    joined_unknown_args: list[str] = []
    skip_next = False
    for i, arg in enumerate(unknown):
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("-") and i + 1 < len(unknown) and not unknown[i + 1].startswith("-"):
            joined_unknown_args.append(f"{arg}={unknown[i + 1]}")
            skip_next = True
        else:
            joined_unknown_args.append(arg)

    for value in joined_unknown_args:
        if value.startswith("--"):
            key, _, val = value[2:].partition("=")
        elif value.startswith("-"):
            value = value.removeprefix("-")
            key, _, val = value.partition("=")
        else:
            continue
        if not val.strip():
            val = True  # --exclusive --> {exclusive: True}
        if val is not None:
            if key in sbatch_args:
                # remove the value so the ordering is preserved based on the positioning in `sbatch_args_list`.
                sbatch_args.pop(key)
            sbatch_args[key] = val
    return sbatch_args


def merge_sbatch_args(from_config: SbatchArgs, from_cli: list[str]) -> SbatchArgs:
    """Merge the sbatch args from the config and from the CLI, with CLI args taking precedence.

    `-t` is normalized to `time` (its long-flag alias) as it's merged in, so `--time=1:00:00
    -t=2:00:00` -- config or CLI, either order -- resolves to a single `time` value (the last
    one written) instead of leaving two separate keys for what's really the same sbatch option.
    """
    sbatch_args_from_config = sbatch_args_from_dict(from_config)
    return sbatch_args_from_args_list(sbatch_args_from_config + from_cli)
    # merged: SbatchArgs = from_config.copy()
    # for val in from_cli:
    #     key, val
    #     if key in merged:
    #         _val = merged.pop(key)
    #     merged[key] = value
    # return merged


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
    ['-N=2']
    >>> sbatch_args_from_dict({"gpus": "", "requeue": False})
    []
    >>> sbatch_args_from_dict({"n": True})
    ['-n']
    >>> sbatch_args_from_dict({"f": "config"})
    ['-f=config']
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
                flags.append(f"-{key}={value}")
            else:
                flags.append(f"--{key}={value}")
    return flags


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: SbatchArgs,
    program_args: list[str],
    git_commit: str,
) -> str:
    """Generate the command to submit the job via `sbatch` on `cluster`, with the appropriate
    sbatch arguments, environment variables and paths set.
    """
    local_project_dir = find_pyproject().parent
    local_job_script = job_script if job_script.is_absolute() else local_project_dir / job_script
    job_script_relative_path = local_job_script.relative_to(local_project_dir)

    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)
    remote_project_dir = cluster_config.project_dir or (
        PurePosixPath("$HOME") / local_project_dir.relative_to(Path.home())
    )
    remote_job_script = PurePosixPath(remote_project_dir) / job_script_relative_path

    env_vars: dict[str, str] = {**config.env, **cluster_config.env}
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    sbatch_flags = sbatch_args_from_dict(sbatch_args)
    if not any("--output" in flag for flag in sbatch_flags):
        # Chunked (job array) jobs need `%A`/`%a` (array job id / task id) instead of `%j`.
        if "array" in sbatch_args:
            env_vars["SBATCH_OUTPUT"] = (
                f"{cluster_config.results_path}/{cluster}_%A/slurm-%A_%a.out"
            )
        else:
            env_vars["SBATCH_OUTPUT"] = f"{cluster_config.results_path}/{cluster}_%j/slurm-%j.out"

        header_output = next(
            (
                line
                for line in local_job_script.read_text().splitlines()
                if line.strip().startswith("#SBATCH") and "--output" in line
            ),
            None,
        )
        if header_output is not None:
            logger.warning(
                f"[yellow]The job script {job_script} sets {header_output.strip()!r}, which "
                f"will be overridden by cluv's SBATCH_OUTPUT so that results can be synced "
                f"back. Consider using cluv in your Python script to decide where to store "
                f"results instead.[/yellow]"
            )

    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = shlex.join(sbatch_flags)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={remote_project_dir} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


async def submit_job(submission: Submission) -> Job:
    """Does the actual sbatch call.

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

    if result.returncode != 0:
        raise JobSubmissionFailed(
            f"Failed to submit job on cluster {submission.cluster}: "
            f"{result.stderr or result.stdout}"
        )

    return Job(
        cluster=submission.cluster,
        remote=submission.remote,
        job_script=submission.job_script,
        sbatch_args=submission.sbatch_args,
        program_args=submission.program_args,
        sbatch_command=submission.sbatch_command,
        n_chunks=submission.n_chunks,
        git_commit=submission.git_commit,
        job_id=int(result.stdout.strip()),
        submitted_at=datetime.datetime.now(),
    )


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
