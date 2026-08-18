from __future__ import annotations

import asyncio
import datetime
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

from cluv import tui
from cluv.cache import Job, save_job
from cluv.cli.submit_utils.chunking import chunking_update_sbatch_args, get_n_chunks
from cluv.cli.submit_utils.first import (
    cancel_job,
    wait_for_jobs_to_cancel,
    wait_for_running_job,
)
from cluv.cli.sync import get_active_remotes, sync
from cluv.config import SbatchArgs, find_pyproject, get_cluv_config
from cluv.remote import Remote, run
from cluv.tui import JobWaitProgress
from cluv.utils import console, current_cluster

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
    chunking: bool = False,
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
        chunking: Whether to split the job up into multiple consecutive short jobs.
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
            label=progress_label(job_script, program_args),
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
    chunking: bool,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit the job on all clusters (and on every allocation of each cluster), and wait until one
    of them starts. Once one starts, cancel the others.
    """
    # Sync with all clusters with an existing connections.
    if not _skip_sync:
        remotes = await sync()
    else:
        remotes = await get_active_remotes()
    cluster_to_remote: dict[str, Remote | None] = {remote.hostname: remote for remote in remotes}
    this_cluster = current_cluster()
    if this_cluster is not None:
        # We are also on a Slurm cluster, so consider this as an option as well.
        cluster_to_remote[this_cluster] = None
        # `sync` does not return a Remote for the current cluster.
        assert not any(remote.hostname == this_cluster for remote in remotes)
    job_scripts = {
        cluster: _check_job_script_exists_locally(
            job_script or get_job_script_path_from_config(cluster), cluster
        )
        for cluster in cluster_to_remote
    }

    # One submission per allocation of each cluster.
    config = get_cluv_config()
    submissions = [
        Submission(
            remote=remote,
            job_script=job_scripts[cluster],
            sbatch_args=sbatch_args_from_dict(config_sbatch_args) + sbatch_args,
            program_args=program_args,
        )
        for cluster, remote in cluster_to_remote.items()
        for config_sbatch_args in config.get_cluster_config(cluster).sbatch_args
    ]
    return await submit_and_keep_first(
        submissions,
        git_commit=git_commit,
        chunking=chunking,
        label=progress_label(job_script, program_args),
    )


def progress_label(job_script: Path | None, program_args: list[str]) -> str:
    """Short identifier for a submission, used to tell concurrent submissions apart."""
    return shlex.join(program_args) if program_args else str(job_script or "job")


async def submit_and_keep_first(
    submissions: list[Submission],
    git_commit: str,
    chunking: bool = False,
    label: str = "job",
) -> Job | None:
    """Submit all the given jobs, wait until one of them starts, then cancel the others.

    `label` identifies this batch among any other `submit_and_keep_first()` calls running
    concurrently (e.g. a Hydra sweep submitting many jobs at once). It is only shown in the
    live display when more than one call is active.
    """

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
        title=f"Jobs submitted on the clusters — {label}",
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

    def make_progress() -> JobWaitProgress:
        return JobWaitProgress(
            label=label,
            cancelling=cancelling,
            rows=[(job.cluster, job.job_id, job_state) for job, job_state in job_to_state.items()],
        )

    try:
        async with tui.registry.section(make_progress()) as live:
            wait_result = await wait_for_running_job(
                job_to_state, cluster_to_remote, max_wait_time_seconds
            )
            live.update(make_progress())
            live.refresh()  # probably not entirely necessary.
            if not wait_result:
                console.log("All submitted jobs have failed! Exiting.")
                return None

            first_running_job, first_state = wait_result

            console.log(
                f"Job {first_running_job.job_id} on cluster {first_running_job.cluster} is {first_state}. "
                f"Cancelling the other jobs...\n",
            )
            cancelling = True
            await wait_for_jobs_to_cancel(
                job_to_state,
                first_running_job,
                cluster_to_remote,
                max_wait_time_seconds,
            )
            live.update(make_progress())
            live.refresh()  # probably not entirely necessary.

        console.print(
            f"Successfully cancelled all other jobs except for job {first_running_job.job_id} "
            f"on cluster {first_running_job.cluster}, which is {first_state}."
        )
        if first_state.startswith("RUNNING"):
            console.print(
                f"Use `ssh {first_running_job.cluster} sacct -j {first_running_job.job_id}` to view its status."
            )
            console.print(
                f"Once completed, run `cluv sync {first_running_job.cluster}` to fetch its results."
            )

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
    chunking: bool,
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
        n_chunks = get_n_chunks(sbatch_args, env_vars, job_script)
        sbatch_args = chunking_update_sbatch_args(n_chunks, sbatch_args)
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
    chunking: bool,
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
