from __future__ import annotations

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

import rich.syntax
import rich.table
import rich.text
from rich.live import Live

from cluv.cache import Job, save_job
from cluv.cli.sync import get_active_remotes, sync
from cluv.config import find_pyproject, get_cluv_config
from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct
from cluv.utils import console, current_cluster

logger = logging.getLogger(__name__)

__all__ = ["submit"]
display_commands = ContextVar("display_commands", default=True)
raise_on_command_error = ContextVar("raise_on_command_error", default=False)


def sbatch_args_from_dict(d: dict[str, str | bool]) -> list[str]:
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


async def submit(
    cluster: str,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool = False,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets `GIT_COMMIT` and any
    environment variables configured in `[tool.cluv.env]` / `[tool.cluv.clusters.<name>.env]`,
    then calls `sbatch` on the remote.

    `sbatch_args` are forwarded as flags to `sbatch`; `program_args` are passed to
    the job script.


    Parameters:
        cluster: SSH hostname of the target cluster. Can be set to "first" to launch the job on all clusters and keep only the first one to starts.
        job_script: Path to the job script to submit, relative to the project root.
            When omitted, uses the configured default for the target cluster.
        sbatch_args: List of additional flags to pass to `sbatch`.
        program_args: List of arguments to pass to the job script, for example `["python", "main.py"]`.
        autocommit: If True, automatically create a local commit with tracked changes before submitting.
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
            job_script, sbatch_args, program_args, git_commit, _skip_sync=_skip_sync
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
    result = await sbatch(remote, job_script, sbatch_args, program_args, git_commit)
    submit_time = datetime.datetime.now()

    if result.returncode != 0:
        console.print(f"Error during sbatch : {result.stderr}", style="red")
        return None

    job_id = int(result.stdout.strip())
    job = Job(
        job_id=job_id,
        cluster=cluster,
        job_script=str(job_script),
        git_commit=git_commit,
        sbatch_args=sbatch_args,
        program_args=program_args,
        submitted_at=submit_time.isoformat(),
    )
    save_job(job)

    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        f"Use `ssh {cluster} sacct -j {job_id}` to view its status, and `cluv sync {cluster}` to "
        f"fetch results once it is complete."
    )

    return job


async def submit_first(
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit the job on all clusters, and wait until one of them starts.
    Once one starts, cancel the others.
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

    # Submit the job on all the clusters (and possibly locally).
    sbatch_commands = {
        cluster: get_sbatch_command(
            cluster, job_scripts[cluster], sbatch_args, program_args, git_commit
        )
        for cluster in cluster_to_remote
    }
    sbatch_results = await asyncio.gather(
        *[
            sbatch(
                remote,
                job_script=job_scripts[cluster],
                sbatch_args=sbatch_args,
                program_args=program_args,
                git_commit=git_commit,
            )
            for cluster, remote in cluster_to_remote.items()
        ],
        return_exceptions=True,
    )
    submit_time = datetime.datetime.now()
    # TODO: This could be a list of tuples eventually, since we could potentially try to submit
    # multiple different jobs per cluster.
    cluster_to_sbatch_result = dict(zip(cluster_to_remote.keys(), sbatch_results))

    cluster_to_jobid: dict[str, int] = {}
    table = rich.table.Table("Cluster", "Result", title="Jobs submitted on the clusters")
    for cluster, sbatch_result in cluster_to_sbatch_result.items():
        sbatch_command = sbatch_commands[cluster]
        if isinstance(sbatch_result, BaseException) or sbatch_result.returncode != 0:
            error_message = (
                str(sbatch_result)
                if isinstance(sbatch_result, BaseException)
                else sbatch_result.stderr.strip()
            )
            output_text = rich.text.Text(f"Error: {error_message}", style="red")
        else:
            job_id = int(sbatch_result.stdout.strip())
            cluster_to_jobid[cluster] = job_id
            output_text = rich.text.Text(f"Job ID: {job_id}", style="green")
        table.add_row(
            cluster,
            rich.console.Group(
                rich.syntax.Syntax(sbatch_command, lexer="sh", word_wrap=True),
                output_text,
            ),
            end_section=True,
        )

    console.print(table)

    if not cluster_to_jobid:
        console.print("No job submitted on clusters. See errors above.")
        return None

    # Wait for a job to start on a cluster.
    # If the wait is interrupted, cancel all jobs.
    first_running_job: JobHandle | None = None

    max_wait_time_seconds = 5

    cluster_and_jobid_to_jobstate: dict[tuple[str, int], str] = {
        (cluster, job_id): "UNKNOWN" for cluster, job_id in cluster_to_jobid.items()
    }
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
        for (cluster, job_id), job_state in cluster_and_jobid_to_jobstate.items():
            table.add_row(
                cluster,
                str(job_id),
                rich.text.Text(
                    job_state,
                    style="green"
                    if job_state.startswith(("RUNNING", "COMPLETED", "CANCELLED"))
                    else "yellow"
                    if job_state.startswith(("PENDING", "UNKNOWN"))
                    else "red",
                ),
            )
        return table

    try:
        with Live(get_renderable=make_table, console=console, refresh_per_second=1) as live:
            first_running_job = await wait_for_running_job(
                cluster_and_jobid_to_jobstate, cluster_to_remote, max_wait_time_seconds
            )
            live.update(make_table(), refresh=True)  # probably not entirely necessary.
            if not first_running_job:
                console.log("All submitted jobs have failed! Exiting.")
                return None

            console.log(
                f"Job {first_running_job.job_id} on cluster {first_running_job.cluster} is {first_running_job.state}. "
                f"Cancelling the other jobs...\n",
            )
            cancelling = True
            await wait_for_jobs_to_cancel(
                cluster_and_jobid_to_jobstate,
                first_running_job,
                cluster_to_remote,
                max_wait_time_seconds,
            )
            live.update(make_table(), refresh=True)  # probably not entirely necessary.

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
        to_cancel = list(cluster_to_jobid.items())
        if first_running_job:
            to_cancel.remove((first_running_job.cluster, first_running_job.job_id))
        await asyncio.gather(
            *[
                cancel_job(cluster_to_remote[cluster], job_id, print=True)
                for cluster, job_id in to_cancel
            ]
        )
        return None

    # TODO: Return the cluster and job id.
    assert first_running_job
    return Job(
        job_id=first_running_job.job_id,
        cluster=first_running_job.cluster,
        job_script=str(job_scripts[first_running_job.cluster]),
        git_commit=git_commit,
        sbatch_args=sbatch_args,
        program_args=program_args,
        submitted_at=submit_time.isoformat(),
    )


@dataclasses.dataclass(frozen=True)
class JobHandle:
    cluster: str
    job_id: int
    state: str


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
            f"{error_text}",
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
                "Working directory is dirty. Please commit your changes before submitting.",
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
                f"[yellow]Could not resolve {remote_head_ref}. Falling back to local HEAD commit.[/yellow]"
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


def _check_job_script_not_none[P: Path | PurePosixPath](job_script: P | None, cluster: str) -> P:
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
) -> str:
    """
    Generate the command to submit the job via sbatch on the remote cluster, with the appropriate env vars set.
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

    in_job_chunking = False
    in_job_packing = False
    assert not in_job_chunking and not in_job_packing, "todo"
    # might contain unresolved env vars.
    cluster_results_path = PurePosixPath(cluster_config.results_path)
    # TODO: Use the `get_run_id` function with the placeholder job id %j and task index %t:
    if not any("--output" in flag for flag in sbatch_args):
        if in_job_chunking:
            assert not in_job_packing, "can't do both right now."
            env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%A/slurm-%A_%a.out"
        elif in_job_packing:
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
    config_sbatch_args = sbatch_args_from_dict(cluster_config.sbatch_args)
    all_sbatch_args = config_sbatch_args + sbatch_args
    sbatch_args_str = shlex.join(all_sbatch_args)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={remote_project_dir} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


async def sbatch(
    remote: Remote | None,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> subprocess.CompletedProcess[str]:
    """Submit the job via sbatch on the remote cluster, and return the job id."""
    cluster = remote.hostname if remote else current_cluster()
    # Should be set, since `remote` is None if current_cluster() is the same as the cluster argument
    # to `submit`.
    assert cluster
    sbatch_command = get_sbatch_command(cluster, job_script, sbatch_args, program_args, git_commit)

    display = display_commands.get()
    hide = not display
    warn = not raise_on_command_error.get()

    if remote:
        return await remote.run(sbatch_command, display=display, warn=warn, hide=hide)
    # Run the sbatch command locally.
    return await run(tuple(shlex.split(sbatch_command)), _display=display, warn=warn, hide=hide)


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
