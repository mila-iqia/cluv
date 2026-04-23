from __future__ import annotations

import datetime
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.remote import Remote
from cluv.utils import console


async def submit(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
):
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets GIT_COMMIT and any
    SBATCH_* env vars configured in [tool.cluv.slurm] / [tool.cluv.clusters.<name>],
    then calls sbatch on the remote.

    sbatch_args are forwarded as flags to sbatch; program_args are passed to
    the job script. main() extracts program_args from argv before argparse runs,
    since argparse strips '--' before REMAINDER sees it.
    """
    # Check git is clean locally (untracked files are fine).
    git_commit = ensure_clean_git_state()

    # Sync.
    remote = (await sync(clusters=[cluster]))[0]
    # TODO: Idea, could also run with --test-only to get the time estimate, then without it to get
    # the job id.
    _, start_time_estimate = await sbatch(
        remote,
        job_script=job_script,
        sbatch_args=sbatch_args + ["--test-only"],
        program_args=program_args,
        git_commit=git_commit,
    )
    job_id, _ = await sbatch(
        remote,
        job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        git_commit=git_commit,
    )
    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        + (
            f"It is expected to start at {start_time_estimate} (in {start_time_estimate - datetime.datetime.now()}).\n"
            if start_time_estimate is not None
            else ""
        )
        + f"Use `ssh {cluster} sacct -j {job_id}` to view its status.",
    )
    return job_id


async def sbatch(
    remote: Remote,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> tuple[int, datetime.datetime | None]:
    cluster = remote.hostname
    remote_cmd = get_sbatch_command(
        cluster, Path(job_script), sbatch_args, program_args, git_commit
    )
    # Submit.
    if "--test-only" in sbatch_args:
        console.print(f"Testing a job submission on [bold]{cluster}[/bold].")
        completed_process = await remote.run(remote_cmd)
        return get_job_id_and_starttime_from_stderr(completed_process.stderr)

    console.print(f"Submitting job on [bold]{cluster}[/bold].")
    job_id = int(await remote.get_output(remote_cmd))
    return job_id, None


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> str:
    project_path = find_pyproject().parent.relative_to(Path.home())
    remote_job_script = f"~/{project_path}/{job_script}"

    # Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    config = get_config()
    env_vars: dict[str, str] = {**config.slurm}
    env_vars.update(config.cluster_configs.get(cluster, {}))

    # Prefix the job name with "cluv-" so it is easy to identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={project_path} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


def ensure_clean_git_state():
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines and not (os.environ.get("SKIP_CLEAN_GIT_CHECK", "0") == "1"):
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]",
        )
        sys.exit(1)

    # Capture current commit hash.
    git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    return git_commit


def get_job_id_and_starttime_from_stderr(stderr: str) -> tuple[int, datetime.datetime]:
    r"""Gets the job ID from the stderr output of sbatch --test-only, which looks like:

    >>> get_job_id_and_starttime_from_stderr("sbatch: Job 10759317 to start at 2026-04-21T16:55:36 using 1 processors on nodes rc32407 in partition cpubase_bycore_b1\n")
    (10759317, datetime.datetime(2026, 4, 21, 16, 55, 36))
    """
    match = re.search(r"sbatch: Job (\d+) to start at", stderr)
    if not match:
        raise ValueError(f"Could not parse job ID from sbatch output: {stderr}")
    job_id = int(match.group(1))

    # Remove the rest of the message (that we don't need).
    if "a using" in stderr:
        # Weird output on the Mila cluster:
        stderr = stderr[: stderr.index(" a using")]
    else:
        # Remove the rest of the message (that we don't need).
        stderr = stderr[: stderr.index(" using")]

    starttime_estimate = datetime.datetime.strptime(
        stderr,
        f"sbatch: Job {job_id} to start at %Y-%m-%dT%H:%M:%S",
    )
    return job_id, starttime_estimate
