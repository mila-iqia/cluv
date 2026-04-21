from __future__ import annotations

import asyncio
import datetime
import re
import shlex
import subprocess
import sys
from pathlib import Path

from cluv.cli.login import login
from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.utils import console


async def submit(
    cluster: str,
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
):
    if cluster == "auto":
        clusters = await login([])
        job_ids_and_estimated_starttimes = await asyncio.gather(
            *(
                submit_job(c.hostname, job_script, sbatch_args, program_args, test_only=True)
                for c in clusters
            )
        )
        print(dict(zip([c.hostname for c in clusters], job_ids_and_estimated_starttimes)))
    else:
        await submit_job(cluster, job_script, sbatch_args, program_args, test_only=False)


async def submit_job(
    cluster: str,
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
    test_only: bool = False,
) -> tuple[int, datetime.datetime | None]:
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets GIT_COMMIT and any
    SBATCH_* env vars configured in [tool.cluv.slurm] / [tool.cluv.clusters.<name>],
    then calls sbatch on the remote.

    sbatch_args are forwarded as flags to sbatch; program_args are passed to
    the job script. main() extracts program_args from argv before argparse runs,
    since argparse strips '--' before REMAINDER sees it.
    """
    # 1. Check git is clean locally (untracked files are fine).
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines:
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]",
        )
        sys.exit(1)

    # 2. Capture current commit hash.
    git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()

    # 3. Sync.
    remotes = await sync(clusters=[cluster])
    remote = remotes[0]

    config = get_config()

    # 4. Resolve remote job script path.
    project_path = find_pyproject().parent.relative_to(Path.home())
    remote_job_script = f"~/{project_path}/{job_script}"

    # 5. Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    env_vars: dict[str, str] = {**config.slurm}
    env_vars.update(config.cluster_configs.get(cluster, {}))
    # Prefix the job name with "cluv-" so admins can identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    env_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    # 6. Submit.
    if test_only:
        remote_cmd = f"bash -l -c '{env_prefix} sbatch --parsable --test-only --chdir={project_path} {sbatch_args_str} {remote_job_script} {program_args_str}'"
        console.print(
            f"Testing a job submission on [bold]{cluster}[/bold]: {job_script}"
            + (f" {sbatch_args_str}" if sbatch_args_str else "")
            + (f" -- {program_args_str}" if program_args_str else "")
        )
        completed_process = await remote.run(remote_cmd)
        return get_job_id_and_starttime_from_stderr(completed_process.stderr)

    remote_cmd = f"bash -l -c '{env_prefix} sbatch --parsable --chdir={project_path} {sbatch_args_str} {remote_job_script} {program_args_str}'"
    console.print(
        f"Submitting job on [bold]{cluster}[/bold]: {job_script}"
        + (f" {sbatch_args_str}" if sbatch_args_str else "")
        + (f" -- {program_args_str}" if program_args_str else "")
    )
    job_id = int(await remote.get_output(remote_cmd))
    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        f"Use `ssh {cluster} sacct -j {job_id}` to view its status."
    )
    return job_id, None


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
