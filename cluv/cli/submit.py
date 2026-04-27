from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.utils import console


async def submit(
    cluster: str,
    job_script: str,
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
    # Check git is clean locally (untracked files are fine) and capture current commit hash.
    git_commit = ensure_clean_git_state()

    # Sync.
    remotes = await sync(clusters=[cluster])
    remote = remotes[0]

    config = get_config()

    # Resolve remote job script path.
    project_path = find_pyproject().parent.relative_to(Path.home())
    remote_job_script = f"~/{project_path}/{job_script}"

    # Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    env_vars: dict[str, str] = {**config.slurm}
    env_vars.update(config.cluster_configs.get(cluster, {}))
    # Prefix the job name with "cluv-" so admins can identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    env_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    # Submit.
    remote_cmd = f"bash -l -c '{env_prefix} sbatch --parsable --chdir={project_path} {sbatch_args_str} {remote_job_script} {program_args_str}'"
    console.print(
        f"Submitting job on [bold]{cluster}[/bold]: {job_script}"
        + (f" {sbatch_args_str}" if sbatch_args_str else "")
        + (f" -- {program_args_str}" if program_args_str else "")
    )
    output = await remote.get_output(remote_cmd)
    job_id = int(output.strip())

    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        f"Use `ssh {cluster} sacct -j {job_id}` to view its status."
    )

    return job_id
    # return the job id?


def ensure_clean_git_state() -> str:
    """
    Check git is clean locally and return the current commit hash.
    """
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines:
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]",
        )
        sys.exit(1)

    # Capture current commit hash.
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
