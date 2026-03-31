from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

import rich_argparse

from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.utils import console


def add_submit_args(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    cluster_choices = get_config().clusters
    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit a SLURM job on a remote cluster.",
        formatter_class=rich_argparse.RichHelpFormatter,
        usage="cluv submit <cluster> <job.sh> [sbatch-args...] [-- program-args...]",
    )
    submit_parser.add_argument(
        "cluster",
        choices=cluster_choices if cluster_choices else None,
        metavar="<cluster>",
        help="The cluster to submit the job on.",
    )
    submit_parser.add_argument(
        "job_script",
        metavar="<job.sh>",
        help="Path to the sbatch job script (relative to project root).",
    )
    submit_parser.add_argument(
        "sbatch_args",
        nargs=argparse.REMAINDER,
        metavar="...",
        help="sbatch flags (before --) and/or program arguments (after --).",
    )
    submit_parser.set_defaults(func=submit)
    return submit_parser


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
    # 1. Check git is clean locally (untracked files are fine).
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines:
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]"
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
    env_vars["GIT_COMMIT"] = git_commit

    env_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    # 6. Submit.
    remote_cmd = f"bash -l -c '{env_prefix} sbatch {sbatch_args_str} {remote_job_script} {program_args_str}'"
    console.print(
        f"Submitting job on [bold]{cluster}[/bold]: {job_script}"
        + (f" {sbatch_args_str}" if sbatch_args_str else "")
        + (f" -- {program_args_str}" if program_args_str else "")
    )
    await remote.run(remote_cmd)
