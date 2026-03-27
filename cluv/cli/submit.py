from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

import rich_argparse
from milatools.utils.remote_v2 import RemoteV2

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
    )
    submit_parser.add_argument(
        "cluster",
        choices=cluster_choices if cluster_choices else None,
        metavar="<cluster>",
        help="The cluster to submit the job on.",
    )
    submit_parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        metavar="<command>",
        help="The command to run inside the job (passed to the job script).",
    )
    submit_parser.add_argument(
        "--job-script",
        default=None,
        metavar="PATH",
        help="Path to the sbatch job script (relative to project root). Overrides [tool.cluv.submit] job_script.",
    )
    submit_parser.add_argument(
        "--no-sync",
        action="store_true",
        default=False,
        help="Skip syncing the project to the cluster before submitting.",
    )
    submit_parser.set_defaults(func=submit)
    return submit_parser


async def submit(
    cluster: str,
    command: list[str],
    job_script: str | None,
    no_sync: bool,
):
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state (like safe_sbatch), sets GIT_COMMIT and any
    SBATCH_* env vars configured in [tool.cluv.slurm] / [tool.cluv.clusters.<name>],
    then calls sbatch on the remote.
    """
    # 1. Check git is clean locally.
    git_status = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True
    )
    if git_status.stdout.strip():
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]"
        )
        sys.exit(1)

    # 2. Capture current commit hash.
    git_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()

    # 3. Sync (or just connect).
    if not no_sync:
        remotes = await sync(clusters=[cluster])
        remote = remotes[0]
    else:
        remote = await RemoteV2.connect(cluster)

    config = get_config()

    # 4. Resolve job script.
    resolved_job_script = job_script or config.submit.job_script
    if not resolved_job_script:
        console.print(
            "[red]No job script specified. Pass --job-script or set job_script in [tool.cluv.submit].[/red]"
        )
        sys.exit(1)

    project_path = find_pyproject().parent.relative_to(Path.home())
    remote_job_script = f"~/{project_path}/{resolved_job_script}"

    # 5. Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    env_vars: dict[str, str] = {**config.slurm}
    env_vars.update(config.cluster_configs.get(cluster, {}))
    env_vars["GIT_COMMIT"] = git_commit

    env_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    command_str = shlex.join(command)

    # 6. Submit.
    remote_cmd = f"bash -l -c '{env_prefix} sbatch {remote_job_script} {command_str}'"
    console.print(f"Submitting job on [bold]{cluster}[/bold]: {resolved_job_script} {command_str}")
    await remote.run_async(remote_cmd)
