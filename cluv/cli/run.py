from __future__ import annotations

import argparse
import asyncio
import logging
import shlex
from pathlib import Path

import rich_argparse
from milatools.cli import console
from milatools.utils.remote_v2 import (
    RemoteV2,
)
from rich.console import Group
from rich.panel import Panel

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.utils import current_cluster

logger = logging.getLogger(__name__)


def add_run_args(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    cluster_choices = get_config().clusters
    run_parser = subparsers.add_parser(
        "run",
        help="Run a command on a cluster",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    run_parser.add_argument(
        "cluster",
        choices=cluster_choices if cluster_choices else None,
        # default=,
        metavar="<cluster>",
        help="The cluster to run the command on",
    )
    run_parser.add_argument(
        "command",
        type=str,
        metavar="<command>",
        help="The command to run",
        nargs=argparse.REMAINDER,
    )
    run_parser.set_defaults(func=run)
    return run_parser


async def run(command: str | list[str], cluster: str):
    """Runs a command in the synced project on a potentially remote cluster.

    Similar in spirit to `uv run`, but runs a command in the synced project on a potentially remote cluster.
    - Idea is that this could maybe be a building block for other commands.
    """
    if not isinstance(command, str):
        command = shlex.join(command)
    logger.debug(f"About to run {command=} on {cluster=}")
    remotes = await sync(clusters=[cluster])
    project_path = find_pyproject().parent.relative_to(Path.home())
    await asyncio.gather(
        *[
            remote.run_async(f"bash -l -c 'uv run --directory={project_path} {command}'")
            for remote in remotes
        ]
    )


async def _get_cluster_remotes(clusters: list[str] | None) -> list[RemoteV2]:
    """Returns the list remote objects for each cluster with a current active connection."""
    if clusters:
        # User specified clusters
        # We don't check for active connection if user explicitly asks for a cluster.
        # RemoteV2.connect will try to connect (and start the socket if needed/possible)
        # When there isn't an existing connection, this might generate a ton of 2FA
        # prompts at once.
        return list(await asyncio.gather(*[RemoteV2.connect(cluster) for cluster in clusters]))
    # Use default list and filter for active connections
    # We need to check which ones are active WITHOUT trying to connect interactively
    # control_socket_is_running_async checks if the socket exists and is running

    # We need to construct RemoteV2 objects to get the control path, but we
    # shouldn't start them yet
    # Actually RemoteV2 constructor doesn't start if _start_control_socket=False
    this_cluster = current_cluster()
    # When no cluster is passed, sync with clusters for which we have an active SSH connection.
    clusters = get_config().clusters
    if this_cluster and this_cluster in clusters:
        clusters.remove(this_cluster)
    connections = await asyncio.gather(
        *(get_remote_without_2fa_prompt(cluster) for cluster in clusters)
    )
    remotes = [conn for conn in connections if conn]
    if not remotes:
        console.log(
            "[red]Not currently connected to any Slurm cluster.[/red] "
            "Use `cluv login` to login and create reusable connections."
        )
        return []
    return remotes


async def _run_multiple_clusters(
    command: str | list[str], clusters: str | list[str] | None = None
):
    """CLI wrapper for the run command."""
    if not command:
        console.print("No command specified.", style="red")
        return

    if isinstance(command, str):
        cmd_str = command
    else:
        # It's only a list of strings because of argparse.REMAINDER.
        # This doesn't mean 'multiple commands'.
        cmd_str = " ".join(command)

    if isinstance(clusters, str):
        clusters = [c.strip() for c in clusters.split(",")]

    if not clusters:
        console.print("No active cluster connections found.", style="yellow")
        return

    console.print(f"Running '{cmd_str}' on {len(clusters)} clusters...", style="bold blue")

    results = await asyncio.gather(*(run(cmd_str, cluster) for cluster in clusters))

    panels = []
    for cluster, result in zip(clusters, results):
        style = "green" if result.returncode == 0 else "red"
        center = " (returncode: " + str(result.returncode) + ")" if result.returncode != 0 else ""
        title = f"[bold]{cluster.hostname}{center}[/bold]"

        content = ""
        # If there is ONLY stdout, then don't add the 'Stdout:' header:
        if result.stdout and not result.stderr:
            content += result.stdout.strip()
        elif result.stdout:
            content += f"[bold]Stdout:[/bold]\n{result.stdout.strip()}\n"
        if result.stderr:
            content += f"[bold]Stderr:[/bold]\n{result.stderr.strip()}\n"

        if not content:
            content = "[italic]No output[/italic]"

        panels.append(Panel(content, title=title, border_style=style))
    # TODO: Do we want to display results differently depending on if all the results are single-line vs multi-line?

    console.print(Group(*panels))
