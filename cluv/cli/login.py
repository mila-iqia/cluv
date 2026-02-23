import asyncio
import logging
from pathlib import Path

import rich
from milatools.utils.remote_v2 import (
    RemoteV2,
    control_socket_is_running_async,
    get_controlpath_for,
)

from cluv.config import get_config

logger = logging.getLogger(__name__)


async def login_cli(clusters: list[str]) -> None:
    """Create an SSH connection with the given clusters, reusing existing connections when possible to avoid triggering 2FA prompts."""
    # Need a function that returns None for the CLI. Other functions (ex. sync) use `login` below as well.
    await login(clusters)


async def login(clusters: list[str]) -> list[RemoteV2]:
    """Create an SSH connection with the given clusters, reusing existing connections when possible to avoid triggering 2FA prompts."""
    clusters = clusters or get_config().clusters
    this_cluster = "mila"  # todo: infer from environment or config perhaps.
    if this_cluster in clusters:
        clusters.remove(
            this_cluster
        )  # don't try to connect to the cluster we're already on.

    console = rich.console.Console()
    connections = await asyncio.gather(
        *(_get_remote_without_2fa_prompt(cluster) for cluster in clusters)
    )
    # For any cluster we don't have an active connection to, connect
    if connections:
        console.log(
            f"Already connected to the following clusters: {[remote.hostname for remote in connections if remote]}"
        )
    else:
        console.log("No active connections to any clusters found.")
    console.log(
        f"Will attempt to connect to the following clusters: "
        f"{[cluster for cluster, remote in zip(clusters, connections) if not remote]}"
    )
    # Need to do each thing sequentially to avoid triggering multiple 2FA prompts at the same time.
    return [
        remote if remote is not None else (await RemoteV2.connect(cluster))
        for cluster, remote in zip(clusters, connections)
    ]


async def _get_remote_without_2fa_prompt(cluster_hostname: str) -> RemoteV2 | None:
    """Returns the Remote object for a given cluster if we already have a connection to it.

    If we don't already have a connection, this will not block for 2FA, and will return None.
    """
    remote = RemoteV2(cluster_hostname, _start_control_socket=False)
    active = await control_socket_is_running_async(
        cluster_hostname,
        control_path=get_controlpath_for(
            cluster_hostname, ssh_config_path=Path.home() / ".ssh" / "config"
        ),
    )
    if active:
        # It's active, so we can "connect" (this just sets _started=True since it's
        # already running)
        # NOTE: This is a bit weird.
        remote._started = True
        return remote
    return None
