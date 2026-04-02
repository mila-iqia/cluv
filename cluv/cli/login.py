import asyncio
import logging

from cluv.config import get_config
from cluv.remote import Remote, control_socket_is_running
from cluv.utils import console, current_cluster

logger = logging.getLogger(__name__)


async def login(clusters: list[str]) -> list[Remote]:
    """Create an SSH connection with the given clusters, reusing existing connections when possible to avoid triggering 2FA prompts."""
    clusters = clusters or get_config().clusters
    if (this_cluster := current_cluster()) and this_cluster in clusters:
        # don't try to connect to the cluster we're already on.
        clusters.remove(this_cluster)

    connections = await asyncio.gather(
        *(get_remote_without_2fa_prompt(cluster) for cluster in clusters)
    )
    # For any cluster we don't have an active connection to, connect
    if any(connections):
        console.log(
            f"Already connected to the following clusters: {[remote.hostname for remote in connections if remote]}"
        )
    else:
        console.log("No active connections to any clusters found.")
    missing_connections = [cluster for cluster, remote in zip(clusters, connections) if not remote]
    if missing_connections:
        console.log(f"Will attempt to connect to the following clusters: {missing_connections}")
    # Need to do each thing sequentially to avoid triggering multiple 2FA prompts at the same time.
    return [
        remote if remote is not None else (await Remote.connect(cluster))
        for cluster, remote in zip(clusters, connections)
    ]


async def get_remote_without_2fa_prompt(cluster_hostname: str) -> Remote | None:
    """Returns the Remote object for a given cluster if we already have a connection to it.

    If we don't already have a connection, this will not block for 2FA, and will return None.
    """
    if await control_socket_is_running(cluster_hostname):
        return Remote(cluster_hostname)
    return None
