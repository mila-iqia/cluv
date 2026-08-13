import asyncio
import logging

from cluv.config import get_cluv_config
from cluv.remote import control_socket_is_running, get_multiplexing_options_to_use, run
from cluv.utils import console, current_cluster

__all__ = ["logout"]
logger = logging.getLogger(__name__)


async def logout(clusters: list[str]) -> None:
    """Closes existing SSH connections (ControlMaster sockets) to the given clusters.

    Parameters:
        clusters: List of cluster hostnames to log out of. If empty, will attempt to
            log out of every cluster in the config that we have an active connection to.
    """
    clusters = list(clusters) if clusters else list(get_cluv_config().clusters_names)
    if (this_cluster := current_cluster()) and this_cluster in clusters:
        # We don't have (or need) an SSH connection to the cluster we're already on.
        clusters.remove(this_cluster)

    logged_out = await asyncio.gather(*(_logout_cluster(cluster) for cluster in clusters))
    logged_out_clusters = [cluster for cluster, ok in zip(clusters, logged_out) if ok]
    if logged_out_clusters:
        console.log(f"Logged out of the following clusters: {logged_out_clusters}")
    else:
        console.log("No active connections to any clusters found.")


async def _logout_cluster(cluster: str) -> bool:
    """Closes the SSH ControlMaster connection to `cluster`, if one is running."""
    if not await control_socket_is_running(cluster):
        return False
    await run(
        ("ssh", *get_multiplexing_options_to_use(cluster), "-O", "exit", cluster),
        warn=True,
        hide=True,
    )
    return True
