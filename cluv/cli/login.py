import asyncio
import logging

from cluv.cache import DisabledCluster
from cluv.config import get_cluv_config
from cluv.remote import Remote, control_socket_is_running
from cluv.utils import console, current_cluster

__all__ = ["login"]
logger = logging.getLogger(__name__)


async def login(clusters: list[str], disabled: dict[str, DisabledCluster] | None = None) -> list[Remote]:
    """Create an SSH connection with the given clusters, reusing existing connections when possible.

    Parameters:
        clusters: List of cluster hostnames to connect to. If empty, will attempt to connect to all
            clusters in the config that we don't already have an active connection to.
        disabled: Optional pre-fetched mapping of disabled clusters. When ``None`` (the default),
            the mapping is fetched from the cache and a warning is printed if any clusters are
            disabled. Pass an already-fetched dict (e.g. from :func:`print_disabled_clusters`) to
            suppress the duplicate warning when ``login`` is called from within another command.

    Returns:
        A list of `Remote` objects, one for each cluster.
    """
    if disabled is None:
        from cluv.cli.disable import print_disabled_clusters

        disabled = print_disabled_clusters()

    clusters = list(clusters or get_cluv_config().clusters_names)
    if (this_cluster := current_cluster()) and this_cluster in clusters:
        # don't try to connect to the cluster we're already on.
        clusters.remove(this_cluster)

    # Skip disabled clusters.
    clusters = [c for c in clusters if c not in disabled]

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
    remotes: list[Remote] = []
    for cluster, remote in zip(clusters, connections):
        try:
            remotes.append(remote if remote is not None else await Remote.connect(cluster))
        except Exception as e:
            console.log(e, style="red")

    return remotes


async def get_remote_without_2fa_prompt(cluster_hostname: str) -> Remote | None:
    """Returns the Remote object for a given cluster if we already have a connection to it.

    If we don't already have a connection, this will not block for 2FA, and will return None.
    """
    if await control_socket_is_running(cluster_hostname):
        return Remote(cluster_hostname)
    return None
