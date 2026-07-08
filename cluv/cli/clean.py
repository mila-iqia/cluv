"""`cluv clean`: remove run results from clusters once they're gone from the local results dir."""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from rich.prompt import Confirm

from cluv.cache import CacheContent, read_cache
from cluv.cli.login import login
from cluv.cli.sync import expandvars, get_active_remotes
from cluv.config import get_cluv_config
from cluv.remote import list_remote_run_dirs
from cluv.utils import console

__all__ = ["clean", "compute_runs_to_delete"]

logger = logging.getLogger(__name__)


def compute_runs_to_delete(
    local_names: set[str],
    remote_runs: list[tuple[str, datetime]],
    watermark: datetime | None,
) -> list[str]:
    """Returns the names of remote run dirs that are safe to delete.

    A remote run dir is safe to delete only if it has no local counterpart AND it was already
    visible during the last successful sync of that cluster, i.e. `mtime <= watermark` (the max
    remote mtime observed during that sync). A genuinely new, never-fetched run always has
    `mtime > watermark`, so it's never selected even though it also has no local counterpart --
    only runs the user pruned locally are.

    Returns an empty list if `watermark` is `None` (the cluster has never been synced).
    """
    if watermark is None:
        return []
    to_delete = sorted(
        name for name, mtime in remote_runs if name not in local_names and mtime <= watermark
    )
    logger.debug(
        f"watermark={watermark.isoformat()}, "
        f"remote_runs={[(name, mtime.isoformat()) for name, mtime in remote_runs]}, "
        f"local_names={sorted(local_names)}, to_delete={to_delete}"
    )
    return to_delete


def _watermark_for(cache: CacheContent, cluster: str) -> datetime | None:
    project_state = cache.project_states.get(cluster)
    return project_state.last_fetch_watermark if project_state else None


async def clean(
    clusters: list[str] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> None:
    """Removes run directories from remote clusters that were pruned from the local results dir.

    Does not run `sync` first: it only reads state cached by the last successful sync of each
    cluster. Clusters that have never been synced are skipped, since there is no watermark yet
    to distinguish a pruned run from one that's simply never been fetched. Running or pending
    Slurm jobs, and cross-cluster run-name collisions, are not specially handled (see the design
    spec's "Non-goals" section).
    """
    logger.info(f"Starting cluv clean: clusters={clusters}, force={force}, dry_run={dry_run}")
    config = get_cluv_config()

    all_remotes = await get_active_remotes()
    if clusters:
        remotes = await login(clusters)
    elif not all_remotes:
        raise RuntimeError(
            "[red]Not currently connected to any Slurm cluster.[/red] "
            "Use `cluv login` to login and create reusable connections."
        )
    else:
        remotes = all_remotes.copy()
    logger.debug(f"Cleaning on remotes: {[remote.hostname for remote in remotes]}")

    cache = read_cache()
    results_path_here = Path(os.path.expandvars(config.results_path))
    local_names = (
        {p.name for p in results_path_here.iterdir() if p.is_dir()}
        if results_path_here.exists()
        else set()
    )
    logger.debug(f"Local run dirs in {results_path_here}: {sorted(local_names)}")

    per_cluster_to_delete: dict[str, list[str]] = {}
    skipped: list[str] = []
    for remote in remotes:
        cluster = remote.hostname
        watermark = _watermark_for(cache, cluster)
        if watermark is None:
            logger.debug(f"{cluster}: no fetch watermark cached, skipping")
            skipped.append(cluster)
            continue
        cluster_config = config.get_cluster_config(cluster)
        results_path_on_cluster = await expandvars(remote, cluster_config.results_path)
        remote_runs = await list_remote_run_dirs(remote, results_path_on_cluster)
        logger.debug(
            f"{cluster}: {len(remote_runs)} remote run dir(s) in {results_path_on_cluster}"
        )
        to_delete = compute_runs_to_delete(local_names, remote_runs, watermark)
        if to_delete:
            per_cluster_to_delete[cluster] = to_delete

    for cluster in skipped:
        console.print(
            f"[yellow]Skipping {cluster}: never synced. Run `cluv sync {cluster}` first.[/yellow]"
        )

    if not per_cluster_to_delete:
        logger.info("Nothing to clean.")
        console.print("[green]Nothing to clean.[/green]")
        return

    total = sum(len(names) for names in per_cluster_to_delete.values())
    logger.info(f"{total} run director{'y' if total == 1 else 'ies'} eligible for deletion")
    console.print("[bold]The following run directories will be removed:[/bold]")
    for cluster, names in per_cluster_to_delete.items():
        console.print(f"  [cyan]{cluster}[/cyan]:")
        for name in names:
            console.print(f"    {name}")

    if dry_run:
        logger.info("Dry run: not deleting anything.")
        return

    if not force and not Confirm.ask(
        f"Delete {total} run director{'y' if total == 1 else 'ies'} across "
        f"{len(per_cluster_to_delete)} cluster(s)?",
        default=False,
    ):
        logger.info("User declined to delete. Aborting.")
        console.print("Aborted.")
        return

    remote_by_hostname = {remote.hostname: remote for remote in remotes}
    removed = 0
    failed: list[tuple[str, str]] = []
    for cluster, names in per_cluster_to_delete.items():
        remote = remote_by_hostname[cluster]
        cluster_config = config.get_cluster_config(cluster)
        results_path_on_cluster = await expandvars(remote, cluster_config.results_path)
        for name in names:
            logger.debug(f"Removing {cluster}:{results_path_on_cluster / name}")
            try:
                await remote.run(f"rm -rf {results_path_on_cluster / name}", hide=True)
            except subprocess.CalledProcessError as err:
                logger.warning(f"Failed to remove {cluster}:{name}: {err}")
                failed.append((cluster, name))
            else:
                removed += 1

    logger.info(
        f"Removed {removed} run director{'y' if removed == 1 else 'ies'}; {len(failed)} failed"
    )
    console.print(
        f"[green]Removed {removed} run director{'y' if removed == 1 else 'ies'}.[/green]"
    )
    if failed:
        console.print(f"[red]Failed to remove {len(failed)}:[/red]")
        for cluster, name in failed:
            console.print(f"  {cluster}: {name}")
