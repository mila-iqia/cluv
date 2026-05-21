"""`cluv history`: inspect and manage the local sacct-derived memory cache."""

from __future__ import annotations

import logging

from rich.table import Table

from cluv import history as history_module
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.utils import console

logger = logging.getLogger(__name__)

__all__ = ["history"]


async def history(
    action: str,
    cluster: str | None,
    key: str | None,
    since_days: int,
) -> None:
    """Dispatch to `list`, `backfill`, or `clear` based on `action`."""
    if action == "list":
        _list(cluster)
    elif action == "backfill":
        if not cluster:
            console.print("[red]`cluv history backfill` requires a <cluster> argument.[/red]")
            return
        await _backfill(cluster, since_days)
    elif action == "clear":
        _clear(cluster, key)
    else:
        console.print(f"[red]unknown history action: {action}[/red]")


def _list(cluster: str | None) -> None:
    rows = history_module.list_keys(cluster)
    if not rows:
        scope = f" for {cluster}" if cluster else ""
        console.print(f"[yellow]no records cached{scope}.[/yellow]")
        console.print(f"cache dir: {history_module.cache_dir()}")
        return
    table = Table(title=f"history cache ({history_module.cache_dir()})")
    table.add_column("cluster")
    table.add_column("key")
    table.add_column("records", justify="right")
    for c, k, n in rows:
        table.add_row(c, k, str(n))
    console.print(table)


async def _backfill(cluster: str, since_days: int) -> None:
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        console.print(
            f"[red]no active SSH connection to {cluster}.[/red] "
            f"Run `cluv login {cluster}` first."
        )
        return
    n = await history_module.backfill_from_sacct(remote, cluster, since_days=since_days)
    console.print(f"backfilled {n} record(s) from sacct on {cluster} (last {since_days} days).")


def _clear(cluster: str | None, key: str | None) -> None:
    if key and not cluster:
        console.print("[red]`--key` requires `<cluster>`.[/red]")
        return
    deleted = history_module.clear(cluster, key)
    scope = (
        f"({cluster}/{key})"
        if cluster and key
        else f"({cluster})"
        if cluster
        else "(all clusters)"
    )
    console.print(f"deleted {deleted} cache file(s) {scope}.")
