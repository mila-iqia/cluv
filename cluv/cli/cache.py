"""`cluv cache`: inspect the local cache."""

from __future__ import annotations

from rich import box
from rich.table import Table

from cluv.cache import _get_cache_file, _get_cached_jobs_path, load_jobs, read_cache
from cluv.cli.disable import format_remaining
from cluv.utils import console

__all__ = ["show"]


def show() -> None:
    """Display the current contents of the local cache."""
    cache = read_cache()
    jobs = load_jobs()

    console.print(f"[bold]Cache file:[/bold] {_get_cache_file()}")

    if not cache.disabled_clusters and not cache.project_states and not jobs:
        console.print("Cache is empty.", style="yellow")
        return

    if cache.disabled_clusters:
        table = Table(
            title="Disabled Clusters",
            box=box.ROUNDED,
            header_style="bold white on #1a1a2e",
            title_style="bold cyan",
        )
        table.add_column("Cluster", style="bold")
        table.add_column("Disabled at")
        table.add_column("Re-enables")
        for cluster, info in cache.disabled_clusters.items():
            re_enables = (
                format_remaining(info.disabled_until) if info.disabled_until else "indefinitely"
            )
            table.add_row(
                cluster, info.disabled_at.strftime("%Y-%m-%d %H:%M:%S %z"), re_enables
            )
        console.print(table)

    if cache.project_states:
        table = Table(
            title="Project State per Cluster",
            box=box.ROUNDED,
            header_style="bold white on #1a1a2e",
            title_style="bold cyan",
        )
        table.add_column("Cluster", style="bold")
        table.add_column("uv version")
        table.add_column("Last synced commit")
        table.add_column("Checked out commit")
        table.add_column("Last fetch watermark")
        for cluster, state in cache.project_states.items():
            table.add_row(
                cluster,
                state.uv_version or "-",
                state.last_uv_sync_git_commit or "-",
                state.checked_out_git_commit or "-",
                state.last_fetch_watermark.strftime("%Y-%m-%d %H:%M:%S %z")
                if state.last_fetch_watermark
                else "-",
            )
        console.print(table)

    if jobs:
        console.print(f"[bold]{len(jobs)}[/bold] job(s) cached ({_get_cached_jobs_path()}).")
