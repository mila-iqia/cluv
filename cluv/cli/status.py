from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import get_config
from cluv.remote import Remote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data layer – replace these with real implementations later
# ---------------------------------------------------------------------------

CLUSTERS = (
    "mila",
    "narval",
    "tamia",
    "rorqual",
    "fir",
    "nibi",
    "killarney",
    "vulcan",
    "trillium",
)

MOCK_DATA_SEED = 42  # deterministic seed so the display is reproducible
OFFLINE_PROBABILITY = 0.08  # ~8 % chance of a cluster being down/maintenance


# Rough GPU pool sizes per cluster (total GPUs available on the cluster).
_GPU_TOTALS: dict[str, int] = {
    "mila": 2048,
    "narval": 1024,
    "tamia": 512,
    "rorqual": 768,
    "fir": 640,
    "nibi": 256,
    "killarney": 384,
    "vulcan": 512,
    "trillium": 1280,
}

# Storage quota in GiB (home, scratch)
_STORAGE_QUOTAS: dict[str, tuple[int, int]] = {
    "mila": (50, 5000),
    "narval": (50, 10000),
    "tamia": (100, 8000),
    "rorqual": (100, 12000),
    "fir": (50, 6000),
    "nibi": (50, 4000),
    "killarney": (100, 7500),
    "vulcan": (100, 9000),
    "trillium": (50, 15000),
}


@dataclass
class JobStats:
    running: int
    pending: int
    # subset of the above that belong to the current user
    my_running: int
    my_pending: int
    cancelled: int | None = None
    completed: int | None = None
    my_completed: int | None = None  # recently completed jobs for the current user


@dataclass
class StorageStats:
    """Disk usage as (used_gib, quota_gib) for $HOME and $SCRATCH."""

    home_used: float
    home_quota: float
    scratch_used: float
    scratch_quota: float


@dataclass
class ClusterStatus:
    name: str
    online: bool
    gpu_idle: int
    gpu_total: int
    gpu_model: str
    jobs: JobStats
    storage: StorageStats
    avg_wait_min: int | None = None  # estimated queue wait time in minutes
    avg_gpu_util_pct: float | None = None  # average GPU utilisation across running jobs


# ---------------------------------------------------------------------------
# Real data layer
# ---------------------------------------------------------------------------

# All commands are separated by a sentinel so we can split a single SSH output.
_SEP = "---CLUV-SEP---"

# sacct command to count the current user's recently completed jobs (last 24 h).
# --allocations skips job-step rows (.batch, .0, …) so we count whole jobs only.
_SACCT_MY_COMPLETED = (
    f"sacct -u $(whoami) --noheader --allocations -S yesterday"
    f" --state=CD --format=JobID 2>/dev/null | wc -l; echo {_SEP}"
)

# Script for DRAC clusters (partition-stats + diskusage_report, no savail/disk-quota)
_REMOTE_SCRIPT_DRAC = f"""
partition-stats 2>/dev/null; echo {_SEP}
sinfo --noheader -N -o "%N %t %G" 2>/dev/null | sort -u | grep gpu; echo {_SEP}
squeue -u $(whoami) -h -t R -o "%i" 2>/dev/null | wc -l; echo {_SEP}
squeue -u $(whoami) -h -t PD -o "%i" 2>/dev/null | wc -l; echo {_SEP}
timeout 1 diskusage_report 2>/dev/null; echo {_SEP}
{_SACCT_MY_COMPLETED}
"""

# Script for the Mila cluster (savail + disk-quota, no partition-stats/diskusage_report)
_REMOTE_SCRIPT_MILA = f"""
echo {_SEP}
sinfo --noheader -N -o "%N %t %G" 2>/dev/null | sort -u | grep gpu; echo {_SEP}
squeue -u $(whoami) -h -t R -o "%i" 2>/dev/null | wc -l; echo {_SEP}
squeue -u $(whoami) -h -t PD -o "%i" 2>/dev/null | wc -l; echo {_SEP}
echo {_SEP}
savail 2>/dev/null; echo {_SEP}
disk-quota 2>/dev/null; echo {_SEP}
squeue -h -t R -o "%i" 2>/dev/null | wc -l; echo {_SEP}
squeue -h -t PD -o "%i" 2>/dev/null | wc -l; echo {_SEP}
{_SACCT_MY_COMPLETED}
"""

_MILA_CLUSTERS = {"mila"}


async def get_real_cluster_status(remote: Remote) -> ClusterStatus:
    """Fetch live Slurm data from a remote cluster and return a ClusterStatus.

    Uses a single SSH round-trip. Falls back gracefully when commands are
    unavailable (e.g. partition-stats is DRAC-only).
    """
    from cluv.cli.slurm import (
        parse_disk_quota,
        parse_diskusage_report,
        parse_partition_stats,
        parse_savail,
        parse_sinfo_nodes,
    )

    cluster = remote.hostname
    script = _REMOTE_SCRIPT_MILA if cluster in _MILA_CLUSTERS else _REMOTE_SCRIPT_DRAC

    try:
        raw = await remote.get_output(
            f"bash -l -c '{script}'",
            hide=True,
            warn=True,
            display=False,
        )
    except Exception as exc:
        logger.warning(f"[red]Could not reach {cluster}: {exc}[/red]")
        return ClusterStatus(
            name=cluster,
            online=False,
            gpu_idle=0,
            gpu_total=0,
            gpu_model="?",
            jobs=JobStats(running=0, pending=0, my_running=0, my_pending=0),
            storage=StorageStats(home_used=0, home_quota=0, scratch_used=0, scratch_quota=0),
        )

    parts = raw.split(_SEP)
    # Pad in case some sections are missing
    parts += [""] * 10
    (
        partition_stats_out,
        sinfo_out,
        running_out,
        pending_out,
        diskusage_out,
        savail_out,
        disk_quota_out,
        all_running_out,
        all_pending_out,
    ) = parts[:9]
    # sacct completed count is appended at the end of both scripts:
    # index 5 for DRAC (after diskusage), index 9 for Mila (after all_pending).
    my_completed_out = parts[9] if cluster in _MILA_CLUSTERS else parts[5]

    # --- GPU info: prefer savail (Mila) over sinfo (DRAC) ---
    savail_idle, savail_total, savail_models = parse_savail(savail_out)
    if savail_total > 0:
        gpu_idle, gpu_total, models = savail_idle, savail_total, savail_models
    else:
        gpu_idle, gpu_total, models = parse_sinfo_nodes(sinfo_out)
    gpu_model = ", ".join(models) if models else "?"

    # --- Job counts ---
    has_partition_stats = bool(partition_stats_out.strip())
    if has_partition_stats:
        ps = parse_partition_stats(partition_stats_out)
        jobs_running = ps["jobs_running"]
        jobs_pending = ps["jobs_pending"]
        # If neither savail nor sinfo gave us GPU counts, fall back to
        # partition-stats node counts (less precise but better than nothing).
        if gpu_total == 0:
            gpu_idle = ps["gpu_idle_nodes"]
            gpu_total = ps["gpu_total_nodes"]
    else:
        try:
            jobs_running = int(all_running_out.strip())
            jobs_pending = int(all_pending_out.strip())
        except ValueError:
            jobs_running = jobs_pending = 0

    try:
        my_running = int(running_out.strip())
        my_pending = int(pending_out.strip())
    except ValueError:
        my_running = my_pending = 0

    try:
        my_completed: int | None = int(my_completed_out.strip())
    except ValueError:
        my_completed = None

    # --- Storage: prefer diskusage_report (DRAC, per-user quotas);
    #     fall back to disk-quota (Mila: lfs for $HOME, beegfs for $SCRATCH) ---
    storage = parse_diskusage_report(diskusage_out)
    if storage.home_quota == 0:
        storage = parse_disk_quota(disk_quota_out)

    return ClusterStatus(
        name=cluster,
        online=True,
        gpu_idle=gpu_idle,
        gpu_total=gpu_total,
        gpu_model=gpu_model,
        jobs=JobStats(
            running=jobs_running,
            pending=jobs_pending,
            my_running=my_running,
            my_pending=my_pending,
            my_completed=my_completed,
        ),
        storage=storage,
    )


async def get_all_cluster_statuses(
    remotes: list[Remote] | None = None,
) -> tuple[list[ClusterStatus], bool]:
    """Query clusters in parallel.

    If *remotes* is provided, query exactly those connections.
    Otherwise, query all clusters that already have an active SSH connection
    (never blocks on 2FA).

    Returns (statuses, any_live) where any_live is False when no cluster
    was reachable.
    """
    if remotes is None:
        clusters = get_config().clusters
        remotes = [
            r
            for r in await asyncio.gather(*(get_remote_without_2fa_prompt(c) for c in clusters))
            if r is not None
        ]

    if not remotes:
        return [], False

    statuses = list(await asyncio.gather(*(get_real_cluster_status(r) for r in remotes)))
    return statuses, True


def get_mock_cluster_status(username: str = "you") -> list[ClusterStatus]:
    """Return fake but plausible status data for every known cluster.

    This function is intentionally free of any UI logic so it can be swapped
    out for a real implementation that queries Slurm / the cluster APIs.
    """
    rng = random.Random(MOCK_DATA_SEED)

    gpu_models = ["A100", "H100", "V100", "A40", "RTX 8000"]

    results: list[ClusterStatus] = []
    for cluster in get_config().clusters:
        gpu_total = _GPU_TOTALS[cluster]
        # Simulate varying load – some clusters busier than others
        load_factor = rng.uniform(0.55, 0.98)
        gpu_busy = int(gpu_total * load_factor)
        gpu_idle = gpu_total - gpu_busy

        total_jobs = int(gpu_busy * rng.uniform(0.8, 1.4))
        pending = int(total_jobs * rng.uniform(0.1, 0.4))
        running = total_jobs - pending
        cancelled = int(total_jobs * rng.uniform(0.01, 0.05))
        completed = int(total_jobs * rng.uniform(0.5, 2.0))

        my_running = rng.randint(0, min(8, running))
        my_pending = rng.randint(0, min(4, pending))
        my_completed = completed * my_running // max(running, 1)

        home_quota, scratch_quota = _STORAGE_QUOTAS[cluster]
        home_used = round(rng.uniform(5, home_quota * 0.90), 1)
        scratch_used = round(rng.uniform(home_quota, scratch_quota * 0.95), 1)

        online = rng.random() > OFFLINE_PROBABILITY

        results.append(
            ClusterStatus(
                name=cluster,
                online=online,
                gpu_idle=gpu_idle,
                gpu_total=gpu_total,
                gpu_model=rng.choice(gpu_models),
                jobs=JobStats(
                    running=running,
                    pending=pending,
                    my_running=my_running,
                    my_pending=my_pending,
                    cancelled=cancelled,
                    completed=completed,
                    my_completed=my_completed,
                ),
                storage=StorageStats(
                    home_used=home_used,
                    home_quota=home_quota,
                    scratch_used=scratch_used,
                    scratch_quota=scratch_quota,
                ),
                avg_wait_min=rng.randint(2, 240),
                avg_gpu_util_pct=round(rng.uniform(40, 99), 1),
            )
        )
    return results


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def _bar(used: float, total: float, width: int = 10) -> Text:
    """Return a coloured block-character progress bar."""
    ratio = used / total if total else 0
    filled = int(ratio * width)
    bar_str = "█" * filled + "░" * (width - filled)
    pct = ratio * 100
    if pct < 60:
        colour = "green"
    elif pct < 85:
        colour = "yellow"
    else:
        colour = "red"
    return Text(f"{bar_str} {pct:4.0f}%", style=colour)


def _gpu_bar(idle: int, total: int, width: int = 10) -> Text:
    """Return a bar that represents *free* GPUs (more free = greener)."""
    ratio = idle / total if total else 0
    filled = int(ratio * width)
    bar_str = "▰" * filled + "▱" * (width - filled)
    pct = ratio * 100
    if pct >= 20:
        colour = "green"
    elif pct >= 8:
        colour = "yellow"
    else:
        colour = "red"
    return Text(f"{bar_str} {idle:>5}/{total}", style=colour)


def _wait_text(minutes: int) -> Text:
    if minutes < 15:
        return Text(f"~{minutes}m", style="green")
    elif minutes < 60:
        return Text(f"~{minutes}m", style="yellow")
    else:
        h = minutes // 60
        m = minutes % 60
        return Text(f"~{h}h{m:02d}m", style="red")


def _util_text(pct: float) -> Text:
    s = f"{pct:.0f}%"
    if pct >= 80:
        return Text(s, style="green")
    elif pct >= 55:
        return Text(s, style="yellow")
    else:
        return Text(s, style="red")


# ---------------------------------------------------------------------------
# Main display
# ---------------------------------------------------------------------------


def _build_cluster_table(data: list[ClusterStatus]) -> Table:
    table = Table(
        title="[bold cyan]Cluster Overview[/bold cyan]",
        box=box.ROUNDED,
        show_lines=True,
        header_style="bold white on #1a1a2e",
        title_style="bold",
        expand=True,
    )

    table.add_column("Cluster", style="bold", min_width=10)
    table.add_column("Status", justify="center", min_width=8)
    table.add_column("GPU model", justify="center", min_width=9)
    table.add_column("Free GPUs", justify="left", min_width=20)
    table.add_column("My jobs\nrun/pend", justify="center", min_width=9)
    table.add_column("All jobs\nrun/pend", justify="center", min_width=10)
    table.add_column("Avg wait", justify="center", min_width=8)
    table.add_column("GPU util", justify="center", min_width=8)
    table.add_column("$HOME", justify="left", min_width=18)
    table.add_column("$SCRATCH", justify="left", min_width=18)

    for c in data:
        if not c.online:
            status_cell = Text("⚠ offline", style="bold red")
        else:
            status_cell = Text("● online", style="bold green")

        my_jobs = Text(f"{c.jobs.my_running} / {c.jobs.my_pending}", style="cyan")
        all_jobs = Text(f"{c.jobs.running} / {c.jobs.pending}", style="white")

        home_bar = _bar(c.storage.home_used, c.storage.home_quota)
        scratch_bar = _bar(c.storage.scratch_used, c.storage.scratch_quota)

        # Dim the whole row if the cluster is offline
        row_style = "dim" if not c.online else ""

        table.add_row(
            Text(c.name, style="bold magenta" if c.online else "dim"),
            status_cell,
            Text(c.gpu_model, style="bright_blue"),
            _gpu_bar(c.gpu_idle, c.gpu_total),
            my_jobs,
            all_jobs,
            Text("—", style="dim") if c.avg_wait_min is None else _wait_text(c.avg_wait_min),
            Text("—", style="dim")
            if c.avg_gpu_util_pct is None
            else _util_text(c.avg_gpu_util_pct),
            home_bar,
            scratch_bar,
            style=row_style,
        )

    return table


def _build_my_jobs_table(data: list[ClusterStatus]) -> Table:
    table = Table(
        title="[bold cyan]Your Jobs Summary[/bold cyan]",
        box=box.SIMPLE_HEAVY,
        header_style="bold white on #1a1a2e",
        expand=True,
    )
    table.add_column("Cluster", style="bold magenta")
    table.add_column("Running", justify="right", style="green")
    table.add_column("Pending", justify="right", style="yellow")
    table.add_column("Cancelled", justify="right", style="red")
    table.add_column("Completed", justify="right", style="blue")

    total_run = total_pend = total_can = total_comp = 0
    for c in data:
        if not c.online:
            continue
        # Approximate user's cancelled count proportionally to their share of running jobs.
        if c.jobs.cancelled is not None:
            my_can = max(0, int(c.jobs.cancelled * c.jobs.my_running / max(c.jobs.running, 1)))
            my_can_str = str(my_can)
        else:
            my_can = 0
            my_can_str = "—"
        my_comp_str = str(c.jobs.my_completed) if c.jobs.my_completed is not None else "—"
        my_comp = c.jobs.my_completed or 0
        table.add_row(
            c.name, str(c.jobs.my_running), str(c.jobs.my_pending), my_can_str, my_comp_str
        )
        total_run += c.jobs.my_running
        total_pend += c.jobs.my_pending
        total_can += my_can
        total_comp += my_comp

    table.add_section()
    table.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold green]{total_run}[/bold green]",
        f"[bold yellow]{total_pend}[/bold yellow]",
        f"[bold red]{total_can}[/bold red]"
        if any(c.jobs.cancelled is not None for c in data if c.online)
        else "—",
        f"[bold blue]{total_comp}[/bold blue]"
        if any(c.jobs.my_completed is not None for c in data if c.online)
        else "—",
    )
    return table


def _build_legend() -> Panel:
    legend = (
        "[green]▰[/green] free GPU  "
        "[red]▱[/red] busy GPU   "
        "[green]█[/green]/[yellow]█[/yellow]/[red]█[/red] disk usage (low/med/high)   "
        "[green]●[/green] online  "
        "[red]⚠[/red] offline"
    )
    return Panel(legend, title="Legend", border_style="dim", padding=(0, 1))


async def status(clusters: list[str] | None = None):
    """Gets the status of available clusters.
    - Gives you an overview of the state of each cluster, and displays an overview of the state of your jobs across the clusters.
    - Displays the number of idle nodes, or the number of idle GPUs, or something similar, for each cluster
    """
    console = Console()
    clusters = list(clusters or [])

    if clusters:
        # Use get_remote_without_2fa_prompt directly so we never filter out the
        # "current" cluster the way login() does. A working socket for mila is
        # perfectly usable even when /home/mila is mounted locally.
        remotes = [
            r
            for r in await asyncio.gather(*(get_remote_without_2fa_prompt(c) for c in clusters))
            if r is not None
        ]
        data, is_live = await get_all_cluster_statuses(remotes=remotes)
    else:
        data, is_live = await get_all_cluster_statuses()

    if not is_live:
        console.print(
            "[yellow]No active cluster connections found. "
            "Run [bold]cluv login[/bold] first, or showing mock data.[/yellow]\n"
        )
        mock = get_mock_cluster_status()
        # When specific clusters were requested, only show mock rows for those.
        data = [c for c in mock if not clusters or c.name in clusters]
        label = "[dim](mock data)[/dim]"
    else:
        label = "[dim](live data)[/dim]"

    console.print()
    console.rule(f"[bold cyan]cluv status[/bold cyan]  {label}")
    console.print()

    console.print(_build_cluster_table(data))
    console.print()
    console.print(_build_my_jobs_table(data))
    console.print()
    console.print(_build_legend())
    console.print()
