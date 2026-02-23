from __future__ import annotations

import random
from dataclasses import dataclass

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

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
    cancelled: int
    completed: int
    # subset of the above that belong to the current user
    my_running: int
    my_pending: int


@dataclass
class StorageStats:
    """Disk usage as (used_gib, quota_gib) for $HOME and $SCRATCH."""

    home_used: float
    home_quota: int
    scratch_used: float
    scratch_quota: int


@dataclass
class ClusterStatus:
    name: str
    online: bool
    gpu_idle: int
    gpu_total: int
    gpu_model: str
    jobs: JobStats
    storage: StorageStats
    avg_wait_min: int  # estimated queue wait time in minutes
    avg_gpu_util_pct: float  # average GPU utilisation across running jobs


def get_mock_cluster_status(username: str = "you") -> list[ClusterStatus]:
    """Return fake but plausible status data for every known cluster.

    This function is intentionally free of any UI logic so it can be swapped
    out for a real implementation that queries Slurm / the cluster APIs.
    """
    rng = random.Random(MOCK_DATA_SEED)

    gpu_models = ["A100", "H100", "V100", "A40", "RTX 8000"]

    results: list[ClusterStatus] = []
    for cluster in CLUSTERS:
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
                    cancelled=cancelled,
                    completed=completed,
                    my_running=my_running,
                    my_pending=my_pending,
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
            _wait_text(c.avg_wait_min),
            _util_text(c.avg_gpu_util_pct),
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

    total_run = total_pend = total_can = 0
    for c in data:
        if not c.online:
            continue
        # Approximate user's cancelled count proportionally to their share of running jobs.
        my_can = max(
            0, int(c.jobs.cancelled * c.jobs.my_running / max(c.jobs.running, 1))
        )
        table.add_row(
            c.name, str(c.jobs.my_running), str(c.jobs.my_pending), str(my_can)
        )
        total_run += c.jobs.my_running
        total_pend += c.jobs.my_pending
        total_can += my_can

    table.add_section()
    table.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold green]{total_run}[/bold green]",
        f"[bold yellow]{total_pend}[/bold yellow]",
        f"[bold red]{total_can}[/bold red]",
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


def status():
    """Gets the status of available clusters.
    - Gives you an overview of the state of each cluster, and displays an overview of the state of your jobs across the clusters.
    - Displays the number of idle nodes, or the number of idle GPUs, or something similar, for each cluster
    """
    console = Console()
    data = get_mock_cluster_status()

    console.print()
    console.rule("[bold cyan]cluv status[/bold cyan]  [dim](mock data)[/dim]")
    console.print()

    console.print(_build_cluster_table(data))
    console.print()
    console.print(_build_my_jobs_table(data))
    console.print()
    console.print(_build_legend())
    console.print()
