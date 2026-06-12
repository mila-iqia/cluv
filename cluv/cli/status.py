from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cluv.cache import Job, load_jobs
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import get_cluv_config
from cluv.slurm import (
    FAILED_JOB_STATES,
    StorageStats,
    parse_disk_quota,
    parse_diskusage_report,
    parse_partition_stats,
    parse_savail,
    parse_sinfo_nodes,
)

logger = logging.getLogger(__name__)
__all__ = ["status"]


@dataclass
class JobStats:
    my_running: int
    my_pending: int
    my_cancelled: int
    my_completed: int


@dataclass
class LiveJobInfo:
    cluster: str
    state: str
    elapsed: str | None  # sacct Elapsed field (HH:MM:SS or D-HH:MM:SS)
    wait_time: str | None  # formatted time from Submit to Start (or to now if still pending)


@dataclass
class ClusterStatus:
    name: str
    online: bool
    gpu_idle: int
    gpu_total: int
    gpu_model: str
    storage: StorageStats


def get_default_cluster_status(cluster: str) -> ClusterStatus:
    return ClusterStatus(
        name=cluster,
        online=False,
        gpu_idle=0,
        gpu_total=0,
        gpu_model="?",
        storage=StorageStats(home_used=0, home_quota=0, scratch_used=0, scratch_quota=0),
    )


# All commands are separated by a sentinel so we can split a single SSH output.
_SEP = "---CLUV-SEP---"

SINFO_LIST_GPUS = 'sinfo --noheader -N -o "%N %t %G" 2>/dev/null | sort -u | grep gpu'

# Script for DRAC clusters (partition-stats + diskusage_report, no savail/disk-quota)
_REMOTE_SCRIPT_DRAC = f"""
partition-stats 2>/dev/null; echo {_SEP}
{SINFO_LIST_GPUS}; echo {_SEP}
timeout 1 diskusage_report 2>/dev/null; echo {_SEP}
echo {_SEP}
echo {_SEP}
"""

# Script for the Mila cluster (savail + disk-quota, no partition-stats/diskusage_report)
_REMOTE_SCRIPT_MILA = f"""
echo {_SEP}
{SINFO_LIST_GPUS}; echo {_SEP}
echo {_SEP}
savail 2>/dev/null; echo {_SEP}
disk-quota 2>/dev/null; echo {_SEP}
"""

_MILA_CLUSTERS = {"mila"}


async def fetch_live_job_info(cluster: str, job_ids: list[int]) -> dict[int, LiveJobInfo]:
    """Batch-fetch Slurm state, elapsed, and wait-time for a list of job IDs."""
    ids_str = ",".join(str(jid) for jid in job_ids)
    cmd = (
        f"sacct -j {ids_str} --format=JobID,State,Start,Submit,Elapsed"
        f" --noheader --allocations --parsable2 2>/dev/null"
    )
    try:
        remote = await get_remote_without_2fa_prompt(cluster)
        if remote is None:
            return {}
        raw = await remote.get_output(cmd, hide=True, warn=True, display=False)
    except Exception:
        return {}

    result: dict[int, LiveJobInfo] = {}
    now = datetime.now(timezone.utc)
    _fmt = "%Y-%m-%dT%H:%M:%S"

    for line in raw.splitlines():
        # Should have 5 columns
        parts = line.strip().split("|")
        if len(parts) != 5:
            continue
        job_id_str, state, start_str, submit_str, elapsed = parts

        elapsed_val = elapsed.strip()
        elapsed_out = elapsed_val if elapsed_val and elapsed_val != "00:00:00" else None

        wait_time = None
        try:
            submit_dt = datetime.strptime(submit_str.strip(), _fmt).replace(tzinfo=timezone.utc)
            start = start_str.strip()
            if start and start not in ("Unknown", "None"):
                start_dt = datetime.strptime(start, _fmt).replace(tzinfo=timezone.utc)
                delta_s = int((start_dt - submit_dt).total_seconds())
            else:
                delta_s = int((now - submit_dt).total_seconds())
            wait_time = _format_duration(max(delta_s, 0))
        except (ValueError, OverflowError):
            pass

        job_id = int(job_id_str.strip())
        result[job_id] = LiveJobInfo(
            cluster=cluster, state=state.strip(), elapsed=elapsed_out, wait_time=wait_time
        )

    return result


async def get_cluster_status(cluster: str) -> ClusterStatus:
    """Fetch live Slurm data from a remote cluster and return a ClusterStatus.

    Uses a single SSH round-trip. Falls back gracefully when commands are
    unavailable (e.g. partition-stats is DRAC-only).
    """
    # Use get_remote_without_2fa_prompt directly so we never filter out the
    # "current" cluster the way login() does. A working socket for mila is
    # perfectly usable even when /home/mila is mounted locally.
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        return get_default_cluster_status(cluster)

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
        return get_default_cluster_status(cluster)

    parts = raw.split(_SEP)
    partition_stats_out, sinfo_out, diskusage_out, savail_out, disk_quota_out = parts[:5]

    # --- GPU info: prefer savail (Mila) over sinfo (DRAC) ---
    savail_idle, savail_total, savail_models = parse_savail(savail_out)
    if savail_total > 0:
        gpu_idle, gpu_total, models = savail_idle, savail_total, savail_models
    else:
        gpu_idle, gpu_total, models = parse_sinfo_nodes(sinfo_out)
    gpu_model = ", ".join(models) if models else "?"

    # --- Partition stats can give us node counts which are a useful
    #     fallback when GPU counts aren't available --
    has_partition_stats = bool(partition_stats_out.strip())
    if has_partition_stats:
        ps = parse_partition_stats(partition_stats_out)
        # If neither savail nor sinfo gave us GPU counts, fall back to
        # partition-stats node counts (less precise but better than nothing).
        if gpu_total == 0:
            gpu_idle = ps["gpu_idle_nodes"]
            gpu_total = ps["gpu_total_nodes"]

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
        storage=storage,
    )


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def _format_duration(total_seconds: int) -> str:
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    elif m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _state_text(state: str) -> Text:
    state = state.strip().upper()
    if state == "RUNNING":
        return Text(state, style="green")
    elif state == "PENDING":
        return Text(state, style="yellow")
    elif state in ("COMPLETED", "COMPLETING"):
        return Text(state, style="blue")
    elif state in FAILED_JOB_STATES:
        return Text(state, style="red")
    return Text(state or "—", style="dim")


def _bar(used: float, total: float, width: int = 10) -> Text:
    """Return a coloured block-character progress bar."""
    ratio = used / total if total else 0
    filled = int(ratio * width)
    bar_str = "▰" * filled + "▱" * (width - filled)
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


# ---------------------------------------------------------------------------
# Main display
# ---------------------------------------------------------------------------
def _build_cluster_table(
    data: list[ClusterStatus], clusters_job_stats: dict[str, JobStats]
) -> Table:
    """Build the cluster overview table with live status info and job counts."""
    table = Table(
        title="Cluster Overview",
        box=box.ROUNDED,
        show_lines=True,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
        expand=True,
    )

    table.add_column("Cluster", style="bold", ratio=1)
    table.add_column("GPU model", justify="center", ratio=2)
    table.add_column("Free GPUs", justify="left", ratio=1)
    table.add_column("My jobs\nrun / pend / fail / comp", justify="center", ratio=2)
    table.add_column("Storage used", justify="left", ratio=2)

    for c in data:
        status = Text("● ", style="bold green") if c.online else Text("⚠ ", style="bold red")
        job_stats = clusters_job_stats.get(
            c.name, JobStats(my_running=0, my_cancelled=0, my_completed=0, my_pending=0)
        )
        my_jobs = Text(
            f"{job_stats.my_running} / {job_stats.my_pending} / {job_stats.my_cancelled} / {job_stats.my_completed}",
            style="cyan",
        )

        home_bar = Text("$HOME     ", style="bold") + _bar(
            c.storage.home_used, c.storage.home_quota
        )
        scratch_bar = Text("$SCRATCH  ", style="bold") + _bar(
            c.storage.scratch_used, c.storage.scratch_quota
        )

        # Dim the whole row if the cluster is offline
        row_style = "dim" if not c.online else ""

        table.add_row(
            status + Text(c.name, style="bold magenta" if c.online else "bold bright_black"),
            Text(c.gpu_model, style="bright_blue") if c.online else "-",
            _gpu_bar(c.gpu_idle, c.gpu_total) if c.online else "-",
            my_jobs if c.online else "-",
            home_bar + "\n" + scratch_bar if c.online else "-",
            style=row_style,
        )

    return table


def _build_cluv_jobs_table(cached_jobs: list[Job], live_info: dict[int, LiveJobInfo]) -> Table:
    """Build the jobs overview table with one row per cached job, enriched with live status info."""
    table = Table(
        title="Jobs Overview",
        box=box.SIMPLE_HEAVY,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
        expand=True,
    )

    table.add_column("Cluster", style="bold magenta")
    table.add_column("Job ID", style="bold magenta")
    table.add_column("Git commit")
    table.add_column("Submitted at")
    table.add_column("Job status")
    table.add_column("Waiting time")
    table.add_column("Elapsed time")

    for job in cached_jobs:
        info = live_info.get(job.job_id)

        try:
            submitted_str = (
                datetime.fromisoformat(job.submitted_at).astimezone().strftime("%b %d %H:%M")
            )
        except (ValueError, TypeError):
            submitted_str = job.submitted_at

        if info is not None:
            state_cell = _state_text(info.state)
            wait_cell = info.wait_time or "-"
            elapsed_cell = info.elapsed or "-"
        else:
            state_cell = Text("-", style="dim")
            wait_cell = "-"
            elapsed_cell = "-"

        table.add_row(
            job.cluster,
            str(job.job_id),
            job.git_commit[:7],
            submitted_str,
            state_cell,
            wait_cell,
            elapsed_cell,
        )

    return table


def _build_legend() -> Panel:
    legend = (
        "[green]●[/green] connected  "
        "[red]⚠[/red] disconnected  "
        "[green]▰[/green] free GPU  "
        "[red]▱[/red] busy GPU   "
        "[green]▰[/green]/[yellow]▰[/yellow]/[red]▰[/red] disk usage (low/med/high)"
    )
    return Panel(legend, title="Legend", border_style="dim", padding=(0, 1))


async def get_job_infos(
    cached_jobs: list[Job], clusters: list[str]
) -> tuple[dict[int, LiveJobInfo], dict[str, JobStats]]:
    """Fetch live job info for all cached jobs, and count job statuses per cluster."""
    # Regroup jobs by cluster
    cluster_jobs: dict[str, list[int]] = {}
    for job in cached_jobs:
        if job.cluster in clusters:
            cluster_jobs.setdefault(job.cluster, []).append(job.job_id)

    # Fetch live job info for all cached jobs
    results = await asyncio.gather(
        *(fetch_live_job_info(c, ids) for c, ids in cluster_jobs.items())
    )
    live_info = {jid: info for cluster_result in results for jid, info in cluster_result.items()}

    # Count jobs status per cluster
    clusters_job_stats: dict[str, JobStats] = {}
    for info in live_info.values():
        cluster_stats = clusters_job_stats.setdefault(
            info.cluster, JobStats(my_running=0, my_cancelled=0, my_completed=0, my_pending=0)
        )
        if info.state == "RUNNING":
            cluster_stats.my_running += 1
        elif info.state == "PENDING":
            cluster_stats.my_pending += 1
        elif info.state in FAILED_JOB_STATES:
            cluster_stats.my_cancelled += 1
        elif info.state in ("COMPLETED", "COMPLETING"):
            cluster_stats.my_completed += 1

    return live_info, clusters_job_stats


async def status(table: str) -> None:
    """Show status of clusters and jobs.

    Parameters:
        table: Which table(s) to show: "clusters", "jobs", or "all".

    Returns:
        None

    The "clusters" table shows live info about each cluster's GPU availability and storage usage,
    along with counts of the user's running/pending/failed/completed jobs on that cluster.

    The "jobs" table shows one row per job from the cache, with live status info (state,
    elapsed time, wait time).
    """
    console = Console()
    clusters = get_cluv_config().clusters_names

    console.print()
    console.rule("[bold cyan]cluv status[/bold cyan]")
    console.print()

    # Load cached jobs
    cached_jobs = load_jobs()

    with console.status("Fetching jobs status..."):
        jobs_status, clusters_job_stats = await get_job_infos(cached_jobs, clusters)

    if table in ("clusters", "all"):
        # Query clusters in parallel
        with console.status("Fetching clusters status..."):
            clusters_status: list[ClusterStatus] = [
                d for d in await asyncio.gather(*(get_cluster_status(c) for c in clusters))
            ]

        # Show a tip message if all clusters are offline.
        if all(not c.online for c in clusters_status):
            console.print(
                (
                    "[yellow]No active connections to any clusters found. "
                    "Run [bold]cluv login[/bold] first.[/yellow]"
                )
            )

        console.print(_build_cluster_table(clusters_status, clusters_job_stats))
        console.print(_build_legend())
        console.print()

    if table in ("jobs", "all"):
        console.print(_build_cluv_jobs_table(cached_jobs, jobs_status))
        console.print()
