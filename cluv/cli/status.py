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

from cluv.alliance_status import ServiceStatus as AllianceServiceStatus
from cluv.alliance_status import fetch_alliance_status_async
from cluv.cache import load_jobs
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import get_config
from cluv.slurm import (
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
    running: int
    pending: int
    # subset of the above that belong to the current user
    my_running: int
    my_pending: int
    cancelled: int | None = None
    completed: int | None = None
    my_completed: int | None = None  # recently completed jobs for the current user


@dataclass
class LiveJobInfo:
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
    jobs: JobStats
    storage: StorageStats


def get_default_cluster_status(cluster: str) -> ClusterStatus:
    return ClusterStatus(
        name=cluster,
        online=False,
        gpu_idle=0,
        gpu_total=0,
        gpu_model="?",
        jobs=JobStats(running=0, pending=0, my_running=0, my_pending=0),
        storage=StorageStats(home_used=0, home_quota=0, scratch_used=0, scratch_quota=0),
    )


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


async def fetch_live_job_info(
    # remote: Remote, job_ids: list[int]
    cluster: str, job_ids: list[int]
) -> dict[int, LiveJobInfo]:
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
        parts = line.strip().split("|")
        if len(parts) < 5:
            continue
        job_id_str, state, start_str, submit_str, elapsed = parts[:5]
        try:
            job_id = int(job_id_str.strip())
        except ValueError:
            continue

        state = state.strip()
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

        result[job_id] = LiveJobInfo(state=state, elapsed=elapsed_out, wait_time=wait_time)

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
    elif state in ("FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED"):
        return Text(state, style="red")
    return Text(state or "—", style="dim")


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


def _alliance_text(status: str | None) -> Text:
    if status == "operational":
        return Text("ok", style="green")
    if status == "degraded":
        return Text("partial", style="yellow")
    if status == "outage":
        return Text("down", style="bold red")
    if status == "scheduled":
        return Text("planned", style="blue")
    if status == "decommissioned":
        return Text("decommissioned", style="dim")
    return Text("—", style="dim")


async def _fetch_alliance_safe() -> list[AllianceServiceStatus]:
    try:
        return await fetch_alliance_status_async()
    except Exception as exc:
        logger.warning(f"Could not fetch Alliance status: {exc}")
        return []


# ---------------------------------------------------------------------------
# Main display
# ---------------------------------------------------------------------------


def _build_cluster_table(
    data: list[ClusterStatus],
    alliance_map: dict[str, AllianceServiceStatus] | None = None,
) -> Table:
    table = Table(
        title="Cluster Overview",
        box=box.ROUNDED,
        show_lines=True,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
        expand=True,
    )

    table.add_column("Cluster", style="bold", ratio=1)
    table.add_column("Alliance", justify="center", ratio=1)
    table.add_column("GPU model", justify="center", ratio=2)
    table.add_column("Free GPUs", justify="left", ratio=1)
    table.add_column("My jobs\nrun/pend", justify="center", ratio=1)
    table.add_column("All jobs\nrun/pend", justify="center", ratio=1)
    table.add_column("$HOME", justify="left", ratio=2)
    table.add_column("$SCRATCH", justify="left", ratio=2)

    for c in data:
        conn = Text("● ", style="bold green") if c.online else Text("⚠ ", style="bold red")
        svc = alliance_map.get(c.name.lower()) if alliance_map else None
        my_jobs = Text(f"{c.jobs.my_running} / {c.jobs.my_pending}", style="cyan")
        all_jobs = Text(f"{c.jobs.running} / {c.jobs.pending}", style="white")

        home_bar = _bar(c.storage.home_used, c.storage.home_quota)
        scratch_bar = _bar(c.storage.scratch_used, c.storage.scratch_quota)

        row_style = "dim" if not c.online else ""

        table.add_row(
            conn + Text(c.name, style="bold magenta" if c.online else "bold bright_black"),
            _alliance_text(svc.status if svc else None),
            Text(c.gpu_model, style="bright_blue"),
            _gpu_bar(c.gpu_idle, c.gpu_total),
            my_jobs,
            all_jobs,
            home_bar,
            scratch_bar,
            style=row_style,
        )

    return table


def _build_cluv_jobs_table(live_info: dict[int, LiveJobInfo]) -> Table:
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
    table.add_column("Job script")
    table.add_column("Waiting time")
    table.add_column("Elapsed time")

    for job in load_jobs():
        info = live_info.get(job.job_id)

        try:
            submitted_str = (
                datetime.fromisoformat(job.submitted_at)
                .astimezone()
                .strftime("%b %d %H:%M")
            )
        except (ValueError, TypeError):
            submitted_str = job.submitted_at

        if info is not None:
            state_cell = _state_text(info.state)
            wait_cell = info.wait_time or "?"
            elapsed_cell = info.elapsed or "?"
        else:
            state_cell = Text("?", style="dim")
            wait_cell = "?"
            elapsed_cell = "?"

        table.add_row(
            job.cluster,
            str(job.job_id),
            job.git_commit[:7],
            submitted_str,
            state_cell,
            job.job_script,
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
        "[green]█[/green]/[yellow]█[/yellow]/[red]█[/red] disk usage (low/med/high)  "
        "Alliance: [green]ok[/green] / [yellow]partial[/yellow] / [bold red]down[/bold red] / [blue]planned[/blue]"
    )
    return Panel(legend, title="Legend", border_style="dim", padding=(0, 1))


def _build_alliance_incidents_table(
    data: list[ClusterStatus],
    alliance_map: dict[str, AllianceServiceStatus],
) -> Table | None:
    rows = [
        (c.name, alliance_map[c.name.lower()])
        for c in data
        if c.name.lower() in alliance_map
        and alliance_map[c.name.lower()].status != "operational"
        and alliance_map[c.name.lower()].incidents
    ]
    if not rows:
        return None

    table = Table(
        title="Alliance Incidents",
        box=box.SIMPLE_HEAVY,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
        expand=True,
    )
    table.add_column("Cluster", style="bold magenta")
    table.add_column("Status", justify="center")
    table.add_column("Incident")
    table.add_column("Start", justify="center")
    table.add_column("End", justify="center")

    for cluster_name, svc in rows:
        for i, inc in enumerate(svc.incidents):
            start = inc.start.strftime("%b %d %H:%M") if inc.start else "—"
            end = inc.end.strftime("%b %d %H:%M") if inc.end else "—"
            table.add_row(
                cluster_name if i == 0 else "",
                _alliance_text(svc.status) if i == 0 else Text(""),
                inc.title,
                start,
                end,
            )

    return table


async def status(table: str) -> None:
    """Gets the status of available clusters.
    - Gives you an overview of the state of each cluster, and displays an overview of the state of your jobs across the clusters.
    - Displays the number of idle nodes, or the number of idle GPUs, or something similar, for each cluster
    """
    console = Console()
    clusters = get_config().clusters_names

    # Query clusters and Alliance status page in parallel
    with console.status("Fetching clusters status..."):
        cluster_statuses, alliance_statuses = await asyncio.gather(
            asyncio.gather(*(get_cluster_status(c) for c in clusters)),
            _fetch_alliance_safe(),
        )
    data: list[ClusterStatus] = list(cluster_statuses)
    alliance_map = {s.name.lower(): s for s in alliance_statuses}

    # Show a tip message if all clusters are offline, which likely means the user hasn't logged in yet (no control sockets).
    if all(not c.online for c in data):
        console.print(
            "[yellow]No active connections to any clusters found. Run [bold]cluv login[/bold] first.[/yellow]"
        )

    console.print()
    console.rule("[bold cyan]cluv status[/bold cyan]")
    console.print()

    # Fetch live job info for all cached jobs that belong to reachable clusters.
    live_info: dict[int, LiveJobInfo] = {}
    cached_jobs = load_jobs()
    cluster_jobs: dict[str, list[int]] = {}
    for job in cached_jobs:
        if job.cluster in clusters:
            cluster_jobs.setdefault(job.cluster, []).append(job.job_id)

        results = await asyncio.gather(
            *(
                fetch_live_job_info(c, ids)
                for c, ids in cluster_jobs.items()
            )
        )
        live_info = {jid: info for cluster_result in results for jid, info in cluster_result.items()}

    if table in ("clusters", "all"):
        console.print(_build_cluster_table(data, alliance_map))
        console.print(_build_legend())
        incidents_table = _build_alliance_incidents_table(data, alliance_map)
        if incidents_table:
            console.print()
            console.print(incidents_table)
        console.print()
    if table in ("jobs", "all"):
        console.print(_build_cluv_jobs_table(live_info))
        console.print()
