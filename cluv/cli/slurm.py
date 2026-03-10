"""Pure parsing functions for Slurm command output.

All functions are free of I/O and can be unit-tested against fixture strings.
"""

from __future__ import annotations

import re

from cluv.cli.status import StorageStats


# ---------------------------------------------------------------------------
# partition-stats (DRAC-only)
# ---------------------------------------------------------------------------

# Matches a row like:
#   GPU       |     8:-    |    9:-    |    2:-    |    0:-    |    0:-    |
# or:
#   Regular   |     0:0    |    0:1    |    0:0    |    0:0    |    0:0    |
_CELL_RE = re.compile(r"\|\s*([\d]+):([\d-]+)\s*")


def _parse_section_gpu_row(section_lines: list[str]) -> tuple[int, int]:
    """Return (sum_node, max_node) for the GPU row in one section block.

    sum_node  → total across all walltime columns (correct for job counts).
    max_node  → maximum across all walltime columns (correct for node counts,
                since the same physical node appears in multiple columns).
    """
    for line in section_lines:
        if line.strip().startswith("GPU"):
            cells = _CELL_RE.findall(line)
            node_counts = [int(n) for n, _ in cells]
            if not node_counts:
                return 0, 0
            return sum(node_counts), max(node_counts)
    return 0, 0


_SECTION_HEADERS = {
    "queued": "Queued",
    "running": "Running",
    "idle": "Idle",
    "total": "Total",
}


def _split_sections(output: str) -> dict[str, list[str]]:
    """Split partition-stats output into named sections."""
    sections: dict[str, list[str]] = {k: [] for k in _SECTION_HEADERS}
    current: str | None = None
    for line in output.splitlines():
        for key, header in _SECTION_HEADERS.items():
            if header in line and "Number" in line:
                current = key
                break
        if current:
            sections[current].append(line)
    return sections


def parse_partition_stats(output: str) -> dict:
    """Parse the output of the `partition-stats` command.

    Returns a dict with keys:
        jobs_running  – total GPU jobs running (sum across walltime partitions)
        jobs_pending  – total GPU jobs queued  (sum across walltime partitions)
        gpu_idle_nodes  – idle GPU nodes (max across walltime partitions)
        gpu_total_nodes – total GPU nodes (max across walltime partitions)
    """
    sections = _split_sections(output)

    jobs_running, _ = _parse_section_gpu_row(sections["running"])
    jobs_pending, _ = _parse_section_gpu_row(sections["queued"])
    _, gpu_idle_nodes = _parse_section_gpu_row(sections["idle"])
    _, gpu_total_nodes = _parse_section_gpu_row(sections["total"])

    return {
        "jobs_running": jobs_running,
        "jobs_pending": jobs_pending,
        "gpu_idle_nodes": gpu_idle_nodes,
        "gpu_total_nodes": gpu_total_nodes,
    }


# ---------------------------------------------------------------------------
# sinfo --noheader -N -o '%t %G' | grep gpu
# ---------------------------------------------------------------------------

# Matches GRES strings like:
#   gpu:h100:4(S:0-1)
#   gpu:a100:8
_GRES_RE = re.compile(r"gpu:([^:(]+):(\d+)")

# Node states that count as idle (sinfo uses mixed-case variants)
_IDLE_STATES = {"idle", "idle~", "idle+"}


def parse_sinfo_nodes(output: str) -> tuple[int, int, list[str]]:
    """Parse the output of ``sinfo --noheader -N -o '%t %G' | grep gpu``.

    Each line looks like:
        idle gpu:h100:4(S:0-1)
        alloc gpu:h200:8(S:0-1)
        mix   gpu:h100:4(S:0-1)

    Returns:
        gpu_idle  – total idle GPUs (sum over idle nodes)
        gpu_total – total GPUs across all nodes
        models    – sorted unique list of GPU model names (e.g. ["h100", "h200"])
    """
    gpu_idle = 0
    gpu_total = 0
    models: set[str] = set()

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        state, gres = parts[0].lower(), parts[1]
        m = _GRES_RE.search(gres)
        if not m:
            continue
        model, count_str = m.group(1), m.group(2)
        count = int(count_str)
        models.add(model.upper())
        gpu_total += count
        if state in _IDLE_STATES:
            gpu_idle += count

    return gpu_idle, gpu_total, sorted(models)


# ---------------------------------------------------------------------------
# savail (Mila-specific)
# ---------------------------------------------------------------------------

# Matches data lines like:
#   a100                 15 / 32
#   rtx8000             130 / 376
_SAVAIL_LINE_RE = re.compile(r"^(\w+)\s+(\d+)\s*/\s*(\d+)")


def parse_savail(output: str) -> tuple[int, int, list[str]]:
    """Parse the output of the Mila-specific ``savail`` command.

    Returns:
        gpu_idle  – total available (idle) GPUs across all types
        gpu_total – total GPUs across all types
        models    – sorted list of GPU model names in upper-case
    """
    gpu_idle = 0
    gpu_total = 0
    models: list[str] = []

    for line in output.splitlines():
        m = _SAVAIL_LINE_RE.match(line.strip())
        if not m:
            continue
        model, avail, total = m.group(1), int(m.group(2)), int(m.group(3))
        gpu_idle += avail
        gpu_total += total
        models.append(model.upper())

    return gpu_idle, gpu_total, sorted(models)


# ---------------------------------------------------------------------------
# disk-quota (Mila-specific: lfs quota for $HOME + beegfs for $SCRATCH)
# ---------------------------------------------------------------------------

# lfs quota data line (after the "Filesystem used quota limit ..." header):
#   /home/mila  99.99G      0k    100G       -  921718 ...
# Columns: filesystem, used, soft-quota, hard-limit, ...
# We want column 1 (used) and column 3 (hard limit = effective quota).
_LFS_DATA_RE = re.compile(
    r"^\s+\S*/home\S*\s+"          # filesystem path containing "home"
    r"([\d.]+)\s*([KMGTP]i?[Bb]?)"  # used  + unit
    r"\s+\S+"                        # soft quota (skip)
    r"\s+([\d.]+)\s*([KMGTP]i?[Bb]?)",  # hard limit + unit
)

# beegfs-ctl data line:
#   normandf|1471600598||   76.61 GiB|    5.00 TiB||   687792|unlimited
_BEEGFS_DATA_RE = re.compile(
    r"\w+\|\d+\|\|"
    r"\s*([\d.]+)\s*(GiB|TiB|MiB|KiB|GiB)"  # used + unit
    r"\|\s*([\d.]+)\s*(GiB|TiB|MiB|KiB|GiB)",  # hard quota + unit
)

_UNIT_TO_GIB: dict[str, float] = {
    "K": 1 / 1024**2, "KB": 1 / 1024**2, "KiB": 1 / 1024**2,
    "M": 1 / 1024,    "MB": 1 / 1024,    "MiB": 1 / 1024,
    "G": 1.0,         "GB": 1.0,         "GiB": 1.0,
    "T": 1024.0,      "TB": 1024.0,      "TiB": 1024.0,
    "P": 1024**2,     "PB": 1024**2,     "PiB": 1024**2,
}


def _to_gib(value: str, unit: str) -> float:
    return float(value) * _UNIT_TO_GIB.get(unit, 1.0)


def parse_disk_quota(output: str) -> StorageStats:
    """Parse the output of the Mila-specific ``disk-quota`` command.

    The command combines ``lfs quota`` (for $HOME) and ``beegfs-ctl``
    (for $SCRATCH) into one output. Returns values in GiB.
    """
    home_used = home_quota = scratch_used = scratch_quota = 0.0

    for line in output.splitlines():
        m = _LFS_DATA_RE.match(line)
        if m:
            home_used = _to_gib(m.group(1), m.group(2))
            home_quota = _to_gib(m.group(3), m.group(4))
            continue

        m = _BEEGFS_DATA_RE.search(line)
        if m:
            scratch_used = _to_gib(m.group(1), m.group(2))
            scratch_quota = _to_gib(m.group(3), m.group(4))

    return StorageStats(
        home_used=home_used,
        home_quota=home_quota,
        scratch_used=scratch_used,
        scratch_quota=scratch_quota,
    )


# ---------------------------------------------------------------------------
# diskusage_report
# ---------------------------------------------------------------------------

# Matches lines like:
#   /home (user normandf)    20GiB/  25GiB    208K/ 250K
#   /scratch (user normandf) 148GiB/ 500GiB   418K/ 500K
_QUOTA_RE = re.compile(r"([\d.]+)\s*GiB\s*/\s*([\d.]+)\s*GiB")


def parse_diskusage_report(output: str) -> StorageStats:
    """Parse the output of ``diskusage_report``.

    Returns a StorageStats with values in GiB.
    Falls back to 0.0 for any filesystem not found in the output.
    """
    home_used = home_quota = scratch_used = scratch_quota = 0.0

    for line in output.splitlines():
        m = _QUOTA_RE.search(line)
        if not m:
            continue
        used, quota = float(m.group(1)), float(m.group(2))
        if "/home" in line:
            home_used, home_quota = used, quota
        elif "/scratch" in line:
            scratch_used, scratch_quota = used, quota

    return StorageStats(
        home_used=home_used,
        home_quota=home_quota,
        scratch_used=scratch_used,
        scratch_quota=scratch_quota,
    )
