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
    """Return (sum_jobs, max_nodes) for the GPU row in one section block.

    Each cell is ``node_jobs:core_jobs``.  Jobs are in exactly one walltime
    column, so summing (node + core) across all columns gives the true job
    count.  Nodes appear in multiple columns (same node listed under every
    walltime it supports), so taking the max gives the true node count.

    sum_jobs  → sum of (node_jobs + core_jobs) across all walltime columns.
    max_nodes → maximum of node_jobs across all walltime columns.
    """
    for line in section_lines:
        if line.strip().startswith("GPU"):
            cells = _CELL_RE.findall(line)
            if not cells:
                return 0, 0
            node_counts = [int(n) for n, _ in cells]
            core_counts = [int(c) if c != "-" else 0 for _, c in cells]
            sum_jobs = sum(n + c for n, c in zip(node_counts, core_counts))
            return sum_jobs, max(node_counts)
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
# sinfo --noheader -N -o '%N %t %G' | sort -u | grep gpu
# ---------------------------------------------------------------------------

# Matches one GRES entry like:
#   gpu:h100:4(S:0-1)       → ('h100', '4')
#   gpu:a100:8               → ('a100', '8')
#   gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3)  → ('nvidia_h100_80gb_hbm3_3g.40gb', '4')
_GRES_RE = re.compile(r"gpu:([^:(,]+):(\d+)")

# Detects a MIG profile suffix like "3g.40gb" or "1g.10gb" in a GRES model name
_MIG_PROFILE_RE = re.compile(r"(\d+)g\.\d+gb", re.IGNORECASE)

# Extracts the base model token (letters + digits) for normalization
_MODEL_TOKEN_RE = re.compile(r"([a-z]+\d+[a-z]*)", re.IGNORECASE)

# Node states that count as idle (sinfo uses mixed-case variants)
_IDLE_STATES = {"idle", "idle~", "idle+"}


def _normalize_gpu_model(raw: str) -> str:
    """Normalize a raw GRES GPU model name to a short human-readable form.

    Examples:
        "h100"                              → "H100"
        "a100"                              → "A100"
        "nvidia_h100_80gb_hbm3_3g.40gb"    → "H100"
    """
    # Strip optional "nvidia_" vendor prefix
    clean = re.sub(r"^nvidia_", "", raw, flags=re.IGNORECASE)
    m = _MODEL_TOKEN_RE.search(clean)
    return m.group(1).upper() if m else raw.upper()


def _mig_physical_gpus(entries: list[tuple[str, int]]) -> int | None:
    """Return the number of physical GPUs represented by a list of MIG GRES entries.

    MIG profile names embed the compute slice fraction as ``<g_val>g.<mem>gb``
    (e.g. ``3g.40gb``).  An H100 has 7 compute slices, so:

        physical_gpus = sum(g_val * count) // 7

    Returns *None* if any entry lacks a parseable MIG profile (not a pure MIG node).
    """
    total_compute = 0
    for model, count in entries:
        m = _MIG_PROFILE_RE.search(model)
        if not m:
            return None  # mixed or non-MIG node
        total_compute += int(m.group(1)) * count
    return total_compute // 7


def parse_sinfo_nodes(output: str) -> tuple[int, int, list[str]]:
    """Parse ``sinfo --noheader -N -o '%N %t %G' | sort -u | grep gpu`` output.

    Each line has the form ``<nodename> <state> <gres_field>`` where the GRES
    field may contain multiple comma-separated entries, e.g.::

        node01 idle  gpu:h100:4(S:0-1)
        rg01   alloc gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)

    The ``sort -u`` upstream ensures each (nodename, state, gres) triple is
    unique, so nodes that belong to multiple Slurm partitions are not counted
    more than once.

    MIG nodes are handled by reconstructing the physical GPU count from the
    per-slice g-values (``sum(g_val * count) // 7`` for H100).

    Returns:
        gpu_idle  – total idle physical GPUs
        gpu_total – total physical GPUs across all nodes
        models    – sorted unique GPU model names (e.g. ``["H100", "A100"]``)
    """
    gpu_idle = 0
    gpu_total = 0
    models: set[str] = set()

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        _node, state, gres_field = parts[0], parts[1].lower(), parts[2]

        matches = _GRES_RE.findall(gres_field)
        if not matches:
            continue

        entries: list[tuple[str, int]] = [
            (model, int(count_str)) for model, count_str in matches
        ]

        for model, _ in entries:
            models.add(_normalize_gpu_model(model))

        # Compute physical GPU count for this node.
        # If all GRES entries are MIG slices, reconstruct the physical count.
        # Otherwise sum only the non-MIG GRES entries.
        node_gpus = _mig_physical_gpus(entries)
        if node_gpus is None:
            node_gpus = sum(c for m, c in entries if not _MIG_PROFILE_RE.search(m))

        gpu_total += node_gpus
        if state in _IDLE_STATES:
            gpu_idle += node_gpus

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
