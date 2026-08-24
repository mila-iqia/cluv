"""Pure parsing functions for Slurm command output.

All functions are free of I/O and can be unit-tested against fixture strings.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from cluv.remote import Remote, run

FAILED_JOB_STATES = ["FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED"]


@dataclass
class StorageStats:
    """Disk usage as (used_gib, quota_gib) for $HOME and $SCRATCH."""

    home_used: float
    home_quota: float
    scratch_used: float
    scratch_quota: float


def clean_job_state(state: str) -> str:
    if "CANCELLED by" in state:
        return "CANCELLED"
    return state


def parse_timestamp(timestamp: str) -> datetime:
    return datetime.strptime(timestamp.strip(), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def parse_slurm_time(time: str) -> timedelta:
    """Parse a time value from the sbatch format to a timedelta object.

    The SLURM time format (https://slurm.schedmd.com/sbatch.html#OPT_time) can be:
        1. days-hours:minutes:seconds
        2. days-hours:minutes
        3. days-hours
        4. hours:minutes:seconds
        5. minutes:seconds
        6. minutes
    """
    value = time.strip()
    if not value:
        raise ValueError(f"Could not parse time value: {time}")

    days, hours, minutes, seconds = 0, 0, 0, 0
    has_days = "-" in value
    if has_days:
        day_part, value = value.split("-", 1)
        if not day_part.isdigit():
            raise ValueError(f"Could not parse time value: {time}")
        days = int(day_part)

    parts = value.split(":")
    if len(parts) == 1:
        if not parts[0].isdigit():
            raise ValueError(f"Could not parse time value: {time}")
        if has_days:
            hours = int(parts[0])
        else:
            minutes = int(parts[0])
    elif len(parts) == 2:
        if not all(part.isdigit() for part in parts):
            raise ValueError(f"Could not parse time value: {time}")
        if has_days:
            hours = int(parts[0])
            minutes = int(parts[1])
        else:
            minutes = int(parts[0])
            seconds = int(parts[1])
    elif len(parts) == 3:
        if not all(part.isdigit() for part in parts):
            raise ValueError(f"Could not parse time value: {time}")
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    else:
        raise ValueError(f"Could not parse time value: {time}")

    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


async def run_saccts(
    remote: Remote | None,
    jobs: list[int],
    format: str = "State",
) -> list[str]:
    """Run sacct on the given job id(s) and return the output as a list of lines."""
    if not jobs:
        return []
    jobs_str = ",".join(str(job) for job in jobs)
    sacct_command = f"sacct -j {jobs_str} --format={format} --parsable2 --noheader --allocations"
    if remote:
        output = await remote.get_output(sacct_command, hide=True)
    else:
        result = await run(tuple(shlex.split(sacct_command)), hide=True)
        output = result.stdout.strip()
    return output.splitlines()


async def run_sacct(
    remote: Remote | None,
    jobs: str | int | list[int],
    format: str = "State",
    additional_args: str = "",
) -> str | list[str]:
    """Run sacct on the given job id(s) and return the output."""
    sacct_command = (
        f"sacct -j {jobs} --format={format} --parsable2 --noheader --allocations {additional_args}"
    )
    if remote:
        return await remote.get_output(sacct_command, hide=True)
    result = await run(tuple(shlex.split(sacct_command)), hide=True)
    return result.stdout.strip()


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

    MIG slices are kept as their own distinct GPU type (suffixed with their
    profile) rather than folded back into the base model, since a MIG slice
    is not interchangeable with a full physical GPU.

    Examples:
        "h100"                              → "H100"
        "a100"                              → "A100"
        "nvidia_h100_80gb_hbm3_3g.40gb"     → "H100-3g.40gb"
    """
    # Strip optional "nvidia_" vendor prefix
    clean = re.sub(r"^nvidia_", "", raw, flags=re.IGNORECASE)
    m = _MODEL_TOKEN_RE.search(clean)
    base = m.group(1).upper() if m else raw.upper()

    mig = _MIG_PROFILE_RE.search(clean)
    if mig:
        return f"{base}-{mig.group(0).lower()}"
    return base


def parse_sinfo_nodes(output: str) -> dict[str, tuple[int, int]]:
    """Parse ``sinfo --noheader -N -o '%N %t %G' | sort -u | grep gpu`` output.

    Each line has the form ``<nodename> <state> <gres_field>`` where the GRES
    field may contain multiple comma-separated entries, e.g.::

        node01 idle  gpu:h100:4(S:0-1)
        rg01   alloc gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)

    The ``sort -u`` upstream ensures each (nodename, state, gres) triple is
    unique, so nodes that belong to multiple Slurm partitions are not counted
    more than once.

    MIG slices are reported as their own GPU type (e.g. ``"H100-3g.40gb"``)
    rather than reconstructed into a physical GPU count, since a MIG slice
    can't be scheduled interchangeably with a full GPU.

    Returns:
        A dict mapping each GPU model/MIG-profile name to a ``(idle, total)``
        tuple of GRES counts, sorted by name.
    """
    per_model: dict[str, list[int]] = {}

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        _, state, gres_field = parts[0], parts[1].lower(), parts[2]

        matches = _GRES_RE.findall(gres_field)
        if not matches:
            continue

        for raw_model, count_str in matches:
            model = _normalize_gpu_model(raw_model)
            count = int(count_str)

            idle_total = per_model.setdefault(model, [0, 0])
            idle_total[1] += count
            if state in _IDLE_STATES:
                idle_total[0] += count

    return {model: (idle, total) for model, (idle, total) in sorted(per_model.items())}


# ---------------------------------------------------------------------------
# savail (Mila-specific)
# ---------------------------------------------------------------------------

# Matches data lines like:
#   a100                 15 / 32
#   rtx8000             130 / 376
_SAVAIL_LINE_RE = re.compile(r"^(\w+)\s+(\d+)\s*/\s*(\d+)")


def parse_savail(output: str) -> dict[str, tuple[int, int]]:
    """Parse the output of the Mila-specific ``savail`` command.

    Returns:
        A dict mapping each GPU model name (e.g. ``"A100"``) to a
        ``(idle, total)`` tuple of GPU counts, sorted by model name.
    """
    per_model: dict[str, tuple[int, int]] = {}

    for line in output.splitlines():
        m = _SAVAIL_LINE_RE.match(line.strip())
        if not m:
            continue
        model, avail, total = m.group(1), int(m.group(2)), int(m.group(3))
        per_model[model.upper()] = (avail, total)

    return dict(sorted(per_model.items()))


# ---------------------------------------------------------------------------
# disk-quota (Mila-specific: lfs quota for $HOME + beegfs for $SCRATCH)
# ---------------------------------------------------------------------------

# lfs quota data line (after the "Filesystem used quota limit ..." header):
#   /home/mila  99.99G      0k    100G       -  921718 ...
# Columns: filesystem, used, soft-quota, hard-limit, ...
# We want column 1 (used) and column 3 (hard limit = effective quota).
_LFS_DATA_RE = re.compile(
    r"^\s+\S*/home\S*\s+"  # filesystem path containing "home"
    r"([\d.]+)\s*([KMGTP]i?[Bb]?)"  # used  + unit
    r"\s+\S+"  # soft quota (skip)
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
    "K": 1 / 1024**2,
    "KB": 1 / 1024**2,
    "KiB": 1 / 1024**2,
    "M": 1 / 1024,
    "MB": 1 / 1024,
    "MiB": 1 / 1024,
    "G": 1.0,
    "GB": 1.0,
    "GiB": 1.0,
    "T": 1024.0,
    "TB": 1024.0,
    "TiB": 1024.0,
    "P": 1024**2,
    "PB": 1024**2,
    "PiB": 1024**2,
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
