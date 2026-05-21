"""Local memory-usage history cache, backed by sacct.

Layout: ``~/.cache/cluv/history/<cluster>/<spec_key>.json`` holds a JSON list of
``JobRecord``s for one (cluster, key) pair. The estimator in
``salvo.history.estimate_mem`` consumes that list.

This module owns I/O. The pure estimator math lives in salvo.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from salvo.history import JobRecord

from cluv.remote import Remote

COMMENT_PREFIX = "cluv:v1:"

# SLURM ReqMem strings: "2G", "4096M", "512K", and on older versions a trailing
# scope suffix like "2Gn" (per-node) or "2Gc" (per-cpu). The leading number +
# unit is the part we care about.
_REQMEM_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMG])", re.IGNORECASE)


def cache_dir() -> Path:
    return Path(os.environ.get("CLUV_HISTORY_DIR") or Path.home() / ".cache" / "cluv" / "history")


def _key_path(cluster: str, key: str) -> Path:
    return cache_dir() / cluster / f"{key}.json"


def build_comment(key: str) -> str:
    """Build the ``--comment`` payload stamped on every cluv submission."""
    return f"{COMMENT_PREFIX}{key}"


def parse_comment(comment: str) -> str | None:
    """Extract the spec key from a sacct ``Comment`` field, or return ``None``."""
    if not comment or not comment.startswith(COMMENT_PREFIX):
        return None
    return comment[len(COMMENT_PREFIX) :].strip() or None


def load(cluster: str, key: str) -> list[JobRecord]:
    """Return records for ``(cluster, key)`` sorted by submit time, newest first."""
    path = _key_path(cluster, key)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    records: list[JobRecord] = []
    for entry in raw:
        try:
            records.append(JobRecord.model_validate(entry))
        except Exception:
            # Skip entries from incompatible older versions rather than fail submit.
            continue
    records.sort(key=lambda r: r.submitted_at, reverse=True)
    return records


def save_record(record: JobRecord) -> None:
    """Append ``record`` to the cache for its (cluster, key) pair.

    De-duplicates by ``job_id`` so retried writes are idempotent. Atomic via
    write-to-tmp + rename.
    """
    path = _key_path(record.cluster, record.key)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load(record.cluster, record.key)
    keep = [r for r in existing if r.job_id != record.job_id]
    keep.append(record)
    keep.sort(key=lambda r: r.submitted_at, reverse=True)
    _write_atomic(path, [r.model_dump(mode="json") for r in keep])


def _write_atomic(path: Path, payload: list[dict]) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=".cluv-", suffix=".tmp", delete=False
    ) as fh:
        json.dump(payload, fh, indent=2, default=str)
        fh.flush()
        os.fsync(fh.fileno())
        tmp_path = Path(fh.name)
    tmp_path.replace(path)


def parse_mem_to_mb(raw: str) -> int | None:
    """Parse a sacct memory string (e.g. ``"2G"``, ``"4096M"``, ``"904K"``).

    Returns ``None`` on empty or unparsable input. Strips trailing SLURM scope
    suffixes (``"n"`` for per-node, ``"c"`` for per-cpu) by matching only the
    leading number + unit.
    """
    raw = (raw or "").strip()
    if not raw or raw == "0":
        return None
    m = _REQMEM_RE.match(raw)
    if not m:
        try:
            value = float(raw.rstrip("Mm"))
            return max(1, int(round(value)))
        except ValueError:
            return None
    value, unit = float(m.group(1)), m.group(2).upper()
    multiplier = {"K": 1 / 1024, "M": 1, "G": 1024}[unit]
    return max(1, int(round(value * multiplier)))


def _parse_submit(raw: str) -> datetime | None:
    """sacct ``Submit`` is naive ISO. Tag as UTC for stable ordering."""
    raw = (raw or "").strip()
    if not raw or raw.lower() == "unknown":
        return None
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=UTC)
    except ValueError:
        return None


def _records_from_sacct(output: str, cluster: str) -> list[JobRecord]:
    """Group sacct rows by allocation job id, build one ``JobRecord`` each.

    sacct emits the allocation row (``"12345"``) and per-step rows
    (``"12345.batch"``, ``"12345.extern"``). MaxRSS lives on the step rows,
    State/ReqMem/Comment/Submit on the allocation row. This walks both and
    merges by the integer leading prefix.

    Only rows whose ``Comment`` matches ``cluv:v1:<key>`` are kept.
    """
    by_id: dict[str, dict] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 7:
            continue
        job_id_str, state, max_rss, req_mem, elapsed, comment, submit = parts[:7]
        base = job_id_str.split(".", 1)[0]
        slot = by_id.setdefault(base, {"max_rss_mb": None})
        if "." not in job_id_str:
            slot["job_id"] = base
            slot["state"] = state.split()[0] if state else ""
            slot["req_mem_mb"] = parse_mem_to_mb(req_mem)
            slot["comment"] = comment
            slot["submitted_at"] = _parse_submit(submit)
            try:
                slot["elapsed_s"] = int(elapsed) if elapsed.strip() else None
            except ValueError:
                slot["elapsed_s"] = None
        rss = parse_mem_to_mb(max_rss)
        if rss is not None:
            existing = slot.get("max_rss_mb")
            slot["max_rss_mb"] = max(existing or 0, rss)

    records: list[JobRecord] = []
    for slot in by_id.values():
        if "job_id" not in slot:
            continue
        key = parse_comment(slot.get("comment", ""))
        if key is None:
            continue
        if slot.get("submitted_at") is None:
            continue
        if slot.get("req_mem_mb") is None:
            continue
        records.append(
            JobRecord(
                job_id=slot["job_id"],
                key=key,
                cluster=cluster,
                state=slot["state"] or "UNKNOWN",
                mem_mb=slot["req_mem_mb"],
                max_rss_mb=slot.get("max_rss_mb"),
                elapsed_s=slot.get("elapsed_s"),
                submitted_at=slot["submitted_at"],
            )
        )
    return records


async def backfill_from_sacct(
    remote: Remote, cluster: str, *, since_days: int = 60
) -> int:
    """Pull recent cluv-stamped jobs from sacct on ``cluster``, write to cache.

    Returns the number of records persisted (includes pre-existing entries that
    get refreshed). One sacct call regardless of how many keys appear.
    """
    cmd = (
        f"sacct --starttime now-{since_days}days "
        "--format=JobID,State,MaxRSS,ReqMem,ElapsedRaw,Comment,Submit "
        "--parsable2 --noheader --units=M"
    )
    output = await remote.get_output(cmd)
    records = _records_from_sacct(output, cluster=cluster)
    for record in records:
        save_record(record)
    return len(records)


def list_keys(cluster: str | None = None) -> list[tuple[str, str, int]]:
    """Return ``(cluster, key, record_count)`` triples for inspection."""
    root = cache_dir()
    if not root.exists():
        return []
    out: list[tuple[str, str, int]] = []
    clusters = [cluster] if cluster else [d.name for d in root.iterdir() if d.is_dir()]
    for c in clusters:
        cluster_dir = root / c
        if not cluster_dir.exists():
            continue
        for path in cluster_dir.glob("*.json"):
            key = path.stem
            try:
                count = len(json.loads(path.read_text()))
            except (json.JSONDecodeError, OSError):
                count = 0
            out.append((c, key, count))
    return sorted(out)


def clear(cluster: str | None = None, key: str | None = None) -> int:
    """Remove cache files. Returns the number of files deleted."""
    root = cache_dir()
    if not root.exists():
        return 0
    if cluster and key:
        path = _key_path(cluster, key)
        if path.exists():
            path.unlink()
            return 1
        return 0
    paths: list[Path] = []
    if cluster:
        cluster_dir = root / cluster
        if cluster_dir.exists():
            paths.extend(cluster_dir.glob("*.json"))
    else:
        for cluster_dir in root.iterdir():
            if cluster_dir.is_dir():
                paths.extend(cluster_dir.glob("*.json"))
    for p in paths:
        p.unlink()
    return len(paths)


__all__ = [
    "COMMENT_PREFIX",
    "backfill_from_sacct",
    "build_comment",
    "cache_dir",
    "clear",
    "list_keys",
    "load",
    "parse_comment",
    "parse_mem_to_mb",
    "save_record",
]


# Convenience for sync callers (CLI subcommands) that don't want to await.
def backfill_sync(remote: Remote, cluster: str, *, since_days: int = 60) -> int:
    return asyncio.run(backfill_from_sacct(remote, cluster, since_days=since_days))
