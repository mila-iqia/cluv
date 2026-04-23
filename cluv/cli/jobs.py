from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from rich.table import Table

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import find_pyproject
from cluv.utils import console

_STATE_STYLE: dict[str, str] = {
    "RUNNING": "green",
    "COMPLETED": "blue",
    "FAILED": "red bold",
    "PENDING": "yellow",
    "CANCELLED": "dim",
    "TIMEOUT": "red",
}


def _jobs_file() -> Path:
    return find_pyproject().parent / ".cluv" / "jobs.jsonl"


def append_record(record: dict) -> None:
    path = _jobs_file()
    path.parent.mkdir(exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def _load_records() -> list[dict]:
    path = _jobs_file()
    if not path.exists():
        return []
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


async def _query_sacct(cluster: str, job_ids: list[int]) -> dict[int, tuple[str, str]]:
    """Return {job_id: (state, elapsed)} for the given cluster, or {} if not connected."""
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        return {}
    ids_str = ",".join(str(j) for j in job_ids)
    output = await remote.get_output(
        f"sacct -j {ids_str} --format=JobID,State,Elapsed --noheader --parsable2"
    )
    result: dict[int, tuple[str, str]] = {}
    for line in output.splitlines():
        parts = line.split("|")
        if len(parts) < 3 or "." in parts[0]:  # skip .batch/.extern sub-jobs
            continue
        try:
            jid = int(parts[0])
            # "CANCELLED by 12345" → "CANCELLED"
            state = parts[1].split()[0]
            result[jid] = (state, parts[2])
        except (ValueError, IndexError):
            pass
    return result


async def jobs(cluster: str | None = None, limit: int = 20) -> None:
    """List submitted jobs for this project."""
    records = _load_records()
    if cluster:
        records = [r for r in records if r["cluster"] == cluster]
    records = list(reversed(records))[:limit]

    if not records:
        console.print("No jobs found.")
        return

    by_cluster: dict[str, list[int]] = defaultdict(list)
    for r in records:
        by_cluster[r["cluster"]].append(r["job_id"])

    results = await asyncio.gather(
        *(_query_sacct(c, ids) for c, ids in by_cluster.items())
    )
    status_map: dict[int, tuple[str, str]] = {}
    for partial in results:
        status_map.update(partial)

    table = Table(show_header=True, header_style="bold")
    table.add_column("ID", style="dim")
    table.add_column("Cluster")
    table.add_column("Status")
    table.add_column("Elapsed")
    table.add_column("Commit", style="dim")
    table.add_column("Script")
    table.add_column("Submitted")

    for r in records:
        jid = r["job_id"]
        state, elapsed = status_map.get(jid, ("?", "?"))
        style = _STATE_STYLE.get(state, "")
        styled_state = f"[{style}]{state}[/{style}]" if style else state

        submitted = r.get("submitted_at", "?")
        try:
            submitted = datetime.fromisoformat(submitted).strftime("%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            pass

        table.add_row(
            str(jid),
            r.get("cluster", "?"),
            styled_state,
            elapsed,
            r.get("git_commit", "?")[:7],
            r.get("job_script", "?"),
            submitted,
        )

    console.print(table)
