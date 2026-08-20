"""Centralized management of the single shared `rich.Live` display.

`submit_first()` (see `cluv/cli/submit.py`) watches SLURM job state across clusters in a live
table. When only one `submit_first()` call is running, it owns the terminal's live region
exactly as a plain `rich.live.Live` would. When several run concurrently (e.g. a Hydra sweep
submitting many jobs at once), `rich.Live` can only have one live region per console, so this
module fuses every concurrent call's progress into a single combined table instead.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import AsyncIterator

import rich.console
import rich.table
import rich.text
from rich.console import RenderableType
from rich.live import Live

from cluv.utils import console


@dataclasses.dataclass
class JobWaitProgress:
    """Snapshot of one concurrent `submit_first()` call's wait/cancel state.

    `label` (e.g. the job's `program_args`, joined) is only shown when more than one call is
    active concurrently, to disambiguate rows belonging to different jobs.
    """

    label: str
    cancelling: bool
    rows: list[tuple[str, int, str]]  # (cluster, job_id, job_state)


def _state_style(job_state: str) -> str:
    if job_state.startswith(("RUNNING", "COMPLETED", "CANCELLED")):
        return "green"
    if job_state.startswith(("PENDING", "UNKNOWN")):
        return "yellow"
    return "red"


def _title_for(cancelling: bool) -> str:
    return "Waiting for jobs to cancel..." if cancelling else "Waiting for a job to start..."


def _build_single_table(progress: JobWaitProgress) -> rich.table.Table:
    """Same shape as the table `submit_first()` rendered before centralization."""
    table = rich.table.Table("Cluster", "Job ID", "Status", title=_title_for(progress.cancelling))
    for cluster, job_id, job_state in progress.rows:
        table.add_row(
            cluster, str(job_id), rich.text.Text(job_state, style=_state_style(job_state))
        )
    return table


def _build_combined_table(entries: list[JobWaitProgress]) -> rich.table.Table:
    labels = shorten_labels([entry.label for entry in entries])
    title = _title_for(any(entry.cancelling for entry in entries))
    table = rich.table.Table("Job", "Cluster", "Job ID", "Status", title=title)
    for label, entry in zip(labels, entries):
        for cluster, job_id, job_state in entry.rows:
            table.add_row(
                label,
                cluster,
                str(job_id),
                rich.text.Text(job_state, style=_state_style(job_state)),
            )
    return table


def shorten_labels(labels: list[str], max_len: int = 40) -> list[str]:
    """Shortens labels that share a long common leading word sequence.

    Labels at or under `max_len` characters are left untouched. Longer labels are rendered as
    ``"<common prefix words> (...) <differing suffix>"``, e.g. ``"python main.py lr=0.1"`` and
    ``"python main.py lr=0.2"`` become ``"python main.py (...) lr=0.1"`` and
    ``"python main.py (...) lr=0.2"``. The differing suffix is what makes labels distinguishable,
    so if the shortened form still doesn't fit, the shared prefix is trimmed (or dropped
    entirely) before the suffix ever is — otherwise labels that only differ in their tail would
    collide into the same truncated string.

    >>> shorten_labels(["python main.py lr=0.1", "python main.py lr=0.2"], max_len=20)
    ['python (...) lr=0.1', 'python (...) lr=0.2']
    >>> shorten_labels(["short", "labels"], max_len=40)
    ['short', 'labels']
    """
    if all(len(label) <= max_len for label in labels):
        return list(labels)

    words_per_label = [label.split() for label in labels]
    common_word_count = 0
    if len(labels) >= 2:
        for words in zip(*words_per_label):
            if len(set(words)) != 1:
                break
            common_word_count += 1

    if common_word_count == 0:
        return [_truncate(label, max_len) for label in labels]

    prefix = " ".join(words_per_label[0][:common_word_count])
    marker = " (...) "
    shortened: list[str] = []
    for words in words_per_label:
        suffix = " ".join(words[common_word_count:])
        if not suffix:
            shortened.append(_truncate(prefix, max_len))
            continue
        budget_for_prefix = max_len - len(marker) - len(suffix)
        if budget_for_prefix <= 0:
            # No room left for (any of) the shared prefix; keep the differentiating suffix,
            # since that's the part that actually distinguishes this label from the others.
            shortened.append(_truncate(suffix, max_len))
        else:
            shortened.append(f"{prefix[:budget_for_prefix].rstrip()}{marker}{suffix}")
    return shortened


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


class Handle:
    """Returned by `LiveRegistry.section()`; lets the caller update its own progress."""

    def __init__(self, registry: LiveRegistry, key: object) -> None:
        self._registry = registry
        self._key = key

    def update(self, progress: JobWaitProgress) -> None:
        self._registry._entries[self._key] = progress

    def refresh(self) -> None:
        if self._registry._live is not None:
            self._registry._live.refresh()


class LiveRegistry:
    """Owns the single shared `rich.Live` for the process.

    Any number of concurrent callers can register a `JobWaitProgress` via `section()`; their
    views are fused into one combined table. With exactly one active registrant, the rendered
    output is identical to what a plain, non-shared `Live` would show for that one table.
    """

    def __init__(self, console: rich.console.Console) -> None:
        self._console = console
        self._live: Live | None = None
        self._entries: dict[object, JobWaitProgress] = {}

    def _render(self) -> RenderableType:
        entries = list(self._entries.values())
        if not entries:
            return rich.text.Text("")
        if len(entries) == 1:
            return _build_single_table(entries[0])
        return _build_combined_table(entries)

    @contextlib.asynccontextmanager
    async def section(self, initial: JobWaitProgress) -> AsyncIterator[Handle]:
        """Register a new concurrent live section.

        Starts the shared `Live` if this is the first active registrant; stops it once the last
        one exits. Always unregisters on exit, including on cancellation or exceptions.
        """
        key = object()
        self._entries[key] = initial
        if self._live is None:
            self._live = Live(
                get_renderable=self._render, console=self._console, refresh_per_second=1
            )
            self._live.start(refresh=True)
        else:
            self._live.refresh()
        try:
            yield Handle(self, key)
        finally:
            del self._entries[key]
            if not self._entries:
                live, self._live = self._live, None
                live.stop()
            else:
                self._live.refresh()


registry = LiveRegistry(console)
