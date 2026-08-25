import asyncio
import contextlib
import contextvars
import os
import socket
import sys
from collections.abc import Iterator, Sequence
from contextvars import ContextVar
from pathlib import Path
from typing import Protocol, TypeVar

import rich.console

# todo: seeing some weird behaviour with stderr, the progress bars repeating themselves, etc.
console = rich.console.Console(record=True, file=sys.stdout)

console_lock: contextvars.ContextVar[asyncio.Lock | None] = contextvars.ContextVar(
    "console_lock", default=None
)


def current_cluster() -> str | None:
    """Returns the name of the current cluster (Mila,DRAC), or `None` if not on a cluster (or on an unknown cluster)."""
    if socket.gethostname().endswith(".server.mila.quebec"):
        return "mila"
    if "CC_CLUSTER" in os.environ:
        return os.environ["CC_CLUSTER"]
    return None


def find_pyproject(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for folder in (current, *current.parents):
        candidate = folder / "pyproject.toml"
        if candidate.is_file():
            return candidate
    raise RuntimeError(
        f"Could not find pyproject.toml starting from {current}!\n"
        f"Cluv can only be used within a project managed with uv."
    )


T = TypeVar("T")


@contextlib.contextmanager
def set_context(var: ContextVar[T], value: T):
    """Equivalent of contextlib.ContextVar.set() context manager for Python < 3.14."""
    token = var.set(value)
    try:
        yield
    finally:
        var.reset(token)


def batched(iterable: Sequence[T], n: int) -> Iterator[tuple[T, ...]]:
    """Backport of `itertools.batched` (added in Python 3.12) for our Python 3.11 baseline."""
    if n < 1:
        raise ValueError("n must be at least one")
    for i in range(0, len(iterable), n):
        yield tuple(iterable[i : i + n])


class HasCluster(Protocol):
    cluster: str


JobLike = TypeVar("JobLike", bound=HasCluster)


def group_by_cluster(objects_with_cluster_field: list[JobLike]) -> dict[str, list[JobLike]]:
    grouped: dict[str, list[JobLike]] = {}
    for job_submission in objects_with_cluster_field:
        grouped.setdefault(job_submission.cluster, []).append(job_submission)
    return grouped
