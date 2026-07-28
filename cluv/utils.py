import asyncio
import contextlib
import contextvars
import os
import socket
import sys
from contextvars import ContextVar
from pathlib import Path

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


@contextlib.contextmanager
def set_context[T](var: ContextVar[T], value: T):
    """Equivalent of contextlib.ContextVar.set() context manager for Python < 3.14."""
    token = var.set(value)
    try:
        yield
    finally:
        var.reset(token)
