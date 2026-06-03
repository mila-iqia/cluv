import os
import socket
import sys
from pathlib import Path

import rich.console

# todo: seeing some weird behaviour with stderr, the progress bars repeating themselves, etc.
console = rich.console.Console(record=True, file=sys.stdout)


def current_cluster() -> str | None:
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
