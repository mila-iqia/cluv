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


def resolve_env_vars(string_or_path: str | Path):
    path = Path(string_or_path)
    parts = path.parts
    new_parts = [
        os.environ.get(part.removeprefix("$")) if part.startswith("$") else part for part in parts
    ]
    return os.path.join(*new_parts)
