import os
import sys
from pathlib import Path

import rich.console

console = rich.console.Console(record=True, file=sys.stdout)


def current_cluster() -> str | None:
    if Path("/home/mila").exists():
        return "mila"
    if "CC_CLUSTER" in os.environ:
        return os.environ["CC_CLUSTER"]
    return None
