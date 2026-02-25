import os
import sys
from pathlib import Path

import milatools.cli
import milatools.utils.local_v2
import milatools.utils.remote_v2
import rich.console

console = rich.console.Console(record=True, file=sys.stdout)
# FIXME: Makes it so milatools uses the same rich console.
milatools.cli.console = console
milatools.utils.remote_v2.console = console
milatools.utils.local_v2.console = console
# err_console = rich.console.Console(record=True, file=sys.stderr)


def current_cluster() -> str | None:
    if Path("/home/mila").exists():
        return "mila"
    if "CC_CLUSTER" in os.environ:
        return os.environ["CC_CLUSTER"]
    return None
