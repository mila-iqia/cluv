import os
import socket
import sys

import rich.console

# todo: seeing some weird behaviour with stderr, the progress bars repeating themselves, etc.
console = rich.console.Console(record=True, file=sys.stdout)


def current_cluster() -> str | None:
    if socket.gethostname().endswith(".server.mila.quebec"):
        return "mila"
    if "CC_CLUSTER" in os.environ:
        return os.environ["CC_CLUSTER"]
    return None
