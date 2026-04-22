import os
import socket
import sys
from typing import TypeIs

import rich.console

# todo: seeing some weird behaviour with stderr, the progress bars repeating themselves, etc.
console = rich.console.Console(record=True, file=sys.stdout)


def current_cluster() -> str | None:
    if socket.gethostname().endswith(".server.mila.quebec"):
        return "mila"
    if "CC_CLUSTER" in os.environ:
        return os.environ["CC_CLUSTER"]
    return None


def is_list_of[T](some_list: list, item_type: type[T]) -> TypeIs[list[T]]:
    return isinstance(some_list, list) and all(isinstance(x, item_type) for x in some_list)
