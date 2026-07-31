"""`cluv sh`: run a raw shell command across every currently-connected cluster via clush."""

from __future__ import annotations

import asyncio
import logging
import shlex
import sys
import tempfile
from pathlib import Path

from cluv.cache import get_disabled_clusters
from cluv.cli.disable import print_disabled_clusters
from cluv.cli.sync import get_active_remotes
from cluv.utils import console, current_cluster

__all__ = ["sh"]

logger = logging.getLogger(__name__)


async def sh(command: list[str]) -> None:
    """Runs `command` via `clush` on every cluster we currently have an active SSH connection to.

    If invoked from a cluster (i.e. `current_cluster()` is set), also runs `command` locally
    first, since `get_active_remotes()` never includes the current cluster.

    Does not sync the project or wrap the command in `uv run` (see `cluv run` for that). Does not
    attempt to connect to clusters we aren't already connected to, so this never triggers a 2FA
    prompt. If either run exits with a non-zero return code, this exits the process with the first
    non-zero code encountered (the local run's code takes precedence if both fail).

    Parameters:
        command: The command (and its arguments) to run.
    """
    disabled = get_disabled_clusters()
    print_disabled_clusters(disabled)

    remotes = await get_active_remotes()
    here = current_cluster()

    if not remotes and not here:
        console.print(
            "[yellow]Not currently connected to any cluster.[/yellow] "
            "Use `cluv login` to connect first."
        )
        return

    returncode = 0

    if here:
        returncode = await _run_locally(command)

    if remotes:
        hostnames = [remote.hostname for remote in remotes]
        with tempfile.NamedTemporaryFile("w", suffix=".txt") as hostfile:
            hostfile.write("\n".join(hostnames) + "\n")
            hostfile.flush()
            clush_returncode = await _invoke_clush(Path(hostfile.name), command)
        returncode = returncode or clush_returncode

    if returncode != 0:
        sys.exit(returncode)


async def _invoke_clush(hostfile: Path, command: list[str]) -> int:
    """Runs `clush` (via `uvx --from=clustershell`) against `hostfile`, running `command` on each host.

    Inherits stdout/stderr from the current process (instead of capturing them) so that `clush`'s
    own TTY detection and per-node colored output pass straight through to the user's terminal.
    """
    argv = ["uvx", "--from=clustershell", "clush", "--hostfile", str(hostfile), *command]
    console.log(f"$ {shlex.join(argv)}", style="green")
    proc = await asyncio.create_subprocess_exec(*argv)
    return await proc.wait()


async def _run_locally(command: list[str]) -> int:
    """Runs `command` directly as a local subprocess (no SSH), inheriting stdout/stderr."""
    console.log(f"({current_cluster()}) $ {shlex.join(command)}", style="green")
    proc = await asyncio.create_subprocess_exec(*command)
    return await proc.wait()
