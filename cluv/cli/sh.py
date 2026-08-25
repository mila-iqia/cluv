"""`cluv sh`: run a raw shell command across every currently-connected cluster via clush."""

from __future__ import annotations

import asyncio
import logging
import shlex
import sys
import tempfile
from collections import defaultdict
from pathlib import Path, PurePosixPath

from cluv.cache import get_disabled_clusters
from cluv.cli.disable import print_disabled_clusters
from cluv.cli.sync import get_active_remotes
from cluv.config import find_pyproject, get_cluv_config
from cluv.utils import console, current_cluster

__all__ = ["sh"]

logger = logging.getLogger(__name__)


async def sh(command: list[str]) -> None:
    """Runs `command` via `clush` on every cluster we currently have an active SSH connection to.

    Runs from each cluster's project directory (see `cluv.project_dir` in the Cluv config) when
    that directory already exists there, and from the user's home directory otherwise (e.g. if the
    project hasn't been synced to that cluster yet).

    If invoked from a cluster (i.e. `current_cluster()` is set), also runs `command` locally
    first, from the local project directory, since `get_active_remotes()` never includes the
    current cluster.

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
        # Group clusters by project dir, since it can be overridden per-cluster: each group gets
        # its own `clush` invocation so it can `cd` there before running `command`. In the common
        # case (no per-cluster override) there's a single group, i.e. a single `clush` call.
        hostnames_by_project_dir: dict[PurePosixPath | None, list[str]] = defaultdict(list)
        config = get_cluv_config()

        for remote in remotes:
            project_dir_for_cluster = config.get_cluster_config(remote.hostname).project_dir
            hostnames_by_project_dir[project_dir_for_cluster].append(remote.hostname)
        for project_dir, hostnames in hostnames_by_project_dir.items():
            with tempfile.NamedTemporaryFile("w", suffix=".txt") as hostfile:
                hostfile.write("\n".join(hostnames) + "\n")
                hostfile.flush()
                clush_returncode = await _invoke_clush(Path(hostfile.name), command, project_dir)
            returncode = returncode or clush_returncode

    if returncode != 0:
        sys.exit(returncode)


async def _invoke_clush(
    hostfile: Path, command: list[str], project_dir: PurePosixPath | None
) -> int:
    """Runs `clush` (via `uvx --from=clustershell`) against `hostfile`, running `command` on each host.

    If `project_dir` is given, `cd`s there first (silently falling back to the default remote
    directory, usually $HOME, if it doesn't exist there yet).

    Inherits stdout/stderr from the current process (instead of capturing them) so that `clush`'s
    own TTY detection and per-node colored output pass straight through to the user's terminal.

    Passes `-S`/`--maxrc` so that `clush` itself exits with the largest of the per-host command
    return codes; without it, `clush` always exits 0 regardless of remote command failures.
    """
    remote_command = _with_cd_prefix(command, project_dir)
    argv = [
        "uvx",
        "--from=clustershell",
        "clush",
        "-S",
        "--hostfile",
        str(hostfile),
        remote_command,
    ]
    console.log(f"$ {shlex.join(argv)}", style="green")
    proc = await asyncio.create_subprocess_exec(*argv)
    return await proc.wait()


async def _run_locally(command: list[str]) -> int:
    """Runs `command` directly as a local subprocess (no SSH), inheriting stdout/stderr.

    Runs from the local project root (i.e. the directory containing `pyproject.toml`).
    """
    project_dir = find_pyproject().parent
    console.log(f"({current_cluster()}) $ {shlex.join(command)}", style="green")
    proc = await asyncio.create_subprocess_exec(*command, cwd=project_dir)
    return await proc.wait()


def _with_cd_prefix(command: list[str], project_dir: PurePosixPath | None) -> str:
    """Builds the shell command string clush should run: `command`, `cd`-ing into `project_dir` first.

    Double-quoted (rather than `shlex.quote`d) so that `$HOME`/other env vars in `project_dir` are
    still expanded by the remote shell. Failure to `cd` (e.g. the directory doesn't exist yet on
    that cluster) is ignored, so `command` still runs from the default remote directory.
    """
    if project_dir is None:
        return shlex.join(command)
    return f'cd "{project_dir}" 2>/dev/null; {shlex.join(command)}'
