from __future__ import annotations

import logging
import shlex
from pathlib import Path
from subprocess import CompletedProcess

from cluv.cli.sync import sync
from cluv.config import find_pyproject

logger = logging.getLogger(__name__)
__all__ = ["run"]


async def run(command: str | list[str], cluster: str) -> CompletedProcess[str]:
    """Runs a command in the synced project on a potentially remote cluster.

    Similar in spirit to `uv run`, but runs a command in the synced project on a potentially remote cluster.
    - Idea is that this could maybe be a building block for other commands.

    Parameters:
        command: The command to run, as a string or list of strings.
        cluster: The cluster to run the command on.
    Returns:
        the `subprocess.CompletedProcess` object returned by the command.
    """
    if not isinstance(command, str):
        command = shlex.join(command)
    logger.debug(f"About to run {command=} on {cluster=}")
    remote = (await sync(clusters=[cluster]))[0]
    project_path = find_pyproject().parent.relative_to(Path.home())
    return await remote.run(f"bash -l -c 'uv run --directory={project_path} {command}'")
