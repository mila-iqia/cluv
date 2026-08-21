"""Regression test for `run_uv_sync` warming a cluster-configured UV_CACHE_DIR.

On a cluster where `$HOME` isn't reachable from compute nodes (trillium-gpu), a job has to point
`UV_CACHE_DIR` somewhere else reachable from both the login node and compute nodes (e.g.
`$SCRATCH`) - but that means `cluv sync`'s own `uv sync`, which runs on the login node and is the
only chance to populate the cache before a job needs it (compute nodes there have no internet
either), has to warm that exact same directory, not uv's default `$HOME/.cache/uv`.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from unittest import mock

from cluv.cache import ProjectStateOnCluster
from cluv.cli.sync import run_uv_sync
from cluv.remote import Remote


async def test_uv_cache_dir_is_forwarded_unquoted_to_the_warm_up_sync() -> None:
    remote = mock.AsyncMock(spec=Remote)
    remote.hostname = "trillium-gpu"

    await run_uv_sync(
        remote,
        PurePosixPath("/scratch/me/repos/cluv/examples/imagenet"),
        ProjectStateOnCluster(),
        uv_cache_dir="$SCRATCH/.cache/uv",
    )

    (command,), _kwargs = remote.run.call_args
    assert command == (
        "bash --login -c 'UV_CACHE_DIR=$SCRATCH/.cache/uv "
        "uv --directory=/scratch/me/repos/cluv/examples/imagenet sync --quiet --reinstall'"
    )
    # The whole point: unlike the job-time env vars in `get_sbatch_command`, this isn't
    # `shlex.quote`d - it has to stay a bare `$SCRATCH` so the single login shell that runs this
    # entire command expands it, rather than being passed through as a literal, useless string.
    assert "'$SCRATCH" not in command


async def test_uv_cache_dir_forces_reinstall_so_the_cache_actually_gets_populated() -> None:
    """A login-node venv that already satisfies the lockfile needs nothing new, so a plain

    `uv sync` wouldn't touch UV_CACHE_DIR at all - leaving it empty for the job that needs it.
    --reinstall forces every package through cache/download regardless.
    """
    remote = mock.AsyncMock(spec=Remote)
    remote.hostname = "trillium-gpu"

    await run_uv_sync(
        remote,
        PurePosixPath("/scratch/me/repos/cluv/examples/imagenet"),
        ProjectStateOnCluster(),
        uv_cache_dir="$SCRATCH/.cache/uv",
    )

    (command,), _kwargs = remote.run.call_args
    assert "--reinstall" in command


async def test_no_uv_cache_dir_prefix_when_not_configured() -> None:
    remote = mock.AsyncMock(spec=Remote)
    remote.hostname = "mila"

    await run_uv_sync(
        remote,
        PurePosixPath("/home/me/project"),
        ProjectStateOnCluster(),
    )

    (command,), _kwargs = remote.run.call_args
    assert command == "bash --login -c 'uv --directory=/home/me/project sync --quiet'"
