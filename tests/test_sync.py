"""Tests for `cluv sync`"""

import importlib
import subprocess
import unittest
import unittest.mock
from pathlib import Path

import pytest

from cluv.cli.sync import expandvars, fetch_results, sync
from cluv.config import LocalConfig, get_cluv_config
from cluv.job import get_datasets_path
from cluv.remote import Remote
from cluv.utils import current_cluster

from .test_integration import IN_GITHUB_CLOUD_CI

pytestmark = [
    pytest.mark.skipif(
        IN_GITHUB_CLOUD_CI,
        reason="Integration tests are only run on a self-hosted github runner or on a dev machine.",
    ),
    pytest.mark.integration,
]


# `cluv/cli/__init__.py` does `from .sync import sync`, which shadows the `cluv.cli.sync`
# submodule attribute with the `sync` function. Use `importlib.import_module` (same idiom as
# `tests/test_init.py`) to get the actual module object for monkeypatching.
sync_module = importlib.import_module("cluv.cli.sync")


@pytest.mark.slow
@pytest.mark.asyncio
async def test_cluv_sync_with_data_path(monkeypatch: pytest.MonkeyPatch, fake_scratch: Path):
    """Test for `cluv sync` with a project that has a 'datasets_path'.

    Need to check that rsync happens from `data_source` (the source) to the `datasets_path` here and
    on all the clusters where we have a connection.
    """
    assert not current_cluster(), "test needs to run locally for now."
    other_cluster = "tamia"
    other_cluster_remote = await Remote.connect(other_cluster)

    monkeypatch.chdir("examples/pytorch-example")
    # We need to change the "tool.cluv.local.env.SCRATCH" to point to the fake scratch path.
    config = get_cluv_config()
    monkeypatch.setattr(config, "local", LocalConfig(env={"SCRATCH": str(fake_scratch)}))
    monkeypatch.setenv("SCRATCH", str(fake_scratch))
    monkeypatch.setattr(sync_module, get_cluv_config.__name__, lambda: config)

    # Avoid re-fetching results to this fake_scratch directory.
    monkeypatch.setattr(
        sync_module, fetch_results.__name__, unittest.mock.AsyncMock(return_value=[])
    )

    assert config.datasets_path

    here_datasets_path = get_datasets_path()
    assert here_datasets_path and here_datasets_path.is_relative_to(fake_scratch)
    assert not here_datasets_path.exists(), "Datasets path should not exist before sync."

    other_cluster_datasets_path = config.get_cluster_config(other_cluster).datasets_path
    assert other_cluster_datasets_path

    other_cluster_datasets_path = await expandvars(
        other_cluster_remote, other_cluster_datasets_path
    )
    other_cluster_files = (
        (
            await other_cluster_remote.get_output(
                f"ls {other_cluster_datasets_path}", warn=True, hide=True
            )
        )
        .strip()
        .splitlines()
    )

    # TODO: For some reason, the dataset files are *not* found on the remote after `sync` during
    # the integration tests on the Self-hosted runner. I'm not
    # sure why this happens, since the paths definitely do exist, and running sync manually works.
    # When this is fixed/figured out, we should uncomment the following, which deletes the dataset
    # from the remote before syncing, to make sure the test is not dependent on the initial state of
    # the remote.
    # Clean up any existing dataset on the other cluster
    # if other_cluster_files:
    #     await other_cluster_remote.run(f"rm -r {other_cluster_datasets_path}")
    #     other_cluster_files = []

    # Dataset shouldn't be present on either the local machine or the remote.
    # TODO: This is a bit inefficient, but for such a small dataset, it's not a big deal.
    this_cluster_files = subprocess.getoutput(f"ls {here_datasets_path}").strip().splitlines()
    assert this_cluster_files != other_cluster_files

    await sync([other_cluster])

    # Check that we now have the files for that dataset in the datasets_path on this machine.
    from torchvision.datasets import CIFAR10

    print(CIFAR10(here_datasets_path))

    # Dataset is synced on the remote as well.
    this_cluster_files = list(
        sorted(subprocess.getoutput(f"ls {here_datasets_path}").strip().splitlines())
    )
    other_cluster_files = list(
        sorted(
            (
                (await other_cluster_remote.get_output(f"ls {other_cluster_datasets_path}"))
                .strip()
                .splitlines()
            )
        )
    )
    assert this_cluster_files == other_cluster_files
