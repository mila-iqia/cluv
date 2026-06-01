"""Tests for `cluv sync`"""

import subprocess

import pytest

from cluv.cli.sync import sync
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


@pytest.mark.asyncio
async def test_cluv_sync_with_data_path(monkeypatch: pytest.MonkeyPatch):
    """TODO: Test for `cluv sync` with a project that has a 'data_path'.


    Need to check that rsync happens from `datasets_path` (the source) to the `datasets_path` (the dest) on all the clusters.
    """
    # assert current_cluster() == "mila"
    assert current_cluster() is None
    other_cluster = "tamia"
    other_cluster_remote = await Remote.connect(other_cluster)

    other_cluster_files = await other_cluster_remote.get_output(
        "ls $SCRATCH/datasets/cifar10", warn=True, hide=True
    )
    if other_cluster_files:
        # Clean up any existing dataset on the other cluster
        await other_cluster_remote.run("rm -r $SCRATCH/datasets/cifar10")
        other_cluster_files = await other_cluster_remote.get_output("ls $SCRATCH/datasets/cifar10")
        assert not other_cluster_files, "Expected no dataset on the other cluster before syncing."

    # Dataset isn't synced
    this_cluster_files = subprocess.getoutput("ls $SCRATCH/datasets/cifar10")
    assert this_cluster_files != other_cluster_files

    await sync([other_cluster], uv_sync_args=None)

    # Dataset is synced
    this_cluster_files = subprocess.getoutput("ls $SCRATCH/datasets/cifar10").strip().splitlines()
    other_cluster_files = (
        (await other_cluster_remote.get_output("ls $SCRATCH/datasets/cifar10"))
        .strip()
        .splitlines()
    )
    assert this_cluster_files == other_cluster_files
