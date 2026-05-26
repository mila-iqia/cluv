"""Tests for `cluv sync`"""

import subprocess

import pytest

from cluv.cli.sync import sync
from cluv.remote import Remote
from cluv.utils import current_cluster


@pytest.mark.asyncio
async def test_cluv_sync_with_data_path():
    """TODO: Test for `cluv sync` with a project that has a 'data_path'.


    Need to check that rsync happens from `datasets_path` (the source) to the `datasets_path` (the dest) on all the clusters.
    """
    assert current_cluster() == "mila"
    other_cluster = "tamia"
    other_cluster_remote = await Remote.connect(other_cluster)

    # Dataset isn't synced
    this_cluster_files = subprocess.getoutput("ls $SCRATCH/data/cifar10")
    other_cluster_files = await other_cluster_remote.get_output("ls $SCRATCH/data/cifar10")
    assert this_cluster_files != other_cluster_files

    await sync([other_cluster], uv_sync_args=None)

    # Dataset is synced
    this_cluster_files = subprocess.getoutput("ls $SCRATCH/data/cifar10")
    other_cluster_files = await other_cluster_remote.get_output("ls $SCRATCH/data/cifar10")
    assert this_cluster_files == other_cluster_files
