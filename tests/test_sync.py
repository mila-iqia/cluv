"""Tests for `cluv sync`"""

import subprocess

import pytest

from cluv.cli.sync import sync
from cluv.config import current_cluster_config, load_cluv_config
from cluv.job import get_datasets_path
from cluv.remote import Remote

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
    """Test for `cluv sync` with a project that has a 'datasets_path'.

    Need to check that rsync happens from `data_source` (the source) to the `datasets_path` here and
    on all the clusters where we have a connection.
    """
    other_cluster = "tamia"
    other_cluster_remote = await Remote.connect(other_cluster)

    monkeypatch.chdir("examples/pytorch-example")
    # assert current_cluster() == "mila"

    config = load_cluv_config()
    assert config
    assert config.datasets_path

    here_config = current_cluster_config()
    here_datasets_path = get_datasets_path() if here_config else None
    assert here_datasets_path

    other_cluster_datasets_path = config.get_cluster_config(other_cluster).datasets_path
    assert other_cluster_datasets_path

    other_cluster_files = await other_cluster_remote.get_output(
        f"ls {other_cluster_datasets_path}", warn=True, hide=True
    )
    if other_cluster_files:
        # Clean up any existing dataset on the other cluster
        await other_cluster_remote.run(f"rm -r {other_cluster_datasets_path}")
        other_cluster_files = ""

    # Dataset isn't synced to begin with.
    this_cluster_files = subprocess.getoutput(f"ls {here_datasets_path}").strip().splitlines()
    assert this_cluster_files != other_cluster_files

    await sync([other_cluster], uv_sync_args=None)

    # Dataset is synced
    this_cluster_files = subprocess.getoutput(f"ls {here_datasets_path}").strip().splitlines()
    other_cluster_files = (
        (await other_cluster_remote.get_output(f"ls {other_cluster_datasets_path}"))
        .strip()
        .splitlines()
    )
    assert this_cluster_files == other_cluster_files
