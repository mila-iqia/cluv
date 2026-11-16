"""Tests for the dataset part of `cluv sync`, with a `data_source` on a remote cluster.

Jobs on the source cluster read the data at `data_source` directly, so syncing with only that
cluster must not copy the data to this machine and back.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from unittest import mock

import pytest

from cluv.cache import ProjectStateOnCluster
from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")
login_module = importlib.import_module("cluv.cli.login")


@pytest.fixture
def config() -> CluvConfig:
    return CluvConfig(
        results_path="/results",
        data_source="mila:/network/datasets/imagenet",
        datasets_path="/scratch/datasets/imagenet",
    )


@pytest.mark.parametrize(
    ("here", "targets", "should_pull"),
    [
        (None, ["mila"], False),
        (None, ["mila", "narval"], True),
        (None, ["narval"], True),
        # The current cluster is a target, and it needs a copy of the data.
        ("narval", ["narval"], True),
        # The current cluster isn't a target, so it doesn't need a copy of the data.
        ("narval", ["mila"], False),
        # The current cluster is the source: the data is already here.
        ("mila", ["narval"], False),
    ],
)
async def test_pull_datasets_only_when_another_cluster_needs_them(
    here: str | None,
    targets: list[str],
    should_pull: bool,
    config: CluvConfig,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        sync_module,
        sync_module.get_remote_without_2fa_prompt.__name__,
        mock.AsyncMock(return_value=Remote(hostname="mila")),
    )
    monkeypatch.setattr(
        sync_module, sync_module.get_datasets_path.__name__, lambda: Path("/local/datasets")
    )
    monkeypatch.setattr(sync_module, "_pull_datasets", mock_pull := mock.AsyncMock())

    # The current cluster is mapped to None, as in `get_cluster_to_remote`.
    cluster_to_remote = {c: None if c == here else Remote(hostname=c) for c in targets}
    await sync_module.pull_datasets_if_needed(here, config, cluster_to_remote)

    assert mock_pull.await_count == (1 if should_pull else 0)


async def test_pull_datasets_with_a_connection_opened_during_sync(
    config: CluvConfig, monkeypatch: pytest.MonkeyPatch
):
    """`cluv sync mila narval` logs in to mila, then pulls the data with that new connection.

    Bug #114: the pull only looked at the connections that were open before the login, so the pull
    raised "need a connection to the source cluster (mila)".
    """
    connected: set[str] = set()  # No SSH connection is open before the command.

    async def control_socket_is_running(hostname: str) -> bool:
        return hostname in connected

    async def connect(hostname: str) -> Remote:  # The login, without the 2FA prompt.
        connected.add(hostname)
        return Remote(hostname=hostname)

    monkeypatch.setattr(
        login_module, control_socket_is_running.__name__, control_socket_is_running
    )
    monkeypatch.setattr(Remote, Remote.connect.__name__, connect)
    # Run as on a local machine, with no disabled cluster.
    for module in (sync_module, login_module):
        monkeypatch.setattr(module, module.current_cluster.__name__, lambda: None)
        monkeypatch.setattr(module, module.get_disabled_clusters.__name__, lambda: {})
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)
    monkeypatch.setattr(sync_module, sync_module.run_git_push_if_needed.__name__, mock.AsyncMock())
    monkeypatch.setattr(
        sync_module, sync_module.sync_per_cluster_part.__name__, mock.AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        sync_module, sync_module.get_datasets_path.__name__, lambda: Path("/local/datasets")
    )
    monkeypatch.setattr(sync_module, "_pull_datasets", mock_pull := mock.AsyncMock())

    # narval needs a copy of the data, so the data is pulled from mila.
    await sync_module.sync(["mila", "narval"])

    assert connected == {"mila", "narval"}  # Both logins happened during the command.
    mock_pull.assert_awaited_once_with(
        Remote(hostname="mila"), "/network/datasets/imagenet", Path("/local/datasets")
    )


async def test_push_datasets_to_a_cluster_without_datasets_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Pushing the data to a cluster that has no `datasets_path` raises an error.

    Without this error, rsync would copy the data to a folder named `None` on that cluster.
    """
    # If no error is raised, these mocks keep `mkdir` and `rsync` from reaching narval.
    monkeypatch.setattr(Remote, "run", mock.AsyncMock())
    monkeypatch.setattr(sync_module, "run", mock.AsyncMock())
    config = CluvConfig(
        results_path="/results",
        data_source="mila:/network/datasets/imagenet",
        clusters={"narval": PartialClusterConfig()},
    )
    with pytest.raises(RuntimeError, match="datasets_path"):
        await sync_module._push_datasets_to_remote(
            tmp_path, Remote(hostname="narval"), config, ProjectStateOnCluster()
        )
