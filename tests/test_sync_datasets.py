"""Tests for the dataset part of `cluv sync`, with a `data_source` on a remote cluster.

Jobs on the source cluster read the data at `data_source` directly, so syncing with only that
cluster must not copy the data to this machine and back (issue #200).
"""

from __future__ import annotations

import importlib
from pathlib import Path
from unittest import mock

import pytest

from cluv.cache import CacheContent
from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> CluvConfig:
    config = CluvConfig(
        results_path="/results",
        data_source="mila:/network/datasets/imagenet",
        datasets_path="/scratch/datasets/imagenet",
        clusters={
            "mila": PartialClusterConfig(project_dir="/home/user/proj"),
            "narval": PartialClusterConfig(project_dir="/home/user/proj"),
        },
    )
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)
    return config


@pytest.mark.parametrize(
    ("targets", "should_pull"),
    [(["mila"], False), (["mila", "narval"], True), (["narval"], True)],
)
async def test_pull_datasets_only_when_another_cluster_needs_them(
    targets: list[str], should_pull: bool, config: CluvConfig, monkeypatch: pytest.MonkeyPatch
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

    await sync_module.pull_datasets_if_needed(None, config, [Remote(hostname=c) for c in targets])

    assert mock_pull.await_count == (1 if should_pull else 0)


@pytest.mark.usefixtures("config")
@pytest.mark.parametrize(("cluster", "should_push"), [("mila", False), ("narval", True)])
async def test_push_datasets_except_to_the_source_cluster(
    cluster: str, should_push: bool, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(sync_module, sync_module.current_cluster.__name__, lambda: None)
    monkeypatch.setattr(sync_module, sync_module.read_cache.__name__, CacheContent)
    monkeypatch.setattr(sync_module, sync_module.write_cache.__name__, mock.Mock())
    monkeypatch.setattr(sync_module, sync_module.install_uv.__name__, mock.AsyncMock())
    monkeypatch.setattr(sync_module, sync_module.clone_project.__name__, mock.AsyncMock())
    monkeypatch.setattr(sync_module, sync_module.run_uv_sync.__name__, mock.AsyncMock())
    monkeypatch.setattr(
        sync_module, sync_module.fetch_results.__name__, mock.AsyncMock(return_value=[])
    )
    monkeypatch.setattr(sync_module, "_push_datasets_to_remote", mock_push := mock.AsyncMock())

    await sync_module.sync_per_cluster_part(Remote(hostname=cluster))

    assert mock_push.await_count == (1 if should_push else 0)
