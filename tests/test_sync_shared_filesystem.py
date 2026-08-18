"""Regression tests for clusters that share a filesystem (e.g. `trillium` / `trillium-gpu`).

`trillium` and `trillium-gpu` are separate login nodes of the same physical cluster that share a
single $HOME (confirmed live via SSH: they mount the identical NFS export) -- but they're
genuinely different Slurm targets: GPU jobs can only be scheduled through `trillium-gpu`, CPU
jobs through `trillium`. So:

- `get_active_remotes()` must still return a `Remote` for *each* of them.
- `sync()` must still return a `Remote` for each of them (so job submission can target either).
- But `sync()` must only actually run the sync steps (git clone/checkout, `uv sync`, fetching
  results) once for the pair, since it's the exact same on-disk checkout -- running it twice is
  redundant, and running it concurrently is unsafe (this is what made
  `checkout -B <branch> FETCH_HEAD` fail intermittently in the `cluster=first` hydra launcher
  integration test).
"""

from __future__ import annotations

import importlib
from unittest import mock

import pytest

from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


@pytest.fixture
def fake_cache_dir(tmp_path, monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    import cluv.cache

    monkeypatch.setattr(
        cluv.cache,
        cluv.cache._get_cache_dir.__name__,
        mock_get_cache_dir := mock.Mock(cluv.cache._get_cache_dir, return_value=cache_dir),
    )
    return mock_get_cache_dir


@pytest.mark.parametrize(
    ("hostnames", "expected"),
    [
        (["mila", "trillium", "narval", "trillium-gpu"], ["mila", "trillium", "narval"]),
        (["trillium-gpu", "trillium", "mila"], ["trillium-gpu", "mila"]),
        (["mila", "narval", "tamia"], ["mila", "narval", "tamia"]),
        (["trillium"], ["trillium"]),
    ],
)
def test_remotes_to_actually_sync_keeps_only_the_first_per_group(hostnames, expected):
    remotes = [Remote(hostname=h) for h in hostnames]
    result = sync_module._remotes_to_actually_sync(remotes)
    assert [r.hostname for r in result] == expected


async def test_get_active_remotes_returns_every_active_cluster(monkeypatch: pytest.MonkeyPatch):
    """Even clusters sharing a filesystem must all come back: they're distinct Slurm targets."""
    config = CluvConfig(
        results_path="/results",
        clusters={
            "trillium": PartialClusterConfig(),
            "trillium-gpu": PartialClusterConfig(),
            "narval": PartialClusterConfig(),
        },
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.get_cluv_config.__name__,
        mock_get_cluv_config := mock.Mock(sync_module.get_cluv_config, return_value=config),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.current_cluster.__name__,
        mock_current_cluster := mock.Mock(sync_module.current_cluster, return_value=None),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.get_disabled_clusters.__name__,
        mock_get_disabled_clusters := mock.Mock(
            sync_module.get_disabled_clusters, return_value={}
        ),
    )

    async def fake_get_remote_without_2fa_prompt(hostname: str) -> Remote:
        return Remote(hostname=hostname)

    monkeypatch.setattr(
        sync_module,
        sync_module.get_remote_without_2fa_prompt.__name__,
        mock_get_remote := mock.AsyncMock(
            sync_module.get_remote_without_2fa_prompt,
            side_effect=fake_get_remote_without_2fa_prompt,
        ),
    )

    remotes = await sync_module.get_active_remotes()

    assert {r.hostname for r in remotes} == {"trillium", "trillium-gpu", "narval"}
    # Every collaborator was actually consulted -- otherwise mocking it would be pointless.
    mock_get_cluv_config.assert_called_once()
    mock_current_cluster.assert_called_once()
    mock_get_disabled_clusters.assert_called_once()
    assert mock_get_remote.await_count == 3
    mock_get_remote.assert_any_await("trillium")
    mock_get_remote.assert_any_await("trillium-gpu")
    mock_get_remote.assert_any_await("narval")


async def test_sync_only_syncs_once_but_returns_every_remote(
    fake_cache_dir: mock.Mock, monkeypatch: pytest.MonkeyPatch
):
    """`sync(["trillium", "trillium-gpu"])` must run the sync steps only once, but return both."""
    config = CluvConfig(
        results_path="/results",
        clusters={
            "trillium": PartialClusterConfig(),
            "trillium-gpu": PartialClusterConfig(),
        },
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.get_cluv_config.__name__,
        mock_get_cluv_config := mock.Mock(sync_module.get_cluv_config, return_value=config),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.current_cluster.__name__,
        mock_current_cluster := mock.Mock(sync_module.current_cluster, return_value=None),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.get_disabled_clusters.__name__,
        mock_get_disabled_clusters := mock.Mock(
            sync_module.get_disabled_clusters, return_value={}
        ),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module._head_is_up_to_date.__name__,
        mock_head_is_up_to_date := mock.AsyncMock(
            sync_module._head_is_up_to_date, return_value=True
        ),
    )

    async def fake_login(clusters: list[str], disabled=None) -> list[Remote]:
        return [Remote(hostname=c) for c in clusters]

    monkeypatch.setattr(
        sync_module,
        sync_module.login.__name__,
        mock_login := mock.AsyncMock(sync_module.login, side_effect=fake_login),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.get_active_remotes.__name__,
        mock_get_active_remotes := mock.AsyncMock(sync_module.get_active_remotes, return_value=[]),
    )

    sync_calls: list[str] = []

    async def fake_step(remote, *args, **kwargs):
        sync_calls.append(remote.hostname)
        return []

    monkeypatch.setattr(
        sync_module,
        sync_module.install_uv.__name__,
        mock_install_uv := mock.AsyncMock(sync_module.install_uv, side_effect=fake_step),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.clone_project.__name__,
        mock_clone_project := mock.AsyncMock(sync_module.clone_project, side_effect=fake_step),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.run_uv_sync.__name__,
        mock_run_uv_sync := mock.AsyncMock(sync_module.run_uv_sync, side_effect=fake_step),
    )
    monkeypatch.setattr(
        sync_module,
        sync_module.fetch_results.__name__,
        mock_fetch_results := mock.AsyncMock(sync_module.fetch_results, side_effect=fake_step),
    )

    remotes = await sync_module.sync(["trillium", "trillium-gpu"], sync_datasets=False)

    # Both clusters are returned (they're genuinely different Slurm targets)...
    assert {r.hostname for r in remotes} == {"trillium", "trillium-gpu"}
    # ...but the sync steps (install_uv, clone_project, run_uv_sync, fetch_results) only ran for
    # one of them.
    assert set(sync_calls) == {"trillium"}
    mock_install_uv.assert_awaited_once()
    mock_clone_project.assert_awaited_once()
    mock_run_uv_sync.assert_awaited_once()
    mock_fetch_results.assert_awaited_once()

    # And every collaborator mocked to get there was actually exercised -- otherwise mocking it
    # would be pointless.
    mock_get_active_remotes.assert_awaited_once()
    mock_login.assert_awaited_once_with(["trillium", "trillium-gpu"], disabled={})
    mock_head_is_up_to_date.assert_awaited_once()
    fake_cache_dir.assert_called()
    mock_get_cluv_config.assert_called()
    mock_current_cluster.assert_called_once()
    mock_get_disabled_clusters.assert_called_once()
