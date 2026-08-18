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
def fake_cache_dir(tmp_path, monkeypatch: pytest.MonkeyPatch):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    import cluv.cache

    monkeypatch.setattr(cluv.cache, cluv.cache._get_cache_dir.__name__, lambda: cache_dir)
    return cache_dir


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
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)
    monkeypatch.setattr(sync_module, sync_module.current_cluster.__name__, lambda: None)
    monkeypatch.setattr(sync_module, sync_module.get_disabled_clusters.__name__, lambda: {})

    async def fake_get_remote_without_2fa_prompt(hostname: str) -> Remote:
        return Remote(hostname=hostname)

    monkeypatch.setattr(
        sync_module,
        sync_module.get_remote_without_2fa_prompt.__name__,
        fake_get_remote_without_2fa_prompt,
    )

    remotes = await sync_module.get_active_remotes()

    assert {r.hostname for r in remotes} == {"trillium", "trillium-gpu", "narval"}


async def test_sync_only_syncs_once_but_returns_every_remote(
    fake_cache_dir, monkeypatch: pytest.MonkeyPatch
):
    """`sync(["trillium", "trillium-gpu"])` must run the sync steps only once, but return both."""
    config = CluvConfig(
        results_path="/results",
        clusters={
            "trillium": PartialClusterConfig(),
            "trillium-gpu": PartialClusterConfig(),
        },
    )
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)
    monkeypatch.setattr(sync_module, sync_module.current_cluster.__name__, lambda: None)
    monkeypatch.setattr(sync_module, sync_module.get_disabled_clusters.__name__, lambda: {})
    monkeypatch.setattr(
        sync_module, sync_module._head_is_up_to_date.__name__, mock.AsyncMock(return_value=True)
    )

    async def fake_login(clusters: list[str], disabled=None) -> list[Remote]:
        return [Remote(hostname=c) for c in clusters]

    monkeypatch.setattr(sync_module, sync_module.login.__name__, fake_login)
    monkeypatch.setattr(
        sync_module, sync_module.get_active_remotes.__name__, mock.AsyncMock(return_value=[])
    )

    sync_calls: list[str] = []

    async def fake_step(remote, *args, **kwargs):
        sync_calls.append(remote.hostname)
        return []

    monkeypatch.setattr(sync_module, sync_module.install_uv.__name__, fake_step)
    monkeypatch.setattr(sync_module, sync_module.clone_project.__name__, fake_step)
    monkeypatch.setattr(sync_module, sync_module.run_uv_sync.__name__, fake_step)
    monkeypatch.setattr(sync_module, "fetch_results", fake_step)

    remotes = await sync_module.sync(["trillium", "trillium-gpu"], sync_datasets=False)

    # Both clusters are returned (they're genuinely different Slurm targets)...
    assert {r.hostname for r in remotes} == {"trillium", "trillium-gpu"}
    # ...but the sync steps (install_uv, clone_project, run_uv_sync, fetch_results) only ran for
    # one of them.
    assert set(sync_calls) == {"trillium"}
