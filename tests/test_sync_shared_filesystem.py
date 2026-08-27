"""Regression tests for clusters that share a filesystem (e.g. `trillium` / `trillium-gpu`).

`trillium` and `trillium-gpu` are separate login nodes of the same physical cluster that share a
single $HOME (confirmed live via SSH: they mount the identical NFS export) -- but they're
genuinely different Slurm targets: GPU jobs can only be scheduled through `trillium-gpu`, CPU
jobs through `trillium`. So:

- `get_active_remotes()` must still return a `Remote` for *each* of them.
- `sync()` must still return a `Remote` for each of them (so job submission can target either).
- But `_remotes_to_actually_sync()` -- the piece of `sync()` that decides what actually gets
  synced -- must only keep one remote per shared-filesystem group, since it's the exact same
  on-disk checkout -- running it twice is redundant, and running it concurrently is unsafe (this
  is what made `checkout -B <branch> FETCH_HEAD` fail intermittently in the `cluster=first` hydra
  launcher integration test).
"""

from __future__ import annotations

import importlib
from unittest import mock

import pytest

from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


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


async def test_sync_only_syncs_once_but_returns_every_remote(monkeypatch: pytest.MonkeyPatch):
    """`sync(["trillium", "trillium-gpu"])` must run the per-remote sync step only once, but
    return both remotes.

    The dedup logic itself (which remote "wins" a shared-filesystem group) is already covered by
    `test_remotes_to_actually_sync_keeps_only_the_first_per_group`, so here we only need to check
    that `sync()` wires that decision through correctly, by mocking `sync_per_cluster_part` as a
    single unit rather than each of its internal steps.
    """
    # sync() skips the "is HEAD up to date" check (and thus _head_is_up_to_date) entirely when
    # GITHUB_ACTIONS is set, which it is in CI but not locally. Force it unset so this test
    # exercises the same branch -- and the same mock.assert_awaited_once() below -- everywhere.
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
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
    monkeypatch.setattr(
        sync_module,
        sync_module.sync_per_cluster_part.__name__,
        mock_sync_per_cluster_part := mock.AsyncMock(
            sync_module.sync_per_cluster_part, return_value=[]
        ),
    )

    remotes = await sync_module.sync(["trillium", "trillium-gpu"], sync_datasets=False)

    # Both clusters are returned (they're genuinely different Slurm targets)...
    assert {r.hostname for r in remotes} == {"trillium", "trillium-gpu"}
    # ...but the actual sync work only ran once, for the cluster that "wins" the shared group.
    mock_sync_per_cluster_part.assert_awaited_once()
    assert mock_sync_per_cluster_part.await_args.args[0].hostname == "trillium"

    # And every collaborator mocked to get there was actually exercised -- otherwise mocking it
    # would be pointless.
    mock_get_active_remotes.assert_awaited_once()
    mock_login.assert_awaited_once_with(["trillium", "trillium-gpu"])
    mock_head_is_up_to_date.assert_awaited_once()
    mock_get_cluv_config.assert_called()
    # `sync()` resolves `current_cluster()` once itself and once more inside
    # `sync_common_part()` (each is independently self-contained); not exactly once, but exercised.
    mock_current_cluster.assert_called()
    mock_get_disabled_clusters.assert_called_once()
