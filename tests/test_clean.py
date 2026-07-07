"""Tests for `cluv clean` and the fetch-watermark mechanism it depends on."""

from __future__ import annotations

import importlib
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

import cluv.__main__ as cluv_main
import cluv.cli.clean
from cluv.cache import CacheContent, ProjectStateOnCluster
from cluv.cli.clean import clean, compute_runs_to_delete
from cluv.cli.sync import expandvars, fetch_results, sync
from cluv.config import CluvConfig, PartialClusterConfig, get_cluv_config
from cluv.remote import Remote
from cluv.utils import current_cluster
from tests.test_integration import IN_GITHUB_CLOUD_CI

# `cluv/cli/__init__.py` does `from .sync import sync`, which shadows the `cluv.cli.sync`
# submodule attribute with the `sync` function. Use `importlib.import_module` (same idiom as
# `tests/test_init.py`) to get the actual module object for monkeypatching.
sync_module = importlib.import_module("cluv.cli.sync")


async def test_fetch_results_updates_watermark(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    async def fake_list_remote_run_dirs(remote, path):
        return [
            ("run_a", datetime(2026, 7, 1, tzinfo=timezone.utc)),
            ("run_b", datetime(2026, 7, 3, tzinfo=timezone.utc)),
        ]

    monkeypatch.setattr(sync_module, "list_remote_run_dirs", fake_list_remote_run_dirs)
    monkeypatch.setattr(
        sync_module, "create_results_dir_with_symlink_to_scratch", mock.AsyncMock()
    )
    monkeypatch.setattr(sync_module, "run", mock.AsyncMock())

    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"mila": PartialClusterConfig(project_dir="/home/user/myproject")},
    )
    remote = Remote(hostname="mila")
    project_state = ProjectStateOnCluster()

    await fetch_results(remote, config, project_state)

    assert project_state.last_fetch_watermark == datetime(2026, 7, 3, tzinfo=timezone.utc)


def test_pruned_run_is_selected():
    watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    remote_runs = [("run_a", datetime(2026, 6, 30, tzinfo=timezone.utc))]
    assert compute_runs_to_delete(set(), remote_runs, watermark) == ["run_a"]


def test_run_present_locally_is_kept():
    watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    remote_runs = [("run_a", datetime(2026, 6, 30, tzinfo=timezone.utc))]
    assert compute_runs_to_delete({"run_a"}, remote_runs, watermark) == []


def test_never_synced_cluster_selects_nothing():
    remote_runs = [("run_a", datetime(2026, 6, 30, tzinfo=timezone.utc))]
    assert compute_runs_to_delete(set(), remote_runs, watermark=None) == []


def test_new_run_not_the_newest_is_still_kept():
    """Guards against re-deriving the watermark as max(remote mtimes) *inside* this
    function instead of using the externally-supplied, previously-cached watermark --
    the over-deletion bug found while designing this feature (see the design spec's
    "Rejected alternative: sync-first" section)."""
    watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    remote_runs = [
        ("new_run_1", datetime(2026, 7, 2, tzinfo=timezone.utc)),
        ("new_run_2", datetime(2026, 7, 5, tzinfo=timezone.utc)),
    ]
    assert compute_runs_to_delete(set(), remote_runs, watermark) == []


def _config(tmp_path: Path) -> CluvConfig:
    return CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"mila": PartialClusterConfig(), "tamia": PartialClusterConfig()},
    )


@pytest.fixture
def clean_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Two clusters, each reporting one prunable run ("run_pruned") and one kept run
    ("run_kept", which exists locally). Returns the list of (hostname, command) pairs
    passed to `Remote.run` -- i.e. the deletion calls actually made."""
    config = _config(tmp_path)
    monkeypatch.setattr(cluv.cli.clean, "get_cluv_config", lambda: config)

    results_path_here = tmp_path / "results"
    results_path_here.mkdir()
    (results_path_here / "run_kept").mkdir()

    remotes = [Remote(hostname="mila"), Remote(hostname="tamia")]
    monkeypatch.setattr(cluv.cli.clean, "get_active_remotes", mock.AsyncMock(return_value=remotes))

    watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    cache = CacheContent(
        project_states={
            "mila": ProjectStateOnCluster(last_fetch_watermark=watermark),
            "tamia": ProjectStateOnCluster(last_fetch_watermark=watermark),
        }
    )
    monkeypatch.setattr(cluv.cli.clean, "read_cache", lambda: cache)

    async def fake_list_remote_run_dirs(remote, path):
        return [
            ("run_kept", datetime(2026, 6, 20, tzinfo=timezone.utc)),
            ("run_pruned", datetime(2026, 6, 20, tzinfo=timezone.utc)),
        ]

    monkeypatch.setattr(cluv.cli.clean, "list_remote_run_dirs", fake_list_remote_run_dirs)

    run_calls: list[tuple[str, str]] = []

    async def fake_remote_run(self, command: str, **kwargs):
        run_calls.append((self.hostname, command))
        return mock.Mock(returncode=0, stderr="")

    monkeypatch.setattr(Remote, "run", fake_remote_run)
    return run_calls


async def test_dry_run_makes_no_delete_calls(clean_env: list[tuple[str, str]]):
    await clean(dry_run=True)
    assert clean_env == []


async def test_declining_confirmation_makes_no_delete_calls(
    clean_env: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(cluv.cli.clean.Confirm, "ask", lambda *a, **k: False)
    await clean()
    assert clean_env == []


async def test_force_skips_confirmation_and_deletes(
    clean_env: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    ask = mock.Mock()
    monkeypatch.setattr(cluv.cli.clean.Confirm, "ask", ask)

    await clean(force=True)

    ask.assert_not_called()
    assert len(clean_env) == 2
    for _hostname, command in clean_env:
        assert "run_pruned" in command
        assert "run_kept" not in command


async def test_confirming_deletes_pruned_run_on_every_cluster(
    clean_env: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(cluv.cli.clean.Confirm, "ask", lambda *a, **k: True)

    await clean()

    assert {hostname for hostname, _ in clean_env} == {"mila", "tamia"}


async def test_one_cluster_failure_does_not_abort_the_others(
    clean_env: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(cluv.cli.clean.Confirm, "ask", lambda *a, **k: True)

    async def failing_remote_run(self, command: str, **kwargs):
        if self.hostname == "mila":
            return mock.Mock(returncode=1, stderr="boom")
        clean_env.append((self.hostname, command))
        return mock.Mock(returncode=0, stderr="")

    monkeypatch.setattr(Remote, "run", failing_remote_run)

    await clean()  # must not raise

    assert clean_env == [("tamia", mock.ANY)]


async def test_never_synced_cluster_is_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = _config(tmp_path)
    monkeypatch.setattr(cluv.cli.clean, "get_cluv_config", lambda: config)
    (tmp_path / "results").mkdir()

    remotes = [Remote(hostname="mila")]
    monkeypatch.setattr(cluv.cli.clean, "get_active_remotes", mock.AsyncMock(return_value=remotes))
    monkeypatch.setattr(cluv.cli.clean, "read_cache", lambda: CacheContent())

    called = mock.AsyncMock()
    monkeypatch.setattr(cluv.cli.clean, "list_remote_run_dirs", called)

    await clean()

    called.assert_not_called()


def test_clean_cli_parses_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cluv_main, "clean", mock_clean := mock.AsyncMock(spec=cluv_main.clean))

    cluv_main.main(["clean"])

    mock_clean.assert_called_once_with(clusters=[], force=False, dry_run=False)


def test_clean_cli_parses_force_and_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cluv_main, "clean", mock_clean := mock.AsyncMock(spec=cluv_main.clean))

    cluv_main.main(["clean", "rorqual", "narval", "--force", "--dry-run"])

    mock_clean.assert_called_once_with(clusters=["rorqual", "narval"], force=True, dry_run=True)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(
    IN_GITHUB_CLOUD_CI,
    reason="Integration tests are only run on a self-hosted github runner or on a dev machine.",
)
async def test_clean_removes_pruned_run_but_keeps_new_one(
    monkeypatch: pytest.MonkeyPatch, fake_scratch: Path
):
    """End-to-end: an old, locally-pruned run is removed from the cluster; a brand-new,
    never-fetched run is left alone."""
    assert not current_cluster(), "test needs to run locally for now."
    cluster = "tamia"
    remote = await Remote.connect(cluster)

    monkeypatch.chdir("examples/pytorch-example")
    config = get_cluv_config()
    results_path_on_cluster = await expandvars(
        remote, config.get_cluster_config(cluster).results_path
    )

    await remote.run(f"rm -rf {results_path_on_cluster}", warn=True, hide=True)
    await remote.run(f"mkdir -p {results_path_on_cluster}", hide=True)

    old_run = results_path_on_cluster / "old_run"
    await remote.run(f"mkdir -p {old_run}", hide=True)
    await remote.run(f"touch -d '2020-01-01' {old_run}", hide=True)

    await sync([cluster], uv_sync_args=None)

    local_results_path = Path(os.path.expandvars(config.results_path))
    shutil.rmtree(local_results_path / "old_run")

    new_run = results_path_on_cluster / "new_run"
    await remote.run(f"mkdir -p {new_run}", hide=True)

    await clean([cluster], force=True)

    remaining = (
        (await remote.get_output(f"ls {results_path_on_cluster}", warn=True)).strip().splitlines()
    )
    assert "old_run" not in remaining
    assert "new_run" in remaining
