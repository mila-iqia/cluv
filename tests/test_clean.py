"""Tests for `cluv clean` and the fetch-watermark mechanism it depends on."""

from __future__ import annotations

import importlib
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest
import rich.prompt

import cluv.__main__ as cluv_main
import cluv.cli.clean
from cluv.cache import CacheContent, ProjectStateOnCluster, read_cache
from cluv.cli.clean import clean, compute_runs_to_delete
from cluv.cli.sync import (
    create_results_dir_with_symlink_to_scratch,
    expandvars,
    fetch_results,
    get_active_remotes,
    sync,
)
from cluv.config import CluvConfig, PartialClusterConfig, get_cluv_config
from cluv.remote import Remote, list_remote_run_dirs, run
from cluv.utils import current_cluster
from tests.test_integration import IN_GITHUB_CLOUD_CI

# `cluv/cli/__init__.py` does `from .sync import sync`, which shadows the `cluv.cli.sync`
# submodule attribute with the `sync` function. Use `importlib.import_module` (same idiom as
# `tests/test_init.py`) to get the actual module object for monkeypatching.
sync_module = importlib.import_module("cluv.cli.sync")


async def test_fetch_results_updates_watermark(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    async def fake_list_remote_run_dirs(remote: Remote, path):
        return [
            ("run_a", datetime(2026, 7, 1, tzinfo=timezone.utc)),
            ("run_b", datetime(2026, 7, 3, tzinfo=timezone.utc)),
        ]

    monkeypatch.setattr(sync_module, list_remote_run_dirs.__name__, fake_list_remote_run_dirs)
    monkeypatch.setattr(
        sync_module,
        create_results_dir_with_symlink_to_scratch.__name__,
        mock.AsyncMock(),
    )
    monkeypatch.setattr(sync_module, run.__name__, mock.AsyncMock())

    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"foo": PartialClusterConfig(project_dir="/home/user/myproject")},
    )
    remote = Remote(hostname="foo")
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


def test_new_runs_are_kept():
    watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    remote_runs = [
        ("new_run_1", datetime(2026, 7, 2, tzinfo=timezone.utc)),
        ("new_run_2", datetime(2026, 7, 5, tzinfo=timezone.utc)),
    ]
    assert compute_runs_to_delete(set(), remote_runs, watermark) == []


@pytest.fixture
def commands_run_during_test(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> list[tuple[str, str]]:
    """Returns the list of (hostname, command) pairs passed to `Remote.run` during the test.

    Can be indirectly parametrized with a dict mapping (hostname, command) to the desired stdout string
    to return for that command. If a command is not in the dict, it will return an empty string.
    This allows simulating different remote outputs for different commands.
    """
    run_calls: list[tuple[str, str]] = []
    command_to_output: dict[tuple[str, str], str] = getattr(request, "param", {})

    async def fake_remote_run(self: Remote, command: str, **kwargs):
        run_calls.append((self.hostname, command))
        if (self.hostname, command) in command_to_output:
            stdout = command_to_output[self.hostname, command]
            return mock.Mock(
                spec=subprocess.CompletedProcess, returncode=0, stdout=stdout, stderr=""
            )
        return mock.Mock(spec=subprocess.CompletedProcess, returncode=0, stderr="")

    monkeypatch.setattr(Remote, "run", fake_remote_run)
    return run_calls  # return the list, which will be modified in-place during the test.


@pytest.fixture
def clean_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two clusters, each reporting one prunable run ("run_pruned") and one kept run
    ("run_kept", which exists locally).

    Returns the list of (hostname, command) pairs passed to `Remote.run`."""
    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"foo": PartialClusterConfig(), "bar": PartialClusterConfig()},
    )
    monkeypatch.setattr(cluv.cli.clean, get_cluv_config.__name__, lambda: config)

    results_path_here = Path(config.results_path)
    results_path_here.mkdir()
    (results_path_here / "foo_run_kept").mkdir()
    (results_path_here / "bar_run_kept").mkdir()

    remotes = [Remote(hostname="foo"), Remote(hostname="bar")]
    monkeypatch.setattr(
        cluv.cli.clean, get_active_remotes.__name__, mock.AsyncMock(return_value=remotes)
    )

    watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
    cache = CacheContent(
        project_states={
            "foo": ProjectStateOnCluster(last_fetch_watermark=watermark),
            "bar": ProjectStateOnCluster(last_fetch_watermark=watermark),
        }
    )
    monkeypatch.setattr(cluv.cli.clean, read_cache.__name__, lambda: cache)

    async def fake_list_remote_run_dirs(remote: Remote, path: str):
        cluster = remote.hostname
        return [
            (f"{cluster}_run_kept", datetime(2026, 6, 20, tzinfo=timezone.utc)),
            (f"{cluster}_run_pruned", datetime(2026, 6, 20, tzinfo=timezone.utc)),
        ]

    monkeypatch.setattr(cluv.cli.clean, list_remote_run_dirs.__name__, fake_list_remote_run_dirs)


async def test_dry_run_makes_no_delete_calls(
    clean_env, commands_run_during_test: list[tuple[str, str]]
):
    await clean(dry_run=True)
    assert (
        commands_run_during_test == []
    )  # no calls were made to Remote.run, so nothing was deleted


async def test_declining_confirmation_makes_no_delete_calls(
    clean_env, commands_run_during_test: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        cluv.cli.clean.Confirm, cluv.cli.clean.Confirm.ask.__name__, lambda *a, **k: False
    )
    await clean()
    assert commands_run_during_test == []


async def test_force_skips_confirmation_and_deletes(
    clean_env, commands_run_during_test: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    ask = mock.Mock()
    monkeypatch.setattr(cluv.cli.clean.Confirm, cluv.cli.clean.Confirm.ask.__name__, ask)

    await clean(force=True)

    ask.assert_not_called()
    assert len(commands_run_during_test) == 2
    for _hostname, command in commands_run_during_test:
        assert "run_pruned" in command
        assert "run_kept" not in command


async def test_confirming_deletes_pruned_run_on_every_cluster(
    clean_env, commands_run_during_test: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        cluv.cli.clean.Confirm,
        cluv.cli.clean.Confirm.ask.__name__,
        mock_ask := mock.Mock(spec=cluv.cli.clean.Confirm.ask, side_effect=lambda *a, **k: True),
    )

    await clean()
    mock_ask.assert_called_once()
    assert {hostname for hostname, _ in commands_run_during_test} == {"foo", "bar"}


@pytest.mark.parametrize(
    commands_run_during_test.__name__,
    [{("foo", "rm -rf /results/foo_run_pruned"): ""}],
    indirect=True,
)
async def test_one_cluster_failure_does_not_abort_the_others(
    clean_env, commands_run_during_test: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        cluv.cli.clean.Confirm,
        cluv.cli.clean.Confirm.ask.__name__,
        mock_confirm := mock.Mock(spec=rich.prompt.Confirm.ask, return_value=True),
    )

    async def failing_remote_run(self, command: str, **kwargs):
        if self.hostname == "mila":
            return mock.Mock(returncode=1, stderr="boom")
        clean_env.append((self.hostname, command))
        return mock.Mock(returncode=0, stderr="")

    monkeypatch.setattr(Remote, "run", failing_remote_run)

    await clean()  # must not raise
    mock_confirm.assert_called_once()

    assert clean_env == [("tamia", mock.ANY)]


async def test_never_synced_cluster_is_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"mila": PartialClusterConfig(), "tamia": PartialClusterConfig()},
    )
    monkeypatch.setattr(cluv.cli.clean, get_cluv_config.__name__, lambda: config)
    (tmp_path / "results").mkdir()

    remotes = [Remote(hostname="mila")]
    monkeypatch.setattr(
        cluv.cli.clean, get_active_remotes.__name__, mock.AsyncMock(return_value=remotes)
    )
    monkeypatch.setattr(cluv.cli.clean, read_cache.__name__, lambda: CacheContent())

    called = mock.AsyncMock()
    monkeypatch.setattr(cluv.cli.clean, list_remote_run_dirs.__name__, called)

    await clean()

    called.assert_not_called()


def test_clean_cli_parses_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cluv_main, cluv_main.clean.__name__, mock_clean := mock.AsyncMock(spec=cluv_main.clean)
    )

    cluv_main.main(["clean"])

    mock_clean.assert_called_once_with(clusters=[], force=False, dry_run=False)


def test_clean_cli_parses_force_and_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cluv_main, cluv_main.clean.__name__, mock_clean := mock.AsyncMock(spec=cluv_main.clean)
    )

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
