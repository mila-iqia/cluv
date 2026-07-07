"""Tests for `cluv clean` and the fetch-watermark mechanism it depends on."""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from cluv.cache import ProjectStateOnCluster
from cluv.cli.sync import fetch_results
from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

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
