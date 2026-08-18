"""Regression test for a cache-clobbering race in `sync_task_function`.

Each cluster's `sync_task_function` call used to read the whole on-disk cache once at the
start, then repeatedly write that same stale snapshot back after each step. Since `sync()`
runs every cluster's task concurrently, whichever cluster's task saved last would overwrite
every other cluster's just-written state -- including `last_fetch_watermark`, which is
exactly what `cluv clean` depends on to avoid over-deleting.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from unittest import mock

import pytest

from cluv.cache import ProjectStateOnCluster, read_cache, write_cache
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


async def test_save_does_not_clobber_a_concurrently_written_cluster(
    tmp_path, fake_cache_dir, monkeypatch: pytest.MonkeyPatch
):
    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"foo": PartialClusterConfig(project_dir="/home/user/proj")},
    )
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)
    monkeypatch.setattr(sync_module, sync_module.install_uv.__name__, mock.AsyncMock())
    monkeypatch.setattr(sync_module, sync_module.clone_project.__name__, mock.AsyncMock())
    monkeypatch.setattr(sync_module, sync_module.run_uv_sync.__name__, mock.AsyncMock())

    bar_watermark = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_fetch_results(remote, config, project_state):
        # Simulate a sibling cluster's sync_task_function completing and writing its own
        # state to disk while our own task is still in progress.
        cache = read_cache()
        cache.project_states["bar"] = ProjectStateOnCluster(last_fetch_watermark=bar_watermark)
        write_cache(cache)

        project_state.last_fetch_watermark = datetime(2026, 7, 1, tzinfo=timezone.utc)
        return []

    monkeypatch.setattr(sync_module, "fetch_results", fake_fetch_results)

    await sync_module.sync_task_function(
        report_progress=lambda **kwargs: None, remote=Remote(hostname="foo")
    )

    cache = read_cache()
    assert cache.project_states["bar"].last_fetch_watermark == bar_watermark
    assert cache.project_states["foo"].last_fetch_watermark == datetime(
        2026, 7, 1, tzinfo=timezone.utc
    )
