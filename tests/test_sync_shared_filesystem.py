"""Regression test for a race between clusters that share the same filesystem.

Some cluster hostnames (e.g. `trillium` and `trillium-gpu`) are actually distinct login
nodes of the same cluster that share a single $HOME. `sync()` used to run every cluster's
`sync_task_function` fully concurrently, so two such clusters would run `git clone`/`fetch`/
`checkout` on the exact same on-disk repo at the same time -- for example clobbering
`.git/FETCH_HEAD` mid-checkout, which is what caused `checkout -B <branch> FETCH_HEAD` to
fail intermittently in the `cluster=first` hydra launcher integration test.
"""

from __future__ import annotations

import asyncio
import importlib

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


async def test_shared_project_dir_is_not_touched_concurrently(
    tmp_path, fake_cache_dir, monkeypatch: pytest.MonkeyPatch
):
    # "foo" and "bar" stand in for `trillium`/`trillium-gpu`: different hostnames, same
    # (already-expanded, no `$`) project directory.
    shared_project_dir = "/home/user/proj"
    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={
            "foo": PartialClusterConfig(project_dir=shared_project_dir),
            "bar": PartialClusterConfig(project_dir=shared_project_dir),
        },
    )
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)

    currently_running = 0
    max_concurrency_seen = 0

    async def fake_step(*_args, **_kwargs):
        nonlocal currently_running, max_concurrency_seen
        currently_running += 1
        max_concurrency_seen = max(max_concurrency_seen, currently_running)
        # Yield control so a racing task (if not serialized) gets a chance to run here too.
        await asyncio.sleep(0.01)
        currently_running -= 1
        return []

    monkeypatch.setattr(sync_module, sync_module.install_uv.__name__, fake_step)
    monkeypatch.setattr(sync_module, sync_module.clone_project.__name__, fake_step)
    monkeypatch.setattr(sync_module, sync_module.run_uv_sync.__name__, fake_step)
    monkeypatch.setattr(sync_module, "fetch_results", fake_step)

    project_dir_locks: dict[str, asyncio.Lock] = {}
    await asyncio.gather(
        *(
            sync_module.sync_task_function(
                report_progress=lambda **kwargs: None,
                remote=Remote(hostname=hostname),
                project_dir_locks=project_dir_locks,
            )
            for hostname in ("foo", "bar")
        )
    )

    assert max_concurrency_seen == 1, (
        "foo and bar share a project directory, so their sync steps must be serialized, "
        "not run concurrently."
    )
