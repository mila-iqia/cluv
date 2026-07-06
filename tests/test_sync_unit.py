import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sync_module = importlib.import_module("cluv.cli.sync")


@pytest.mark.asyncio
async def test_sync_uses_newly_created_connection_for_data_source(
    monkeypatch: pytest.MonkeyPatch,
):
    source_remote = SimpleNamespace(hostname="mila")

    monkeypatch.setenv("GITHUB_ACTIONS", "1")
    monkeypatch.setattr(sync_module, "current_cluster", lambda: None)
    monkeypatch.setattr(sync_module, "get_cluv_config", lambda: SimpleNamespace(data_source="mila:/data"))
    monkeypatch.setattr(sync_module, "get_active_remotes", AsyncMock(return_value=[]))
    monkeypatch.setattr(sync_module, "login", AsyncMock(return_value=[source_remote]))
    monkeypatch.setattr(sync_module, "get_datasets_path", lambda: Path("/tmp/datasets"))

    pull_datasets = AsyncMock()
    monkeypatch.setattr(sync_module, "_pull_datasets", pull_datasets)

    async def fake_run_async_tasks_with_progress_bar(*, async_task_fns, **kwargs):
        return [[] for _ in async_task_fns]

    monkeypatch.setattr(
        sync_module, "run_async_tasks_with_progress_bar", fake_run_async_tasks_with_progress_bar
    )

    await sync_module.sync(clusters=["mila"])

    pull_datasets.assert_awaited_once_with(source_remote, "/data", Path("/tmp/datasets"))


@pytest.mark.asyncio
async def test_sync_uses_existing_connection_for_data_source_not_in_sync_targets(
    monkeypatch: pytest.MonkeyPatch,
):
    source_remote = SimpleNamespace(hostname="mila")
    target_remote = SimpleNamespace(hostname="tamia")

    monkeypatch.setenv("GITHUB_ACTIONS", "1")
    monkeypatch.setattr(sync_module, "current_cluster", lambda: None)
    monkeypatch.setattr(sync_module, "get_cluv_config", lambda: SimpleNamespace(data_source="mila:/data"))
    monkeypatch.setattr(sync_module, "get_active_remotes", AsyncMock(return_value=[source_remote]))
    monkeypatch.setattr(sync_module, "login", AsyncMock(return_value=[target_remote]))
    monkeypatch.setattr(sync_module, "get_datasets_path", lambda: Path("/tmp/datasets"))

    pull_datasets = AsyncMock()
    monkeypatch.setattr(sync_module, "_pull_datasets", pull_datasets)

    async def fake_run_async_tasks_with_progress_bar(*, async_task_fns, **kwargs):
        return [[] for _ in async_task_fns]

    monkeypatch.setattr(
        sync_module, "run_async_tasks_with_progress_bar", fake_run_async_tasks_with_progress_bar
    )

    await sync_module.sync(clusters=["tamia"])

    pull_datasets.assert_awaited_once_with(source_remote, "/data", Path("/tmp/datasets"))
