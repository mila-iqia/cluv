"""Unit tests for the per-cluster success/failure reporting in `cluv sync`."""

import importlib
import subprocess
from unittest import mock

import pytest

from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


@pytest.mark.asyncio
async def test_sync_one_cluster_reports_ok(monkeypatch: pytest.MonkeyPatch):
    remote = Remote(hostname="fir")
    monkeypatch.setattr(
        sync_module, sync_module.sync_per_cluster_part.__name__, mock.AsyncMock(return_value=[])
    )
    monkeypatch.setattr(subprocess, "getoutput", lambda *a: "abc123")
    logs: list[str] = []
    monkeypatch.setattr(sync_module.console, "log", lambda *a, **k: logs.append(str(a[0])))

    ok = await sync_module._sync_one_cluster(remote, sync_datasets=False)

    assert ok is True
    assert any("fir: ok abc123" in line for line in logs)


@pytest.mark.asyncio
async def test_sync_one_cluster_reports_failure(monkeypatch: pytest.MonkeyPatch):
    remote = Remote(hostname="fir")

    async def boom(*args, **kwargs):
        raise RuntimeError("git pull failed")

    monkeypatch.setattr(sync_module, sync_module.sync_per_cluster_part.__name__, boom)
    logs: list[str] = []
    monkeypatch.setattr(sync_module.console, "log", lambda *a, **k: logs.append(str(a[0])))

    ok = await sync_module._sync_one_cluster(remote, sync_datasets=False)

    assert ok is False
    assert any("fir: FAILED" in line and "git pull failed" in line for line in logs)
