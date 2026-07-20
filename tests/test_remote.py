"""Tests for cluv.remote helpers that don't require a real SSH connection."""

from datetime import datetime, timezone
from pathlib import PurePosixPath

import pytest

from cluv.remote import Remote, list_remote_run_dirs


async def test_list_remote_run_dirs_parses_find_output(monkeypatch: pytest.MonkeyPatch):
    async def fake_get_output(self, command: str, **kwargs) -> str:
        assert command.startswith("find ")
        return "1751328000.0 run_a\n1751414400.5 run_b\n"

    monkeypatch.setattr(Remote, Remote.get_output.__name__, fake_get_output)

    remote = Remote(hostname="mila")
    result = await list_remote_run_dirs(remote, PurePosixPath("/scratch/results"))

    assert result == [
        ("run_a", datetime.fromtimestamp(1751328000.0, tz=timezone.utc)),
        ("run_b", datetime.fromtimestamp(1751414400.5, tz=timezone.utc)),
    ]


async def test_list_remote_run_dirs_empty_when_path_missing(monkeypatch: pytest.MonkeyPatch):
    calls = []

    async def fake_get_output(self, command: str, **kwargs) -> str:
        calls.append(kwargs)
        return ""

    monkeypatch.setattr(Remote, Remote.get_output.__name__, fake_get_output)

    remote = Remote(hostname="mila")
    result = await list_remote_run_dirs(remote, PurePosixPath("/scratch/results"))

    assert result == []
    assert calls[0].get("warn") is True
