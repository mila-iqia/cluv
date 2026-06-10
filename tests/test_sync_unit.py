"""Unit tests for sync helpers that don't require real cluster connections."""

import importlib
from types import SimpleNamespace

import pytest

from cluv.cli.sync import create_results_dir_with_symlink_to_scratch


class FakeRemote:
    def __init__(self):
        self.hostname = "killarney"
        self.commands: list[str] = []

    async def get_output(self, command: str, **kwargs) -> str:
        if "echo $SCRATCH/cluv" in command:
            return "/scratch/user/cluv\n"
        if "echo $SCRATCH/logs/cluv" in command:
            return "/scratch/user/logs/cluv\n"
        raise AssertionError(f"Unexpected get_output command: {command}")

    async def run(self, command: str, **kwargs):
        self.commands.append(command)
        return SimpleNamespace(returncode=0)


@pytest.mark.asyncio
async def test_create_results_symlink_uses_configured_project_dir(monkeypatch: pytest.MonkeyPatch):
    sync_module = importlib.import_module("cluv.cli.sync")
    remote = FakeRemote()

    async def fake_remote_test(flag: str, path: str, remote_arg: FakeRemote) -> bool:
        assert remote_arg is remote
        return False

    monkeypatch.setattr(sync_module, "remote_test", fake_remote_test)

    await create_results_dir_with_symlink_to_scratch(
        remote=remote,
        project_dir="$SCRATCH/cluv",
        results_symlink="logs",
        results_path="$SCRATCH/logs/cluv",
    )

    assert "mkdir -p /scratch/user/logs/cluv" in remote.commands
    assert "ln -s -T /scratch/user/logs/cluv /scratch/user/cluv/logs" in remote.commands
