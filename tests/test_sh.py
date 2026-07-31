"""Tests for `cluv sh`."""

from pathlib import Path
from unittest import mock

import pytest

import cluv.cli.sh as sh_module
from cluv.cli.sh import sh
from cluv.remote import Remote
from cluv.utils import console


@pytest.fixture(autouse=True)
def no_disabled_clusters(monkeypatch: pytest.MonkeyPatch):
    """By default, pretend no clusters are disabled, so tests don't depend on the real cache."""
    monkeypatch.setattr(sh_module, "get_disabled_clusters", lambda: {})


@pytest.fixture(autouse=True)
def not_on_a_cluster(monkeypatch: pytest.MonkeyPatch):
    """By default, pretend we're not running from a cluster (e.g. a dev laptop)."""
    monkeypatch.setattr(sh_module, "current_cluster", lambda: None)


async def test_sh_prints_message_and_returns_when_nothing_to_run_on(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=[]))
    invoke_clush = mock.AsyncMock()
    run_locally = mock.AsyncMock()
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush)
    monkeypatch.setattr(sh_module, "_run_locally", run_locally)

    with console.capture() as cap:
        await sh(["echo", "hi"])

    invoke_clush.assert_not_called()
    run_locally.assert_not_called()
    assert "cluv login" in cap.get()


async def test_sh_writes_hostfile_with_connected_hostnames_and_invokes_clush(
    monkeypatch: pytest.MonkeyPatch,
):
    remotes = [Remote(hostname="mila"), Remote(hostname="narval")]
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=remotes))

    captured: dict = {}

    async def fake_invoke_clush(hostfile: Path, command: list[str]) -> int:
        captured["hostfile_contents"] = Path(hostfile).read_text()
        captured["command"] = command
        return 0

    monkeypatch.setattr(sh_module, "_invoke_clush", fake_invoke_clush)

    await sh(["squeue", "--me"])

    assert captured["hostfile_contents"].splitlines() == ["mila", "narval"]
    assert captured["command"] == ["squeue", "--me"]


async def test_sh_exits_with_clush_return_code_on_failure(monkeypatch: pytest.MonkeyPatch):
    remotes = [Remote(hostname="mila")]
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=remotes))
    monkeypatch.setattr(sh_module, "_invoke_clush", mock.AsyncMock(return_value=17))

    with pytest.raises(SystemExit) as exc_info:
        await sh(["false"])

    assert exc_info.value.code == 17


async def test_sh_runs_locally_when_on_a_cluster_even_with_no_other_connections(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=[]))
    invoke_clush = mock.AsyncMock()
    run_locally = mock.AsyncMock(return_value=0)
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush)
    monkeypatch.setattr(sh_module, "_run_locally", run_locally)

    await sh(["squeue", "--me"])

    run_locally.assert_called_once_with(["squeue", "--me"])
    invoke_clush.assert_not_called()  # nothing to hostfile — no other clusters connected.


async def test_sh_runs_locally_then_clush_in_order_when_both_apply(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(
        sh_module, "get_active_remotes", mock.AsyncMock(return_value=[Remote(hostname="narval")])
    )
    call_order: list[str] = []

    async def fake_run_locally(command: list[str]) -> int:
        call_order.append("local")
        return 0

    async def fake_invoke_clush(hostfile: Path, command: list[str]) -> int:
        call_order.append("clush")
        return 0

    monkeypatch.setattr(sh_module, "_run_locally", fake_run_locally)
    monkeypatch.setattr(sh_module, "_invoke_clush", fake_invoke_clush)

    await sh(["squeue", "--me"])

    assert call_order == ["local", "clush"]


async def test_sh_exits_with_local_return_code_when_local_fails_before_clush_runs(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(
        sh_module, "get_active_remotes", mock.AsyncMock(return_value=[Remote(hostname="narval")])
    )
    monkeypatch.setattr(sh_module, "_run_locally", mock.AsyncMock(return_value=3))
    invoke_clush = mock.AsyncMock(return_value=0)
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush)

    with pytest.raises(SystemExit) as exc_info:
        await sh(["false"])

    invoke_clush.assert_called_once()  # clush still runs even though the local run failed.
    assert exc_info.value.code == 3  # but the local (first) failure's code wins.
