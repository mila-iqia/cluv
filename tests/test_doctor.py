"""Tests for `cluv doctor`."""

from __future__ import annotations

import subprocess

import pytest

import cluv.cli.doctor as doctor_module
from cluv.cli.doctor import (
    Status,
    check_accounts,
    check_cluster,
    check_github_auth,
    check_project_dir,
    check_ssh_config_entry,
    doctor,
)
from cluv.config import CluvConfig
from cluv.remote import Remote


def make_config(**cluster_config) -> CluvConfig:
    return CluvConfig.model_validate(
        {"results_path": "$SCRATCH/logs", "clusters": {"narval": cluster_config}}
    )


@pytest.fixture
def fake_remote_outputs(monkeypatch: pytest.MonkeyPatch):
    """Maps a substring of a remote command to the (returncode, stdout, stderr) it returns."""
    outputs: dict[str, tuple[int, str, str]] = {}

    async def _run(self: Remote, command: str, **kwargs) -> subprocess.CompletedProcess[str]:
        returncode, stdout, stderr = next(
            (v for k, v in outputs.items() if k in command), (0, "", "")
        )
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    monkeypatch.setattr(Remote, "run", _run)
    return outputs


@pytest.mark.parametrize(
    "hostnames, expected",
    [({"*", "narval"}, Status.OK), ({"*"}, Status.FAIL), ({"narv*"}, Status.OK)],
)
def test_check_ssh_config_entry(monkeypatch: pytest.MonkeyPatch, hostnames, expected):
    monkeypatch.setattr(doctor_module, "get_ssh_hostnames", lambda: hostnames)
    assert check_ssh_config_entry("narval").status is expected


@pytest.mark.parametrize(
    "stderr, expected",
    [
        ("Hi bob! You've successfully authenticated, but GitHub does not ...", Status.OK),
        ("git@github.com: Permission denied (publickey).", Status.FAIL),
    ],
)
async def test_check_github_auth(fake_remote_outputs, stderr: str, expected: Status):
    fake_remote_outputs["git@github.com"] = (1, "", stderr)
    assert (await check_github_auth(Remote("narval"))).status is expected


@pytest.mark.parametrize(
    "account, expected",
    [("def-prof", Status.OK), ("rrg-prof", Status.FAIL), ("def-prof_gpu", Status.OK)],
)
async def test_check_accounts(fake_remote_outputs, account: str, expected: Status):
    fake_remote_outputs["sacctmgr"] = (0, "def-prof_cpu\ndef-prof_gpu\n", "")
    config = make_config(sbatch_args={"account": account})
    assert (await check_accounts(config, Remote("narval"))).status is expected


async def test_check_accounts_unset(fake_remote_outputs):
    assert (await check_accounts(make_config(), Remote("narval"))).status is Status.OK


@pytest.mark.parametrize(
    "outputs, expected",
    [
        ({"set -u": (0, "/scratch/me/proj\n", ""), "test -d": (0, "", "")}, Status.OK),
        ({"set -u": (0, "/scratch/me/proj\n", ""), "test -d": (1, "", "")}, Status.WARN),
        ({"set -u": (1, "", "SCRATCH: unbound variable")}, Status.FAIL),
    ],
)
async def test_check_project_dir(fake_remote_outputs, outputs, expected: Status):
    fake_remote_outputs.update(outputs)
    config = make_config(project_dir="$SCRATCH/proj")
    assert (await check_project_dir(config, Remote("narval"))).status is expected


async def test_check_cluster_not_configured():
    [check] = await check_cluster(make_config(), "rorqual")
    assert check.status is Status.FAIL


async def test_check_cluster_skips_remote_checks_when_disconnected(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _not_running(host: str) -> bool:
        return False

    monkeypatch.setattr(doctor_module, "control_socket_is_running", _not_running)
    monkeypatch.setattr(doctor_module, "current_cluster", lambda: None)
    checks = await check_cluster(make_config(), "narval")
    assert checks[-2].status is Status.FAIL
    assert "cluv login narval" in checks[-2].hint
    assert checks[-1].status is Status.SKIP


async def test_doctor_exits_with_error_on_failure(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(doctor_module, "get_cluv_config", make_config)
    with pytest.raises(SystemExit):
        await doctor(["rorqual"])
