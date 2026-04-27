"""Unit tests for cluv/ssh.py — pure, no real SSH config touched."""

from pathlib import Path

import pytest

import cluv.ssh as cluv_ssh


@pytest.fixture(autouse=True)
def patch_ssh_config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(cluv_ssh, "SSH_CONFIG_PATH", tmp_path / "config")


class TestGetHostnames:
    def test_returns_empty_set_when_no_file(self):
        assert cluv_ssh.get_ssh_hostnames() == set()

    def test_returns_default_wildcard_for_empty_file(self, tmp_path: Path):
        p = tmp_path / "config"
        p.touch()
        assert cluv_ssh.get_ssh_hostnames() == set("*")

    def test_get_all_hosts(self, tmp_path: Path):
        p = tmp_path / "config"
        p.write_text("Host mila\nHost narval\nHost rorqual\n")
        assert cluv_ssh.get_ssh_hostnames() == {"*", "mila", "narval", "rorqual"}

    def test_returns_no_duplicates(self, tmp_path: Path):
        p = tmp_path / "config"
        p.write_text("Host mila\nHost mila\n")
        assert cluv_ssh.get_ssh_hostnames() == {"*", "mila"}
