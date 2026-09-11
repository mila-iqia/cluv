"""Unit tests for cluv/ssh.py — pure, no real SSH config touched."""

from pathlib import Path

import pytest

import cluv.ssh as cluv_ssh


@pytest.fixture(autouse=True)
def ssh_config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    (tmp_path / ".ssh").mkdir()
    return tmp_path / ".ssh" / "config"


class TestGetHostnames:
    def test_returns_empty_set_when_no_file(self):
        assert cluv_ssh.get_ssh_hostnames() == set()

    def test_returns_default_wildcard_for_empty_file(self, ssh_config_path: Path):
        ssh_config_path.touch()
        assert cluv_ssh.get_ssh_hostnames() == set("*")

    def test_get_all_hosts(self, ssh_config_path: Path):
        ssh_config_path.write_text("Host mila\nHost narval\nHost rorqual\n")
        assert cluv_ssh.get_ssh_hostnames() == {"*", "mila", "narval", "rorqual"}

    def test_returns_no_duplicates(self, ssh_config_path: Path):
        ssh_config_path.write_text("Host mila\nHost mila\n")
        assert cluv_ssh.get_ssh_hostnames() == {"*", "mila"}
