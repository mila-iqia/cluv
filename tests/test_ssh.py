"""Unit tests for cluv/ssh.py — pure, no real SSH config touched."""

import cluv.ssh as cluv_ssh
from .utils import write_file
import pytest


@pytest.fixture(autouse=True)
def patch_ssh_config_path(tmp_path, monkeypatch):
    monkeypatch.setattr(cluv_ssh, "SSH_CONFIG_PATH", tmp_path / "config")


class TestGetHostnames:
    def test_returns_empty_set_when_no_file(self):
        assert cluv_ssh.get_ssh_hostnames() == set()

    def test_returns_empty_set_for_empty_file(self, tmp_path):
        write_file(tmp_path, "", "config")
        assert cluv_ssh.get_ssh_hostnames() == set("*")

    def test_get_all_hosts(self, tmp_path):
        write_file(tmp_path, "Host mila\nHost narval\nHost rorqual\n", "config")
        assert cluv_ssh.get_ssh_hostnames() == {"*", "mila", "narval", "rorqual"}

    def test_returns_no_duplicates(self, tmp_path):
        write_file(tmp_path, "Host mila\nHost mila\n", "config")
        assert cluv_ssh.get_ssh_hostnames() == {"*", "mila"}
