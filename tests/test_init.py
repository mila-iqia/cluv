import os

from cluv.config import get_config
from cluv.cli.init import check_git, init
from milatools.cli.init_command import DRAC_CLUSTERS

import pytest

class TestInitGitCheck:
    def test_init_fails_if_not_in_git_repo(self, tmp_path, monkeypatch) -> None:
        """check_git() should raise an error if the current directory is not a git repository"""
        monkeypatch.chdir(tmp_path) # No git project in tmp_path

        with pytest.raises(RuntimeError, match="The current project is not a git repository. Try running 'git init' or clone a GitHub project."):
            check_git()

class TestInitCluvConfig:
    def test_init_fails_if_not_under_home(self, tmp_path, monkeypatch) -> None:
        """init() should raise an error if the current directory is not under the user's home directory"""
        monkeypatch.setattr(os.path, "expanduser", lambda _: str(tmp_path)) # Set the home directory to tmp_path
        monkeypatch.chdir(tmp_path.parent) # Move to the parent of tmp_path, which is not under the "home" directory

        with pytest.raises(RuntimeError, match="cluv init should be run in a directory under your home directory."):
            init()

    def test_init_generates_toml_default_config(self, tmp_path, monkeypatch) -> None:
        """init() should create a pyproject.toml file with the default configuration if it doesn't exist"""
        monkeypatch.setattr(os.path, "expanduser", lambda _: str(tmp_path)) # Set the home directory to tmp_path to pass the home check
        monkeypatch.chdir(tmp_path)

        init()
        config = get_config()

        assert config.clusters == ["mila"] + DRAC_CLUSTERS
        assert config.results_path == "logs"
        assert config.slurm == {'UV_OFFLINE': 1, 'WANDB_MODE': 'offline'}
        assert config.cluster_configs == {"mila": {"UV_OFFLINE": 0, "WANDB_MODE": "online"}}
