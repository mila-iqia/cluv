"""Unit tests for cluv/cli/init.py check functions."""

from pathlib import Path

import pytest
from milatools.cli.init_command import DRAC_CLUSTERS

from cluv.config import load_cluv_config
from cluv.cli.init import (
    check_home_dir,
    check_git,
    check_cluv_config,
    check_symlink_to_scratch,
    DEFAULT_RESULTS_PATH
)
from .utils import write_pyproject


class TestCheckHomeDir:
    def test_not_under_home(self, tmp_path, monkeypatch) -> None:
        """check_home_dir() should raise an error if the current directory is not under the user's home directory"""
        monkeypatch.setattr(Path, "home", lambda: str(tmp_path)) # Set the home directory to tmp_path
        monkeypatch.chdir(tmp_path.parent) # Set the current work dir to the parent of tmp_path, which is not under the "home" directory

        with pytest.raises(RuntimeError, match="cluv init should be run in a directory under your home directory."):
            check_home_dir()


class TestGitCheck:
    def test_not_in_git_repo(self, tmp_path, monkeypatch) -> None:
        """check_git() should raise an error if the current directory is not a git repository"""
        monkeypatch.chdir(tmp_path) # Set the working dir to tmp_path

        with pytest.raises(RuntimeError, match="Error when checking git remote: "):
            check_git()


class TestCheckCluvConfig:
    def test_add_missing_cluv_config(self, tmp_path) -> None:
        """check_cluv_config() should add a cluv config section if the toml doesn't have it"""
        p = write_pyproject(tmp_path, "")

        results_path = check_cluv_config(p)
        config = load_cluv_config(p)

        assert results_path == DEFAULT_RESULTS_PATH
        assert config.clusters == ["mila"] + DRAC_CLUSTERS
        assert config.results_path == DEFAULT_RESULTS_PATH
        assert config.slurm == {'UV_OFFLINE': 1, 'WANDB_MODE': 'offline'}
        assert config.cluster_configs == {"mila": {"UV_OFFLINE": 0, "WANDB_MODE": "online"}}


    def test_keep_existing_cluv_config(self, tmp_path) -> None:
        """check_cluv_config() should not overwrite an existing cluv config"""
        p = write_pyproject(tmp_path, """
[tool.cluv]
clusters = ["mila"]
results_path = "results"
""")

        results_path = check_cluv_config(p)
        config = load_cluv_config(p)

        assert results_path == "results"
        assert config.clusters == ["mila"]
        assert config.results_path == "results"


class TestSymlinkCheck():
    def test_no_symlink_if_results_path_is_none(self, tmp_path) -> None:
        """check_symlink_to_scratch() should not create a symlink if the results_path is None"""
        check_symlink_to_scratch(tmp_path, None)

        assert not (tmp_path / DEFAULT_RESULTS_PATH).exists()


    def test_no_symlink_if_scratch_not_set(self, tmp_path, monkeypatch) -> None:
        """check_symlink_to_scratch() should not create a symlink if the $SCRATCH env var is not set"""
        monkeypatch.delenv("SCRATCH", raising=False)

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH)

        assert not (tmp_path / DEFAULT_RESULTS_PATH).exists()


    def test_create_symlink(self, tmp_path, monkeypatch) -> None:
        """check_symlink_to_scratch() should create a symlink from results_path to scratch"""
        scratch_path = tmp_path / "scratch"
        monkeypatch.setenv("SCRATCH", str(scratch_path))
        expected_results_path = tmp_path / DEFAULT_RESULTS_PATH
        expected_results_scratch_path = scratch_path / DEFAULT_RESULTS_PATH / tmp_path.name

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH)

        assert expected_results_path.exists()
        assert expected_results_path.is_symlink()
        assert expected_results_scratch_path.exists()
        assert expected_results_path.resolve() == expected_results_scratch_path.resolve()
