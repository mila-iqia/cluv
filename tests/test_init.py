from pathlib import Path

from cluv.config import get_config
from cluv.cli.init import check_git, init, DEFAULT_RESULTS_PATH, JOB_SCRIPT_PATH
from milatools.cli.init_command import DRAC_CLUSTERS
from .utils import write_pyproject

import cluv.cli.init
import pytest

TEST_RESULTS_PATH = "test_results"

@pytest.fixture
def clean_config_cache():
    """
    To avoid that a test reads the cached config of an other, we need to clear the cache between each test.
    """
    get_config.cache_clear()

class TestInitCommand:
    def test_fail_if_not_under_home(self, tmp_path, monkeypatch) -> None:
        """init() should raise an error if the current directory is not under the user's home directory"""
        monkeypatch.setattr(Path, "home", lambda: str(tmp_path)) # Set the home directory to tmp_path
        monkeypatch.chdir(tmp_path.parent) # Move to the parent of tmp_path, which is not under the "home" directory

        with pytest.raises(RuntimeError, match="cluv init should be run in a directory under your home directory."):
            init()

    
    def test_generate_default_toml_config(self, tmp_path, monkeypatch) -> None:
        """init() should create a pyproject.toml file with the default configuration if it doesn't exist"""
        monkeypatch.setattr(Path, "home", lambda: str(tmp_path)) # Set the home directory to tmp_path to pass the home check
        monkeypatch.chdir(tmp_path)

        init()
        config = get_config()

        assert config.clusters == ["mila"] + DRAC_CLUSTERS
        assert config.results_path == "logs"
        assert config.slurm == {'UV_OFFLINE': 1, 'WANDB_MODE': 'offline'}
        assert config.cluster_configs == {"mila": {"UV_OFFLINE": 0, "WANDB_MODE": "online"}}


    @pytest.mark.usefixtures("clean_config_cache")
    def test_keep_toml_config(self, tmp_path, monkeypatch) -> None:
        """init() should keep the cluv config of an already existing pyproject.toml"""
        monkeypatch.setattr(Path, "home", lambda: str(tmp_path)) # Set the home directory to tmp_path to pass the home check
        monkeypatch.setattr(cluv.cli.init, "check_git", lambda: None) # Skip git check
        monkeypatch.chdir(tmp_path)
        write_pyproject(tmp_path, """
[tool.cluv]
clusters = ["mila"]
results_path = "results"
""")

        init()
        config = get_config()

        assert config.results_path == "results"
        assert config.clusters == ["mila"]


    @pytest.mark.usefixtures("clean_config_cache")
    def test_results_path_as_none(self, tmp_path, monkeypatch) -> None:
        """init() should skip the job script and symlink creation if the results_path is set to None in the config"""
        monkeypatch.setattr(Path, "home", lambda: str(tmp_path)) # Set the home directory to tmp_path to pass the home check
        monkeypatch.setattr(cluv.cli.init, "check_git", lambda: None) # Skip git check
        monkeypatch.chdir(tmp_path)

        write_pyproject(tmp_path, """
[tool.cluv]
clusters = ["mila"]
""")

        init()
        config = get_config()

        assert config.results_path is None
        assert not (tmp_path / JOB_SCRIPT_PATH).exists()
        assert not (tmp_path / DEFAULT_RESULTS_PATH).exists()


class TestGitCheck:
    def test_fail_if_not_in_git_repo(self, tmp_path, monkeypatch) -> None:
        """check_git() should raise an error if the current directory is not a git repository"""
        monkeypatch.chdir(tmp_path) # No git project in tmp_path

        with pytest.raises(RuntimeError, match="Error when checking git remote: "):
            check_git()

