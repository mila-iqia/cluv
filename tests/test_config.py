"""Unit tests for cluv/config.py — pure, no I/O beyond tmp_path."""

from pathlib import Path

import pytest
from milatools.cli.init_command import DRAC_CLUSTERS

from cluv.config import ClusterConfig, PartialClusterConfig, get_cluv_config, load_cluv_config


def write_pyproject(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pyproject.toml"
    p.write_text(content)
    return p


@pytest.fixture(autouse=True)
def clear_get_cluv_config_cache() -> None:
    get_cluv_config.cache_clear()


# ---------------------------------------------------------------------------
# clusters field — table format
# ---------------------------------------------------------------------------


class TestClustersTableFormat:
    def test_cluster_names_from_keys(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.mila]
[tool.cluv.clusters.rorqual.env]
SBATCH_ACCOUNT = "def-me"
""",
        )
        cfg = load_cluv_config(p)
        assert set(cfg.clusters) == {"mila", "rorqual"}

    def test_empty_cluster_section_included(self, tmp_path):
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.mila]
""",
        )
        cfg = load_cluv_config(p)
        assert "mila" in cfg.clusters
        assert isinstance(cfg.clusters["mila"], PartialClusterConfig)
        assert cfg.clusters["mila"].env == {}
        assert isinstance(cfg.get_cluster_config("mila"), ClusterConfig)

    def test_per_cluster_sbatch_vars_new_format(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.rorqual.env]
SBATCH_ACCOUNT = "def-bengioy"
SBATCH_PARTITION = "main"
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.clusters["rorqual"].env["SBATCH_ACCOUNT"] == "def-bengioy"
        assert cfg.clusters["rorqual"].env["SBATCH_PARTITION"] == "main"

    def test_cluster_with_no_vars(self, tmp_path):
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.mila]
[tool.cluv.clusters.rorqual.env]
SBATCH_ACCOUNT = "def-bengioy"
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.clusters["mila"].env == {}
        assert cfg.clusters["rorqual"].env == {"SBATCH_ACCOUNT": "def-bengioy"}

    def test_clusters_names_should_not_returned_ignored_clusters(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
    [tool.cluv]
    results_path = "logs"
    [tool.cluv.clusters.mila]
    [tool.cluv.clusters.rorqual]
    [tool.cluv.clusters.narval]
    ignore = true
    """,
        )
        cfg = load_cluv_config(p)
        assert cfg.clusters_names == ["mila", "rorqual"]


# ---------------------------------------------------------------------------
# [tool.cluv.env] — global SBATCH_* defaults
# ---------------------------------------------------------------------------


class TestGlobalEnv:
    def test_global_env_vars_parsed(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.env]
SBATCH_TIME = "1:00:00"
SBATCH_GPUS = "1"

[tool.cluv.clusters.mila]
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.env["SBATCH_TIME"] == "1:00:00"
        assert cfg.env["SBATCH_GPUS"] == "1"

    def test_missing_env_section_defaults_to_empty(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.mila]
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.env == {}

    def test_job_script_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.mila]
job_script_path = "scripts/job.sh"
""",
        )
        monkeypatch.chdir(tmp_path)
        cfg = load_cluv_config(p)
        assert cfg.job_script_path is None
        assert cfg.get_cluster_config("mila").job_script_path == Path("scripts/job.sh")

    def test_cluster_job_script_path_overrides_global_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
job_script_path = "scripts/job.sh"

[tool.cluv.clusters.mila]
job_script_path = "scripts/mila_job.sh"

[tool.cluv.clusters.rorqual]
""",
        )
        monkeypatch.chdir(tmp_path)
        cfg = load_cluv_config(p)
        assert cfg.job_script_path == "scripts/job.sh"
        assert cfg.get_cluster_config("mila").job_script_path == Path("scripts/mila_job.sh")
        assert cfg.get_cluster_config("rorqual").job_script_path == Path("scripts/job.sh")


# ---------------------------------------------------------------------------
# ClusterConfig helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# sbatch_args
# ---------------------------------------------------------------------------


class TestSbatchArgs:
    def test_global_sbatch_args_parsed(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"

[tool.cluv.sbatch_args]
time = "2:00:00"
gpus = "1"
exclusive = true

[tool.cluv.clusters.mila]
""",
        )
        cfg = load_cluv_config(p)
        expected = {
            "time": "2:00:00",
            "gpus": "1",
            "exclusive": True,
        }
        assert cfg.sbatch_args == expected
        assert cfg.get_cluster_config("mila").sbatch_args == expected

    def test_per_cluster_sbatch_args_override_global(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"

[tool.cluv.sbatch_args]
gpus = "1"
time = "2:00:00"

[tool.cluv.clusters.mila]
[tool.cluv.clusters.mila.sbatch_args]
gpus = "a100:2"

[tool.cluv.clusters.cpu_cluster]
[tool.cluv.clusters.cpu_cluster.sbatch_args]
gpus = ""
""",
        )
        cfg = load_cluv_config(p)
        # Check that per-cluster sbatch_args are stored correctly
        assert cfg.get_cluster_config("mila").sbatch_args == {"time": "2:00:00", "gpus": "a100:2"}
        assert cfg.get_cluster_config("cpu_cluster").sbatch_args == {"time": "2:00:00", "gpus": ""}
        # Global defaults are preserved in the global config
        assert cfg.sbatch_args == {"gpus": "1", "time": "2:00:00"}

    def test_missing_sbatch_args_defaults_to_empty(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.clusters.mila]
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.sbatch_args == {}
        assert cfg.get_cluster_config("mila").sbatch_args == {}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_cluv_section_raises_error(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[project]
name = "foo"
""",
        )
        with pytest.raises(RuntimeError, match="No cluv config in"):
            load_cluv_config(p)

    def test_full_config_round_trip(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
[tool.cluv.env]
SBATCH_TIME = "3:00:00"

[tool.cluv.clusters.mila.env]
SBATCH_PARTITION = "long"

[tool.cluv.clusters.rorqual.env]
SBATCH_ACCOUNT = "def-bengioy"
SBATCH_PARTITION = "main"
""",
        )
        cfg = load_cluv_config(p)
        assert set(cfg.clusters) == {"mila", "rorqual"}
        assert cfg.env == {"SBATCH_TIME": "3:00:00"}
        assert cfg.clusters["mila"].env == {"SBATCH_PARTITION": "long"}
        assert cfg.clusters["rorqual"].env == {
            "SBATCH_ACCOUNT": "def-bengioy",
            "SBATCH_PARTITION": "main",
        }

    def test_top_level_project_dir_used_for_cluster(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
project_dir = "$HOME/repos/cluv"

[tool.cluv.clusters.mila]
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.get_cluster_config("mila").project_dir == Path("$HOME/repos/cluv")

    def test_cluster_project_dir_overrides_top_level(self, tmp_path: Path) -> None:
        p = write_pyproject(
            tmp_path,
            """
[tool.cluv]
results_path = "logs"
project_dir = "$HOME/repos/cluv"

[tool.cluv.clusters.killarney]
project_dir = "$SCRATCH/cluv"
""",
        )
        cfg = load_cluv_config(p)
        assert cfg.get_cluster_config("killarney").project_dir == Path("$SCRATCH/cluv")


# ---------------------------------------------------------------------------
# Real project config
# ---------------------------------------------------------------------------


class TestRealProjectConfig:
    def test_loads_without_error(self, pytestconfig) -> None:
        root_dir = pytestconfig.rootpath
        cfg = load_cluv_config(root_dir / "pyproject.toml")
        assert cfg is not None
        assert set(cfg.clusters_names) == set(DRAC_CLUSTERS + ["mila"])
