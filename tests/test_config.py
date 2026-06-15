"""Unit tests for cluv/config.py — pure, no I/O beyond tmp_path."""

from pathlib import Path

import pytest
from milatools.cli.init_command import DRAC_CLUSTERS

from cluv.config import ClusterConfig, PartialClusterConfig, load_cluv_config


def write_pyproject(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pyproject.toml"
    p.write_text(content)
    return p


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


# ---------------------------------------------------------------------------
# Real project config
# ---------------------------------------------------------------------------


class TestRealProjectConfig:
    def test_loads_without_error(self, pytestconfig) -> None:
        root_dir = pytestconfig.rootpath
        cfg = load_cluv_config(root_dir / "pyproject.toml")
        assert cfg is not None
        assert set(cfg.clusters_names) == set(DRAC_CLUSTERS + ["mila"])
