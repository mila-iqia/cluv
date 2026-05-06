"""Unit tests for cluv/config.py — pure, no I/O beyond tmp_path."""

from pathlib import Path

from cluv.config import load_cluv_config
import pytest

def write_pyproject(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pyproject.toml"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# clusters field — new table format
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
        assert cfg.clusters.get("mila", None) is not None
        assert cfg.clusters["mila"].env == {}

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
