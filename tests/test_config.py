"""Unit tests for cluv/config.py — pure, no I/O beyond tmp_path."""

from pathlib import Path

from cluv.config import load_cluv_config


def write_file(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pyproject.toml"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# clusters field — old list format (backward compat)
# ---------------------------------------------------------------------------


class TestClustersListFormat:
    def test_cluster_names(self, tmp_path: Path) -> None:
        p = write_file(tmp_path , """
[tool.cluv]
clusters = ["mila", "narval", "rorqual"]
""")
        cfg = load_cluv_config(p)
        assert cfg.clusters == ["mila", "narval", "rorqual"]

    def test_no_per_cluster_configs(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv]
clusters = ["mila"]
""")
        cfg = load_cluv_config(p)
        assert cfg.cluster_configs == {}

    def test_results_path_preserved(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv]
clusters = ["mila"]
results_path = "results"
""")
        cfg = load_cluv_config(p)
        assert cfg.results_path == "results"


# ---------------------------------------------------------------------------
# clusters field — new table format
# ---------------------------------------------------------------------------


class TestClustersTableFormat:
    def test_cluster_names_from_keys(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.clusters.mila]
[tool.cluv.clusters.rorqual]
SBATCH_ACCOUNT = "def-me"
""")
        cfg = load_cluv_config(p)
        assert set(cfg.clusters) == {"mila", "rorqual"}

    def test_empty_cluster_section_included(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.clusters.mila]
""")
        cfg = load_cluv_config(p)
        assert "mila" in cfg.clusters
        assert cfg.cluster_configs.get("mila", {}) == {}

    def test_per_cluster_sbatch_vars(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.clusters.rorqual]
SBATCH_ACCOUNT = "def-bengioy"
SBATCH_PARTITION = "main"
""")
        cfg = load_cluv_config(p)
        assert cfg.cluster_configs["rorqual"]["SBATCH_ACCOUNT"] == "def-bengioy"
        assert cfg.cluster_configs["rorqual"]["SBATCH_PARTITION"] == "main"

    def test_cluster_with_no_vars_not_in_cluster_configs(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.clusters.mila]
[tool.cluv.clusters.rorqual]
SBATCH_ACCOUNT = "def-bengioy"
""")
        cfg = load_cluv_config(p)
        assert "mila" not in cfg.cluster_configs
        assert "rorqual" in cfg.cluster_configs


# ---------------------------------------------------------------------------
# [tool.cluv.slurm] — global SBATCH_* defaults
# ---------------------------------------------------------------------------


class TestGlobalSlurm:
    def test_global_slurm_vars_parsed(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.slurm]
SBATCH_TIME = "1:00:00"
SBATCH_GPUS = "1"

[tool.cluv.clusters.mila]
""")
        cfg = load_cluv_config(p)
        assert cfg.slurm["SBATCH_TIME"] == "1:00:00"
        assert cfg.slurm["SBATCH_GPUS"] == "1"

    def test_missing_slurm_section_defaults_to_empty(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.clusters.mila]
""")
        cfg = load_cluv_config(p)
        assert cfg.slurm == {}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_cluv_section_returns_empty_clusters(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[project]
name = "foo"
""")
        cfg = load_cluv_config(p)
        assert cfg.clusters == []

    def test_full_config_round_trip(self, tmp_path: Path) -> None:
        p = write_file(tmp_path, """
[tool.cluv.slurm]
SBATCH_TIME = "3:00:00"

[tool.cluv.clusters.mila]
SBATCH_PARTITION = "long"

[tool.cluv.clusters.rorqual]
SBATCH_ACCOUNT = "def-bengioy"
SBATCH_PARTITION = "main"
""")
        cfg = load_cluv_config(p)
        assert set(cfg.clusters) == {"mila", "rorqual"}
        assert cfg.slurm == {"SBATCH_TIME": "3:00:00"}
        assert cfg.cluster_configs["mila"] == {"SBATCH_PARTITION": "long"}
        assert cfg.cluster_configs["rorqual"] == {
            "SBATCH_ACCOUNT": "def-bengioy",
            "SBATCH_PARTITION": "main",
        }