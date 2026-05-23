"""Unit tests for cluv/cli/build.py — pure, no I/O beyond string generation."""

from pathlib import Path

import pytest

from cluv.cli.build import generate_def
from cluv.config import ContainerConfig, PartialClusterConfig, load_cluv_config


class TestGenerateDef:
    def test_minimal(self):
        result = generate_def("python:3.12-slim", [], "")
        assert "Bootstrap: docker" in result
        assert "From: python:3.12-slim" in result
        assert "pip install --no-cache-dir -r /build/requirements.txt" in result
        assert "apt-get" not in result

    def test_with_apt_packages(self):
        result = generate_def("python:3.12-slim", ["gcc", "libgomp1"], "")
        assert "apt-get update" in result
        assert "gcc libgomp1" in result

    def test_with_extra_pip_args(self):
        result = generate_def(
            "python:3.12-slim", [],
            "--extra-index-url https://download.pytorch.org/whl/cu126",
        )
        assert "--extra-index-url https://download.pytorch.org/whl/cu126" in result

    def test_custom_base_image(self):
        result = generate_def("nvidia/cuda:12.6.0-runtime-ubuntu22.04", [], "")
        assert "From: nvidia/cuda:12.6.0-runtime-ubuntu22.04" in result

    def test_all_options(self):
        result = generate_def(
            "python:3.12-slim",
            ["gcc", "libc6-dev"],
            "--extra-index-url https://download.pytorch.org/whl/cu126",
        )
        assert "apt-get" in result
        assert "gcc libc6-dev" in result
        assert "--extra-index-url" in result
        assert "%test" in result


class TestContainerConfig:
    def test_defaults(self):
        cfg = ContainerConfig(deploy_path="/project/acct/containers")
        assert cfg.base_image == "python:3.12-slim"
        assert cfg.extra_apt == []
        assert cfg.extra_pip_args == ""

    def test_custom_values(self):
        cfg = ContainerConfig(
            deploy_path="/project/acct/containers",
            base_image="nvidia/cuda:12.6.0-runtime-ubuntu22.04",
            extra_apt=["gcc", "libgomp1"],
            extra_pip_args="--extra-index-url https://download.pytorch.org/whl/cu126",
        )
        assert cfg.deploy_path == "/project/acct/containers"
        assert cfg.base_image == "nvidia/cuda:12.6.0-runtime-ubuntu22.04"
        assert cfg.extra_apt == ["gcc", "libgomp1"]

    def test_cluster_without_container(self):
        cfg = PartialClusterConfig(env={"SBATCH_ACCOUNT": "def-me"})
        assert cfg.container is None

    def test_relative_deploy_path_rejected(self):
        with pytest.raises(ValueError, match="absolute path"):
            ContainerConfig(deploy_path="relative/path")


class TestContainerConfigFromToml:
    def test_parse_container_config(self, tmp_path: Path):
        p = tmp_path / "pyproject.toml"
        p.write_text("""\
[tool.cluv]
results_path = "logs"

[tool.cluv.clusters.mila]

[tool.cluv.clusters.rorqual.env]
SBATCH_ACCOUNT = "def-bengioy"

[tool.cluv.clusters.rorqual.container]
deploy_path = "/project/acct/containers"
base_image = "python:3.12-slim"
extra_apt = ["gcc", "libgomp1"]
extra_pip_args = "--extra-index-url https://download.pytorch.org/whl/cu126"
""")
        cfg = load_cluv_config(p)
        assert cfg.clusters["mila"].container is None
        container = cfg.clusters["rorqual"].container
        assert container is not None
        assert container.deploy_path == "/project/acct/containers"
        assert container.extra_apt == ["gcc", "libgomp1"]

    def test_minimal_container_config(self, tmp_path: Path):
        p = tmp_path / "pyproject.toml"
        p.write_text("""\
[tool.cluv]
results_path = "logs"

[tool.cluv.clusters.narval.container]
deploy_path = "/project/acct/containers"
""")
        cfg = load_cluv_config(p)
        container = cfg.clusters["narval"].container
        assert container is not None
        assert container.deploy_path == "/project/acct/containers"
        assert container.base_image == "python:3.12-slim"
        assert container.extra_apt == []
