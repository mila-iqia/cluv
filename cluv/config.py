"""Lightweight config system based on a section in the pyproject.toml file."""

from __future__ import annotations

import functools
import logging
import tomllib
from pathlib import Path

from pydantic import BaseModel

from cluv.utils import find_pyproject

logger = logging.getLogger(__name__)


class ClusterConfig(BaseModel):
    """Per-cluster configuration options."""

    env: dict[str, str] = {}
    """Environment variables to set when running Slurm commands on this cluster."""


class CluvConfig(BaseModel):
    """Configuration options for Cluv, loaded from the pyproject.toml file."""

    results_path: str
    """Path to the results directory, relative to the project root.

    !!! info
        On Slurm clusters, this will be a symlink to a folder in `$SCRATCH/<results_path>/<project_name>`.
    """

    env: dict[str, str] = {}
    """Global environment variables set on all clusters when running Slurm commands."""

    clusters: dict[str, ClusterConfig] = {}
    """Configuration options for each cluster.

    The keys are cluster names; each value is a `ClusterConfig` whose `env` dict contains
    environment variables to set when running Slurm commands on that cluster.
    """

    @property
    def clusters_names(self) -> list[str]:
        return list(self.clusters.keys())


@functools.cache
def get_config() -> CluvConfig:
    """Get the cluv config, loading it from the pyproject.toml if needed."""
    return load_cluv_config(find_pyproject())


def has_cluv_config(pyproject_path: Path) -> bool:
    """Check if the pyproject.toml contains a cluv config"""
    with pyproject_path.open("rb") as handle:
        data = tomllib.load(handle)
    return "cluv" in data.get("tool", {})


def load_cluv_config(pyproject_path: Path) -> CluvConfig:
    with pyproject_path.open("rb") as handle:
        data = tomllib.load(handle)

    cluv = data.get("tool", {}).get("cluv", {})
    if not cluv:
        raise RuntimeError(f"No cluv config in {pyproject_path} file.")

    return CluvConfig.model_validate(cluv)


def get_cluster_choices() -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    return get_config().clusters_names
