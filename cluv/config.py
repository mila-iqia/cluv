"""Lightweight config system based on a section in the pyproject.toml file."""

from __future__ import annotations

import functools
import logging
import tomllib
from dataclasses import field
from pathlib import Path

from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from cluv.utils import current_cluster, resolve_env_vars

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartialClusterConfig:
    """Per-cluster configuration options."""

    env: dict[str, str] = field(default_factory=dict)
    """Environment variables to set when running Slurm commands on this cluster."""

    results_path: str | None = None  # TODO: Change to `Path` instead. Fix any pydantic errors.
    """Path to the results directory for a specific cluster."""

    datasets_path: str | None = None  # TODO: Change to `Path` instead. Fix any pydantic errors.
    """Different path where the datasets should be replicated on this cluster.

    When `None`, this defaults to the top-level config's `datasets_path`.

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """


@dataclass(frozen=True)
class ClusterConfig:
    """Per-cluster configuration options."""

    env: dict[str, str]
    """Environment variables to set when running Slurm commands on this cluster."""

    results_path: Path
    """Path to the results directory for a specific cluster."""

    datasets_path: Path
    """Different path where the datasets should be replicated on this cluster.

    When `None`, this defaults to the top-level config's `datasets_path`.

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    def resolve_env_vars_in_paths(self):
        return ClusterConfig(
            env=self.env,
            results_path=resolve_env_vars(self.results_path),
            datasets_path=resolve_env_vars(self.datasets_path),
        )


class CluvConfig(BaseModel):
    """Configuration options for Cluv, loaded from the pyproject.toml file."""

    env: dict[str, str] = {}
    """Global environment variables set on all clusters when running Slurm commands."""

    results_path: str
    """Default path to the results directory for all clusters.

    !!! info
        On Slurm clusters, this will be a symlink to a folder in `$SCRATCH/<results_path>/<project_name>`.
    """

    datasets_path: str | None
    """Path to a dataset directory, for example, `'$SCRATCH/my_dataset'`

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    data_source: str | None
    """`hostname:/path` of where to get the data from."""

    clusters: dict[str, PartialClusterConfig] = {}
    """Configuration options for each cluster.

    The keys are cluster names; each value is a `ClusterConfig` whose `env` dict contains
    environment variables to set when running Slurm commands on that cluster.
    """

    @property
    def clusters_names(self) -> list[str]:
        return list(self.clusters.keys())

    def get_cluster_config(self, cluster: str) -> ClusterConfig:
        """Returns the cluster config for a specific cluster.

        The environment variables as part of paths will *not* be resolved.
        """
        cluv_config = load_cluv_config(find_pyproject())
        cluster_config = cluv_config.clusters[cluster]
        return ClusterConfig(
            env=cluv_config.env | cluster_config.env,
            results_path=Path(cluster_config.results_path or cluv_config.results_path),
            datasets_path=Path(cluster_config.datasets_path or cluv_config.datasets_path),
        )


@functools.cache
def get_config() -> CluvConfig:
    """Get the cluv config, loading it from the pyproject.toml if needed."""
    return load_cluv_config(find_pyproject())


def find_pyproject(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for folder in (current, *current.parents):
        candidate = folder / "pyproject.toml"
        if candidate.is_file():
            return candidate
    raise RuntimeError(
        f"Could not find pyproject.toml starting from {current}!\n"
        f"Cluv can only be used within a project managed with uv."
    )


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


def current_cluster_config() -> ClusterConfig | None:
    """Returns the `ClusterConfig` of the current cluster, or None if not currently on a cluster."""
    cluster = current_cluster()
    if not cluster:
        return None  # not on a cluster.
    cluv_config = load_cluv_config(find_pyproject())
    config = cluv_config.get_cluster_config(cluster)
    return config.resolve_env_vars_in_paths()
