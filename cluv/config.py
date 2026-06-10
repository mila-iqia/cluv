"""Lightweight config system based on a section in the pyproject.toml file."""

from __future__ import annotations

import dataclasses
import functools
import logging
import os
import tomllib
from dataclasses import field
from pathlib import Path

from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from cluv.utils import current_cluster, find_pyproject

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

    job_script_path: str | None = None
    """Path to the job script to use by default on this cluster."""

    project_dir: str | None = None
    """Path where the project should be cloned on this cluster."""


@dataclass(frozen=True)
class ClusterConfig:
    """Per-cluster configuration options."""

    env: dict[str, str]
    """Environment variables to set when running Slurm commands on this cluster."""

    results_path: Path
    """Path to the results directory for a specific cluster."""

    datasets_path: Path | None
    """Different path where the datasets should be replicated on this cluster.

    When `None`, this defaults to the top-level config's `datasets_path`.

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    job_script_path: Path | None
    """Path to the job script to use by default on this cluster."""

    project_dir: Path | None
    """Path where the project should be cloned on this cluster."""

    def expandvars(self):
        return ClusterConfig(
            env=self.env,
            results_path=Path(os.path.expandvars(str(self.results_path))),
            datasets_path=(
                Path(os.path.expandvars(str(self.datasets_path))) if self.datasets_path else None
            ),
            job_script_path=(
                Path(os.path.expandvars(str(self.job_script_path)))
                if self.job_script_path
                else None
            ),
            project_dir=Path(os.path.expandvars(self.project_dir)) if self.project_dir else None,
        )


class CluvConfig(BaseModel):
    """Configuration options for Cluv, loaded from the pyproject.toml file."""

    env: dict[str, str] = {}
    """Global environment variables set on all clusters when running Slurm commands."""

    results_path: str
    """Default path to the results directory for all clusters (may contain env vars like $SCRATCH)."""

    results_symlink: str = "logs"
    """Name of the symlink created in the project directory pointing to `results_path`."""

    data_source: str | None = None
    """`hostname:/path` of where to get the data from."""

    datasets_path: str | None = None
    """Path to a dataset directory, for example, `'$SCRATCH/my_dataset'`

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    job_script_path: str | None = None
    """Default path to the job script to submit when one is not passed explicitly to `cluv submit`.

    This can be overridden for specific clusters in the `clusters` section, and can also be
    overridden on the fly by passing a different job script to `cluv submit`.
    """

    project_dir: str | None = None
    """Default path where the project should be cloned on clusters."""

    clusters: dict[str, PartialClusterConfig] = {}
    """Configuration options for each cluster.

    The keys are cluster names, and values are configs that override options for that cluster.
    """

    @property
    def clusters_names(self) -> list[str]:
        return list(self.clusters.keys())

    def get_cluster_config(self, cluster: str) -> ClusterConfig:
        """Returns the cluster config for a specific cluster.

        The environment variables as part of paths will *not* be resolved.
        """
        cluster_config = self.clusters.get(cluster)
        if cluster_config is None:
            raise KeyError(
                f"Cluster {cluster!r} is not configured. Available: {self.clusters_names}"
            )
        results_path = cluster_config.results_path or self.results_path
        datasets_path = cluster_config.datasets_path or self.datasets_path
        job_script_path = cluster_config.job_script_path or self.job_script_path
        project_dir = cluster_config.project_dir or self.project_dir
        return ClusterConfig(
            env=self.env | cluster_config.env,
            results_path=Path(results_path),
            datasets_path=Path(datasets_path) if datasets_path else None,
            project_dir=Path(project_dir) if project_dir else None,
            job_script_path=Path(job_script_path) if job_script_path else None,
        )


@functools.cache
def get_cluv_config() -> CluvConfig:
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

    return CluvConfig.model_validate(cluv, extra="forbid")


def get_cluster_choices() -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    return get_cluv_config().clusters_names


def current_cluster_config() -> ClusterConfig | None:
    """Returns the `ClusterConfig` of the current cluster, or None if not currently on a cluster."""
    cluster = current_cluster()
    if not cluster:
        return None  # not on a cluster.
    cluv_config = get_cluv_config()
    data_source = cluv_config.data_source
    config = cluv_config.get_cluster_config(cluster)
    if data_source:
        source_cluster, data_path = data_source.split(":", 1)
        if cluster == source_cluster:
            # use the dataset path from the data_source setting as the datasets_path.
            config = dataclasses.replace(config, datasets_path=data_path)
    return config.expandvars()
