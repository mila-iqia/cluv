"""Lightweight config system based on a section in the pyproject.toml file."""

from __future__ import annotations

import dataclasses
import functools
import logging
import os
import tomllib
from dataclasses import field
from pathlib import Path, PurePath, PurePosixPath

from pydantic import BaseModel, ConfigDict
from pydantic.dataclasses import dataclass

from cluv.utils import current_cluster, find_pyproject

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartialClusterConfig:
    """Per-cluster configuration options."""

    env: dict[str, str] = field(default_factory=dict)
    """Environment variables to set when running Slurm commands on this cluster."""

    sbatch_args: dict[str, str | int | float | bool] = field(default_factory=dict)
    """Per-cluster sbatch flags, overriding the global `sbatch_args`."""

    results_path: str | None = None
    """Path to the results directory for a specific cluster."""

    datasets_path: str | None = None
    """Different path where the datasets should be replicated on this cluster.

    When `None`, this defaults to the top-level config's `datasets_path`.

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    job_script_path: str | None = None
    """Path to the job script to use by default on this cluster."""

    project_dir: str | None = None
    """Path where the project should be cloned on this cluster."""

    ignore: bool = False
    """Whether to ignore this cluster when running commands on all clusters."""


@dataclass(frozen=True)
class ClusterConfig[PathType: Path | PurePosixPath = PurePosixPath]:
    """Per-cluster configuration options.

    The path fields in this class are by default 'pure' posix paths, to make it explicit that they
    can't be used on the local filesystem, and to avoid errors like trying to call .open/.mkdir/etc.
    On the current cluster, when calling `current_cluster_config`, they are actual `Path`s.
    """

    env: dict[str, str]
    """Environment variables to set when running Slurm commands on this cluster."""

    sbatch_args: dict[str, str | int | float | bool]
    """Merged sbatch flags (global defaults overridden by per-cluster values)."""

    results_path: PathType
    """Path to the results directory for a specific cluster."""

    datasets_path: PathType | None
    """Different path where the datasets should be replicated on this cluster.

    When `None`, this defaults to the top-level config's `datasets_path`.

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    job_script_path: PathType | None
    """Path to the job script to use by default on this cluster."""

    project_dir: PathType | None
    """Path where the project should be cloned on this cluster."""

    ignore: bool
    """Whether to ignore this cluster when running commands on all clusters."""


@dataclass(frozen=True)
class LocalConfig:
    """Config for using cluv on a local machine (not on a Slurm cluster)."""

    env: dict[str, str] = field(default_factory=dict)
    """Environment variables to set when using cluv on a local machine (not on a Slurm cluster).
    For example, this can be used to set a fake "$SCRATCH" directory to use when not on a Slurm cluster.
    """


class CluvConfig(BaseModel):
    """Configuration options for Cluv, loaded from the pyproject.toml file."""

    # Pydantic Model configuration.
    model_config = ConfigDict(
        extra="forbid",  # throw an error if extra fields are provided
        validate_default=True,  # validate default values
        use_attribute_docstrings=True,  # for field descriptions
        revalidate_instances="always",
    )

    env: dict[str, str] = {}
    """Global environment variables set on all clusters when running Slurm commands."""

    sbatch_args: dict[str, str | int | float | bool] = {}
    """Global sbatch flags applied on all clusters.

    These are passed directly to `sbatch` and complement `env` (which sets `SBATCH_*` env vars).
    See `[tool.cluv.clusters.<name>.sbatch_args]` for per-cluster overrides.
    """

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

    local: LocalConfig = LocalConfig()

    @property
    def clusters_names(self) -> list[str]:
        return [name for name, config in self.clusters.items() if not config.ignore]

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
            sbatch_args=self.sbatch_args | cluster_config.sbatch_args,
            results_path=PurePosixPath(results_path),
            datasets_path=PurePosixPath(datasets_path) if datasets_path else None,
            job_script_path=PurePosixPath(job_script_path) if job_script_path else None,
            project_dir=PurePosixPath(project_dir) if project_dir else None,
            ignore=cluster_config.ignore,
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

    cluv: dict = data.get("tool", {}).get("cluv", {})
    if not cluv:
        raise RuntimeError(f"No cluv config in {pyproject_path} file.")

    if current_cluster() is None:
        for key, value in cluv.get("local", {}).get("env", {}).items():
            while "$" in value:
                value = os.path.expandvars(value)
            if key in os.environ:
                logger.warning(
                    "Not overwriting local env var %s=%s with value from [tool.cluv.local.env] %s",
                    key,
                    os.environ[key],
                    value,
                )
                continue
            logger.info("Setting local env var %s=%s from [tool.cluv.local.env]", key, value)
            os.environ[key] = value
    config = CluvConfig.model_validate(cluv, extra="forbid")
    return config


def current_cluster_config() -> ClusterConfig[Path] | None:
    """Returns the `ClusterConfig` of the current cluster, or None if not currently on a cluster."""
    cluster = current_cluster()
    if not cluster:
        return None  # not on a cluster.
    cluv_config = get_cluv_config()
    cluster_config = cluv_config.get_cluster_config(cluster)
    data_source = cluv_config.data_source
    if data_source:
        source_cluster, _, data_path = data_source.partition(":")
        if cluster == source_cluster:
            # use the dataset path from the data_source setting as the datasets_path.
            cluster_config = dataclasses.replace(cluster_config, datasets_path=Path(data_path))
    return dataclasses.replace(  # type: ignore
        cluster_config,
        **{
            f.name: Path(os.path.expandvars(v))
            for f in dataclasses.fields(cluster_config)
            if isinstance(v := getattr(cluster_config, f.name), PurePath)
        },
    )
