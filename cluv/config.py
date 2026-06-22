"""Lightweight config system based on a section in the pyproject.toml file."""

from __future__ import annotations

import dataclasses
import functools
import logging
import os
import tomllib
from dataclasses import field
from pathlib import Path, PurePath, PurePosixPath

import pydantic
from pydantic.dataclasses import dataclass

from cluv.utils import current_cluster, find_pyproject

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CommonFields:
    """Fields that are in the top-level config as well as the per-cluster overrides."""

    env: dict[str, str | int | float | bool]
    """Environment variables to set when running Slurm commands."""

    sbatch_args: dict[str, str | int | float | bool]
    """Arguments to pass to `sbatch`."""

    results_path: PurePath | None
    """Path to the results directory."""

    datasets_path: PurePath | None
    """Path where the datasets should be replicated.

    Files will be synced from the `data_source` of the top-level config to the `datasets_path` on
    all clusters.
    """

    job_script_path: PurePath | None
    """Path to the job script to use by default on this cluster."""

    project_dir: PurePath | None
    """Path where the project should be cloned on this cluster."""


@dataclass(frozen=True)
class ClusterConfigSchema(_CommonFields):
    """Pydantic schema for fields of `ClusterConfig` that are set in the config file for each cluster.

    This describes the values that can be set in the [tool.cluv.clusters.<cluster_name>]
    section of pyproject.toml.

    Only used for validation of the config file with Pydantic. Most of the fields here are optional.
    """

    env: dict[str, str | int | float | bool] = field(default_factory=dict)
    sbatch_args: dict[str, str | int | float | bool] = field(default_factory=dict)
    results_path: Path | None = None
    datasets_path: Path | None = None
    job_script_path: Path | None = None
    project_dir: Path | None = None

    ignore: bool = False
    """Whether to ignore this cluster when running commands on all clusters."""


@dataclass(frozen=True)
class ClusterConfig[PathType: Path | PurePosixPath = PurePosixPath](_CommonFields):
    """Per-cluster configuration options.

    The fields here are set using the overlap of the top-level config and the per-cluster overrides,
    with the per-cluster values taking precedence.

    Take a look at `CluvConfig.get_cluster_config` to see exactly how the fields are merged.

    Note: the path fields in this class are by default 'pure' posix paths, to make it explicit that
    they can't be used on the local filesystem, and to avoid errors like trying to call
    `.open`/`.mkdir`/etc. On the current cluster, when calling `current_cluster_config`, they are
    actual `Path`s.
    """

    env: dict[str, str | int | float | bool]
    sbatch_args: dict[str, str | int | float | bool]
    results_path: PathType
    project_dir: PathType
    datasets_path: PathType | None
    job_script_path: PathType | None
    ignore: bool


@dataclass(frozen=True, kw_only=True)
class CluvConfig(_CommonFields):
    """Configuration options for Cluv, loaded from the pyproject.toml file.

    The fields here are set using the [tool.cluv] section of the pyproject.toml file.

    The `clusters` field contains per-cluster overrides for any of the other fields, which are
    merged with the top-level values when calling `get_cluster_config`.
    """

    results_symlink: str = "logs"
    """Name of the symlink created in the project directory pointing to `results_path`."""

    data_source: str | None = None
    """`hostname:/path` of where to get the data from."""

    env: dict[str, str | int | float | bool] = field(default_factory=dict)
    """Global environment variables set on all clusters when running Slurm commands."""

    sbatch_args: dict[str, str | int | float | bool] = field(default_factory=dict)
    """Global sbatch flags applied on all clusters.

    These are passed directly to `sbatch` and complement `env` (which sets `SBATCH_*` env vars).
    See `[tool.cluv.clusters.<name>.sbatch_args]` for per-cluster overrides.
    """

    results_path: Path
    """Default path to the results directory for all clusters (may contain env vars like $SCRATCH)."""

    datasets_path: Path | None = None
    """Path to a dataset directory, for example, `'$SCRATCH/my_dataset'`

    This folder will be synced from the current cluster to all other clusters at their respective `dataset_path`.
    """

    job_script_path: Path | None = None
    """Default path to the job script to submit when one is not passed explicitly to `cluv submit`.

    This can be overridden for specific clusters in the `clusters` section, and can also be
    overridden on the fly by passing a different job script to `cluv submit`.
    """

    project_dir: Path | None = None
    """Default path where the project should be cloned on clusters."""

    clusters: dict[str, ClusterConfigSchema] = field(default_factory=dict)
    """Configuration options for each cluster.

    The keys are cluster names, and values are configs that override options for that cluster.
    """

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
        project_dir = (
            cluster_config.project_dir
            or self.project_dir
            or PurePosixPath(
                find_pyproject().parent.relative_to(Path.home())
                if find_pyproject().is_relative_to(Path.home())
                else find_pyproject().parent
            )
        )
        return ClusterConfig(
            env=self.env | cluster_config.env,
            sbatch_args=self.sbatch_args | cluster_config.sbatch_args,
            results_path=PurePosixPath(results_path),
            datasets_path=PurePosixPath(datasets_path) if datasets_path else None,
            project_dir=PurePosixPath(project_dir),
            job_script_path=PurePosixPath(job_script_path) if job_script_path else None,
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

    cluv = data.get("tool", {}).get("cluv", {})
    if not cluv:
        raise RuntimeError(f"No cluv config in {pyproject_path} file.")

    return pydantic.TypeAdapter(CluvConfig).validate_python(cluv, extra="forbid")


def current_cluster_config() -> ClusterConfig[Path] | None:
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
    return ClusterConfig(
        env=config.env,
        sbatch_args=config.sbatch_args,
        results_path=Path(os.path.expandvars(config.results_path)),
        datasets_path=(
            Path(os.path.expandvars(config.datasets_path)) if config.datasets_path else None
        ),
        project_dir=Path(os.path.expandvars(config.project_dir)),
        job_script_path=(
            Path(os.path.expandvars(config.job_script_path)) if config.job_script_path else None
        ),
        ignore=config.ignore,
    )
