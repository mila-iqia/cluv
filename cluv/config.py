"""Lightweight config system based on a section in the pyproject.toml file.

Should at the very least contain:
- a list of available clusters (hostnames).

Ideally, should also contain:
- A set of overrides for each cluster (which partition, gpu, account, etc to use).

Stretch goal (might be useful):
- Documentation links for each cluster (for LLMs to look at?)
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import tomllib
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ClusterConfig:
    """Per-cluster configuration options."""

    env: dict[str, str] = dataclasses.field(default_factory=dict)
    """Environment variables to set when running Slurm commands on this cluster."""


@dataclasses.dataclass
class CluvConfig:
    """Configuration options for Cluv, loaded from the pyproject.toml file."""

    results_path: str | None = None
    """Path to the results directory, relative to the project root. If not set, defaults to "logs".

    !!! info
        On Slurm clusters, this will be a symlink to a folder in `$SCRATCH/<results_path>/<project_name>`.
    """

    env: dict[str, str] = dataclasses.field(default_factory=dict)
    """Global environment variables set on all clusters when running Slurm commands."""

    cluster_configs: dict[str, ClusterConfig] = dataclasses.field(default_factory=dict)
    """Configuration options for each cluster.

    The keys are cluster names; each value is a `ClusterConfig` whose `env` dict contains
    environment variables to set when running Slurm commands on that cluster.
    """

    @property
    def clusters(self) -> list[str]:
        return list(self.cluster_configs.keys())


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
        logger.warning(
            UserWarning(
                f"[red]No [tool.cluv] section found in {pyproject_path}, using defaults.[/red]"
            )
        )
        return CluvConfig()

    # clusters: list (backward compat) or table (new format with per-cluster settings)
    clusters_section = cluv.get("clusters", {})
    if isinstance(clusters_section, list):
        cluster_configs: dict[str, ClusterConfig] = {k: ClusterConfig() for k in clusters_section}
    else:
        cluster_configs = {}
        for name, raw in clusters_section.items():
            if "env" in raw:
                # New format: [tool.cluv.clusters.<name>.env]
                cluster_configs[name] = ClusterConfig(env=dict(raw["env"]))
            else:
                # Old flat format: env vars directly in [tool.cluv.clusters.<name>]
                cluster_configs[name] = ClusterConfig(env=dict(raw))

    # global env: new [tool.cluv.env] key, with backward compat for old [tool.cluv.slurm]
    env: dict[str, str] = cluv.get("env", cluv.get("slurm", {}))

    return CluvConfig(
        results_path=cluv.get("results_path"),
        env=env,
        cluster_configs=cluster_configs,
    )


def get_cluster_choices() -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    return get_config().clusters
