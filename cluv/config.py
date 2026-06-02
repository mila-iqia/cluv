"""Lightweight config system based on a section in the pyproject.toml file."""

from __future__ import annotations

import functools
import logging
import tomllib
from pathlib import Path
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ClusterConfig(BaseModel):
    """Per-cluster configuration options."""

    env: dict[str, str] = {}
    """Environment variables to set when running Slurm commands on this cluster."""


class RetryConfig(BaseModel):
    """Opt-in OOM-aware resubmit policy.

    DSL strings in `on_oom` are parsed by `salvo.policy.parse` at first use, not at
    config load. Keeps `cluv.config` free of a pysalvo import and lets users see a
    clear error pointing at the bad step.
    """

    on_oom: list[str] = []
    """Ordered policy steps tried on `OUT_OF_MEMORY`. First applicable step wins.

    Example: `["bump_mem(1.5x, max=128G)", "fail"]`.
    """

    max_hops: int = 5
    """Hard ceiling on resubmits per top-level invocation."""


class EstimateConfig(BaseModel):
    """Opt-in memory estimator driven by past sacct observations.

    When enabled, `cluv submit` looks up the local history cache for the (script,
    git_commit, program_args) key and, if there are enough samples, overrides
    `SBATCH_MEM` with `estimate_mem(...).mem_mb`. The retry loop still runs as a
    safety net if the estimate underpredicts.
    """

    enabled: bool = False
    """Master switch. When false the estimator is bypassed and submit is unchanged."""

    safety: float = 1.2
    """Multiplier applied to the P95 of past MaxRSS observations."""

    window: int = 20
    """Maximum number of recent records considered per submit."""

    min_samples: int = 3
    """Minimum learnable observations before the estimator overrides SBATCH_MEM."""

    backfill: bool = True
    """On a cold-cache key, run a single `sacct` query to backfill from cluster history."""


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

    retry: RetryConfig | None = None
    """Optional OOM-aware resubmit policy. When absent, `cluv submit` is unchanged."""

    estimate: EstimateConfig | None = None
    """Optional memory estimator. When absent or disabled, `cluv submit` is unchanged."""

    @property
    def clusters_names(self) -> list[str]:
        return list(self.clusters.keys())


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
