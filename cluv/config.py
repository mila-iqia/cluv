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
class CluvConfig:
    clusters: list[str]
    results_path: str | None = None
    slurm: dict[str, str] = dataclasses.field(default_factory=dict)
    cluster_configs: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)


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
    with pyproject_path.open("rb") as handle:
        data = tomllib.load(handle)
    return "cluv" in data.get("tool", {}) and "results_path" in data.get("tool", {}).get("cluv", {})


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
        return CluvConfig(clusters=[])

    # clusters: list (backward compat) or table (new format with per-cluster settings)
    clusters_section = cluv.get("clusters", {})
    if isinstance(clusters_section, list):
        clusters = clusters_section
        cluster_configs: dict[str, dict[str, str]] = {}
    else:
        clusters = list(clusters_section.keys())
        cluster_configs = {k: dict(v) for k, v in clusters_section.items() if v}

    slurm: dict[str, str] = cluv.get("slurm", {})

    return CluvConfig(
        clusters=clusters,
        results_path=cluv.get("results_path"),
        slurm=slurm,
        cluster_configs=cluster_configs,
    )


def get_cluster_choices() -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    return get_config().clusters
