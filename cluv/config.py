"""Lightweight config system based on a section in the pyproject.toml file.

Should at the very least contain:
- a list of available clusters (hostnames).

Ideally, should also contain:
- A set of overrides for each cluster (which partition, gpu, account, etc to use).

Stretch goal (might be useful):
- Documentation links for each cluster (for LLMs to look at?)
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import tomllib
import warnings

DEFAULT_CLUSTERS: tuple[str, ...] = ("mila", "narval", "tamia", "all")


def find_pyproject(start: Path | None = None) -> Path | None:
    current = (start or Path.cwd()).resolve()
    for folder in (current, *current.parents):
        candidate = folder / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None


def _clusters_from_value(value: object) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("cluv clusters must be a list.")
    if not value:
        raise ValueError("cluv clusters must contain at least one entry.")
    clusters: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("cluv clusters must contain only strings.")
        cluster = item.strip()
        if not cluster:
            raise ValueError("cluv clusters must not contain blank entries.")
        clusters.append(cluster)
    return clusters


def load_cluv_config(pyproject_path: Path | None = None) -> dict[str, object]:
    if pyproject_path is None:
        return {}
    path = pyproject_path
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        warnings.warn(f"Unable to read {path}: {exc}")
        return {}
    tool_config = data.get("tool", {})
    if isinstance(tool_config, dict):
        cluv_config = tool_config.get("cluv", {})
        if isinstance(cluv_config, dict):
            return cluv_config
    return {}


@lru_cache(maxsize=None)
def _get_cluster_choices_cached(pyproject_path: Path | None = None) -> tuple[str, ...]:
    config_path = pyproject_path or find_pyproject()
    if config_path is None:
        return DEFAULT_CLUSTERS
    raw_clusters = load_cluv_config(config_path).get("clusters")
    if raw_clusters is None:
        return DEFAULT_CLUSTERS
    try:
        return tuple(_clusters_from_value(raw_clusters))
    except ValueError as exc:
        location = f" in {config_path}"
        warnings.warn(f"Invalid [tool.cluv].clusters{location}: {exc}")
    return DEFAULT_CLUSTERS


def get_cluster_choices(pyproject_path: Path | None = None) -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    return list(_get_cluster_choices_cached(pyproject_path))


def get_default_cluster(cluster_choices: list[str] | None = None) -> str:
    """Return the default cluster, validating provided cluster choices."""
    choices = get_cluster_choices() if cluster_choices is None else cluster_choices
    if not choices:
        raise ValueError("Cluster choices must contain at least one cluster name.")
    return "all" if "all" in choices else choices[0]


def get_cluster_config(pyproject_path: Path | None = None) -> tuple[list[str], str]:
    cluster_choices = get_cluster_choices(pyproject_path)
    return cluster_choices, get_default_cluster(cluster_choices)
