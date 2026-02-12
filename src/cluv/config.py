"""Lightweight config system based on a section in the pyproject.toml file.

Should at the very least contain:
- a list of available clusters (hostnames).

Ideally, should also contain:
- A set of overrides for each cluster (which partition, gpu, account, etc to use).

Stretch goal (might be useful):
- Documentation links for each cluster (for LLMs to look at?)
"""

from __future__ import annotations

from pathlib import Path
import tomllib

DEFAULT_CLUSTERS: tuple[str, ...] = ("mila", "narval", "tamia", "all")


def find_pyproject(start: Path | None = None) -> Path | None:
    current = (start or Path.cwd()).resolve()
    for folder in (current, *current.parents):
        candidate = folder / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None


def _clusters_from_value(value: object) -> list[str] | None:
    if isinstance(value, list) and value and all(isinstance(item, str) for item in value):
        return list(value)
    return None


def load_cluv_config(pyproject_path: Path | None = None) -> dict[str, object]:
    path = pyproject_path or find_pyproject()
    if path is None:
        return {}
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    tool_config = data.get("tool", {})
    if isinstance(tool_config, dict):
        cluv_config = tool_config.get("cluv", {})
        if isinstance(cluv_config, dict):
            return cluv_config
    return {}


def get_cluster_choices(pyproject_path: Path | None = None) -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    clusters = _clusters_from_value(load_cluv_config(pyproject_path).get("clusters"))
    if clusters is not None:
        return clusters
    return list(DEFAULT_CLUSTERS)


def get_default_cluster(
    cluster_choices: list[str] | None = None,
    pyproject_path: Path | None = None,
) -> str:
    choices = cluster_choices or get_cluster_choices(pyproject_path)
    return "all" if "all" in choices else choices[0]
