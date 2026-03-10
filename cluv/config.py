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


def load_cluv_config(pyproject_path: Path) -> CluvConfig:
    with pyproject_path.open("rb") as handle:
        data = tomllib.load(handle)

    tool_config = data.get("tool", {})

    if isinstance(tool_config, dict) and (cluv_config := tool_config.get("cluv", {})):
        return CluvConfig(**cluv_config)
    logger.warning(
        UserWarning(
            f"[red]No [tool.cluv] section found in {pyproject_path}, using defaults.[/red]"
        )
    )
    return CluvConfig(clusters=[])


def get_cluster_choices() -> list[str]:
    """Return configured clusters or the defaults when config is missing/invalid."""
    return get_config().clusters
