"""The template of the `[tool.cluv]` section that `cluv init` writes, and helpers to write the lines
of that section.

The checks of the clusters (`cluster_checks.py`) use these helpers too. `cluv/cli/init.py` imports
the checks, so the checks can't import these helpers from `cluv/cli/init.py`.
"""

import json
import re
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
# Repository root when running cluv from a source checkout.
REPO_ROOT = Path(__file__).resolve().parents[3]
MILA_CLUSTER = "mila"


def load_cluv_config_template() -> str:
    """
    Load the [tool.cluv] section from the pyproject.toml template file.
    """
    pyproject_template_path = _get_pyproject_template_path()
    pyproject_lines = pyproject_template_path.read_text().splitlines()
    start = next(
        (line_index for line_index, line in enumerate(pyproject_lines) if line == "[tool.cluv]"),
        None,
    )
    if start is None:
        raise RuntimeError(
            f"Template file {pyproject_template_path} is missing required [tool.cluv] section."
        )
    end = next(
        (
            line_index
            for line_index, line in enumerate(pyproject_lines[start + 1 :], start=start + 1)
            if line.startswith("[") and not line.startswith("[tool.cluv")
        ),
        len(pyproject_lines),
    )
    return "\n".join(pyproject_lines[start:end]).strip() + "\n"


def split_sections(config: str) -> list[list[str]]:
    """Splits the lines of a TOML text in sections, each starting with a `[table]` line."""
    sections: list[list[str]] = []
    for line in config.splitlines():
        if line.startswith("[") or not sections:
            sections.append([])
        sections[-1].append(line)
    return sections


def get_cluster_name(section: list[str]) -> str | None:
    """The cluster of a `[tool.cluv.clusters.<cluster>]` section, or None for another section."""
    match = re.fullmatch(r"\[tool\.cluv\.clusters\.([\w-]+)\]", section[0])
    return match.group(1) if match else None


def get_cluster_names(config: str) -> list[str]:
    """The clusters that have a section in the config, in order."""
    return [
        cluster for section in split_sections(config) if (cluster := get_cluster_name(section))
    ]


def get_sbatch_args_line(accounts: list[str]) -> str:
    """The `sbatch_args` line of a cluster with these Slurm accounts.

    >>> get_sbatch_args_line(["def-pi"])
    'sbatch_args = { account = "def-pi" }'
    >>> get_sbatch_args_line(["def-pi", "rrg-pi-ad"])
    'sbatch_args = [{ account = "def-pi" }, { account = "rrg-pi-ad" }]'
    """
    tables = [f"{{ account = {to_toml_string(account)} }}" for account in accounts]
    value = tables[0] if len(tables) == 1 else f"[{', '.join(tables)}]"
    return f"sbatch_args = {value}"


def to_toml_string(value: str) -> str:
    """Quotes the value as a TOML string. (A JSON string is also a valid TOML string.)"""
    return json.dumps(value, ensure_ascii=False)


def get_home_project_dir(project_root: Path) -> str:
    """The path of the project, with `$HOME` instead of the home directory.

    That's where the project goes on the clusters when `project_dir` isn't set. A project outside
    of the home directory keeps its absolute path.
    """
    try:
        return f"$HOME/{project_root.relative_to(Path.home()).as_posix()}"
    except ValueError:
        return str(project_root)


def _get_pyproject_template_path() -> Path:
    """
    Get the path to the pyproject.toml template file for cluv init, either from the source
    repository or from the installed package.
    """
    checked_paths = [REPO_ROOT / "pyproject.toml", PACKAGE_ROOT / "templates" / "pyproject.toml"]
    for pyproject_template_path in checked_paths:
        if pyproject_template_path.exists():
            return pyproject_template_path
    checked_paths_text = ", ".join(str(path) for path in checked_paths)
    raise RuntimeError(
        f"Couldn't find pyproject.toml template for cluv init. Checked: {checked_paths_text}."
    )
