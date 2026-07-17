import os
import re
import subprocess
from pathlib import Path

from cluv.config import find_pyproject, has_cluv_config, load_cluv_config
from cluv.ssh import get_ssh_hostnames
from cluv.utils import console

__all__ = ["init"]

SCRIPTS_DIR_PATH = "scripts"
DEFAULT_RESULTS_PATH = "$SCRATCH/logs/cluv"
DEFAULT_RESULTS_LINKNAME = "logs"
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
# Repository root when running cluv from a source checkout.
REPO_ROOT = Path(__file__).resolve().parents[2]


def init(path: Path | None = None) -> None:
    """Initialize a new project with cluv.

    This does the following:

    * Runs `uv init --package --build-backend hatch --python 3.13` to initialize a new uv project
      in the current directory (if there isn't one already).
    * Adds a default configuration for Cluv in the `[tool.cluv]` section of pyproject.toml.
    * Adds job script templates in the `scripts/` directory of the project.
    """
    console.print()
    console.rule("[bold cyan]cluv init[/bold cyan]")
    console.print()

    if path is not None:
        path.mkdir(parents=True, exist_ok=True)
        os.chdir(path)

    # Check if the current directory is under the home directory. If not, raise an error and exit.
    check_home_dir()

    # Try to run "uv init" to create a new project
    console.print()
    console.print("Initializing uv project: running [bold]uv init[/bold]...")
    run_uv_init()

    # Check status of the git repository
    check_git()

    # Read the pyproject.toml file and try to find a cluv config.
    console.print()
    console.print("Reading pyproject.toml...")
    pyproject_path = find_pyproject()

    # If it doesn't exist, add a cluv config section with the default settings and clusters.
    check_cluv_config(pyproject_path)
    config = load_cluv_config(pyproject_path)

    # Compare the cluster names in the config to the SSH hostnames.
    check_ssh_hostnames(config.clusters_names)

    # Check if project structure is correct
    console.print()
    console.print("Validating project structure...")

    # Check if the job script exists
    check_job_script(pyproject_path.parent, config.results_path)

    # Check if the results path is correctly symlinked to scratch
    check_symlink_to_scratch(pyproject_path.parent, config.results_path, config.results_symlink)

    # Show what the user can do next after the project setup
    console.print()
    console.print(":tada: Your cluv config is ready to go !")
    console.print()
    console.print("Next steps :")
    console.print(
        "=>  Learn more how to use cluv with user guides : https://mila-iqia.github.io/cluv/"
    )
    console.print(
        "=> [bold] cluv login [/bold] : open a SSH connections to all configured clusters."
    )
    console.print(
        "=> [bold] cluv sync [/bold]  : synchronize the project on all configured clusters."
    )
    console.print()


def check_home_dir() -> None:
    """
    Check if the current directory is under the home directory. If not, raise an error.
    """
    if Path.cwd().is_relative_to(Path.home()):
        console.print("✅ Current directory is under home directory.", style="green")
    else:
        console.print(
            "❌ cluv init should be run in a directory under your home directory.", style="red"
        )
        raise RuntimeError("cluv init should be run in a directory under your home directory.")


def run_uv_init() -> None:
    """
    Try to run the uv init command. If the command fails because a pyproject.toml file
    already exists,continue. Otherwise, raise an error.
    """
    command = ["uv", "init", "--package", "--build-backend", "hatch", "--python", "3.13"]
    console.log(" ".join(command))

    uv_init = subprocess.run(command, capture_output=True, text=True)

    # An expected error is that uv fails if a pyproject.toml file already exists
    if uv_init.returncode == 2:
        if uv_init.stderr.endswith("(`pyproject.toml` file exists)\n"):
            console.print(
                "✅ uv: a project already exists (see pyproject.toml file). Skipping initialization.",
                style="green",
            )
        else:
            raise RuntimeError("Error occurred while initializing uv project: ", uv_init.stderr)
    else:
        console.print("✅ uv: project initialized.", style="green")


def check_cluv_config(pyproject_path: Path) -> None:
    """
    Check if the pyproject.toml file contains a cluv config.
    If not, add a default config section with the default clusters and settings.
    """
    if has_cluv_config(pyproject_path):
        console.print("✅ Project already have a cluv config in pyproject.toml.", style="green")
        return

    console.print(
        "No config found for [bold]cluv[/bold] in the pyproject.toml file. Adding config..."
    )
    console.print("Adding config for cluv tool :")

    cluv_config_template = _load_cluv_config_template()
    cluv_config = _update_clug_config_template(cluv_config_template, pyproject_path.parent.name)
    add_cluv_config_section(pyproject_path, cluv_config)


def add_cluv_config_section(pyproject_path: Path, section_lines: str) -> None:
    """
    Write the given lines to the pyproject.toml file.
    """
    console.log("\n" + section_lines.replace("[", "\\["))
    with pyproject_path.open("a") as f:
        f.write("\n" + section_lines)


def check_git() -> None:
    """
    Check if the current project is in a git repository. If not, raise an error and exit.
    """
    git_remote = subprocess.run(["git", "remote"], capture_output=True, text=True)
    if git_remote.returncode == 0:
        if git_remote.stdout.strip() == "":
            console.print(
                "⚠️  Warning: No git remote found. You won't be able to use some features "
                "(like syncing or submitting jobs). Consider adding a remote repository to your git config.",
                style="yellow",
            )
        else:
            console.print(
                f"✅ Git remote repository found: {git_remote.stdout.strip()}", style="green"
            )
    else:
        console.print("❌ Invalid git repository found.", style="red")
        raise RuntimeError("Error when checking git remote: ", git_remote.stderr)


def check_symlink_to_scratch(project_path: Path, results_path: str, results_symlink: str) -> None:
    """
    Check if a symlink from the results_path in the project in $HOME to the corresponding path in
    $SCRATCH already exists. If not, create it.

    The symlink should be : <project_path>/<results_symlink> -> <results_path>
    """
    if "SCRATCH" not in os.environ:
        console.print(
            "⚠️  Warning: $SCRATCH variable not set. Skipping symlink creation.", style="yellow"
        )
        return

    # Generate the expected scratch and symlink path
    scratch_path = Path(os.path.expandvars(results_path))
    symlink_path = project_path / results_symlink

    if symlink_path.is_symlink():
        if symlink_path.resolve() == scratch_path.resolve():
            console.print(
                "✅ Symlink from $HOME results_path to $SCRATCH already exists.", style="green"
            )
            return
        else:
            console.print(
                f"⚠️  Warning: Symlink from {symlink_path} points to an other path "
                f"({symlink_path.resolve()}) than the expected scratch path.",
                style="yellow",
            )
            return
    else:
        console.print(f"Creating symlink from {symlink_path} to {scratch_path}")
        scratch_path.mkdir(parents=True, exist_ok=True)
        symlink_path.symlink_to(scratch_path, target_is_directory=True)


def check_ssh_hostnames(clusters: list[str]) -> None:
    """
    Check if the names of the clusters in the cluv config are present in the SSH config file.
    If not, print a warning.
    """
    ssh_hostnames = get_ssh_hostnames()
    missing_clusters = set(clusters).difference(ssh_hostnames)

    if len(missing_clusters) > 0:
        console.print(
            f"⚠️  Warning: Missing SSH config for {len(missing_clusters)} clusters. "
            "Try to run [bold]mila init[/bold] to add all available clusters.",
            style="yellow",
        )
        for cluster in missing_clusters:
            console.print(f"    - {cluster}", style="yellow")
    else:
        console.print(
            "✅ All clusters in the cluv config are present in your SSH config.", style="green"
        )


def check_job_script(project_root: Path, results_path: str) -> None:
    """
    Check if job script templates exist. If not, create them.
    The scripts are templates for users to submit jobs to Slurm with cluv.
    """
    try:
        project_root_relative_to_home = project_root.relative_to(Path.home())
        project_root_for_script = f"$HOME/{project_root_relative_to_home}"
    except ValueError:
        project_root_for_script = str(project_root)
    scripts_dir = project_root / SCRIPTS_DIR_PATH
    script_templates_path = _get_script_templates_path()
    script_templates = sorted(script_templates_path.glob("*.sh"))

    if not script_templates:
        console.print("⚠️  Warning: No script templates found.", style="yellow")
        return

    scripts_dir.mkdir(parents=True, exist_ok=True)
    for script_template in script_templates:
        script_path = scripts_dir / script_template.name
        if script_path.exists():
            console.print(
                f"✅ Job template script already exists at '{script_path}'.", style="green"
            )
            continue
        script_content = script_template.read_text()
        script_content = re.sub(
            r"^#SBATCH --output=.*$",
            f"#SBATCH --output={results_path}/%j/slurm-%j.out",
            script_content,
            flags=re.MULTILINE,
        )
        script_content = re.sub(
            r"^project_name=.*$",
            f'project_name="{project_root.name}"',
            script_content,
            flags=re.MULTILINE,
        )
        script_content = re.sub(
            r"^project_root=.*$",
            f'project_root="{project_root_for_script}"',
            script_content,
            flags=re.MULTILINE,
        )
        script_content = re.sub(
            r"^results_(?:dir|path)=.*$",
            f'results_path="{results_path}"',
            script_content,
            flags=re.MULTILINE,
        )
        script_content = re.sub(
            r"\$(?:\{results_dir\}|results_dir\b)", "$results_path", script_content
        )
        script_path.write_text(script_content)
        console.print(f"Adding job template script at '{script_path}'.")


def _load_cluv_config_template() -> str:
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


def _update_clug_config_template(template_config: str, project_name: str) -> str:
    return template_config.replace(DEFAULT_RESULTS_PATH, f"$SCRATCH/logs/{project_name}")


def _get_script_templates_path() -> Path:
    checked_paths = [REPO_ROOT / "scripts", PACKAGE_ROOT / "templates" / "scripts"]
    for script_templates_path in checked_paths:
        if script_templates_path.exists():
            return script_templates_path
    checked_paths_text = ", ".join(str(path) for path in checked_paths)
    raise RuntimeError(
        f"Couldn't find the script templates folder. Checked: {checked_paths_text}."
    )


def _get_pyproject_template_path() -> Path:
    checked_paths = [REPO_ROOT / "pyproject.toml", PACKAGE_ROOT / "templates" / "pyproject.toml"]
    for pyproject_template_path in checked_paths:
        if pyproject_template_path.exists():
            return pyproject_template_path
    checked_paths_text = ", ".join(str(path) for path in checked_paths)
    raise RuntimeError(
        f"Couldn't find pyproject.toml template for cluv init. Checked: {checked_paths_text}."
    )
