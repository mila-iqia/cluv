import dataclasses
import json
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

import questionary

from cluv.config import find_pyproject, has_cluv_config, load_cluv_config
from cluv.ssh import get_ssh_hostnames
from cluv.utils import console

__all__ = ["init"]

SCRIPTS_DIR_PATH = "scripts"
DEFAULT_RESULTS_PATH = "$SCRATCH/logs/cluv"
# The template's `project_dir` on the clusters that can't run jobs from $HOME.
DEFAULT_SCRATCH_PROJECT_DIR = "$SCRATCH/repos/cluv"
DEFAULT_RESULTS_LINKNAME = "logs"
MILA_CLUSTER = "mila"
# The other clusters of the template, except Mila, are DRAC clusters.
PAICE_CLUSTERS = ("tamia", "killarney", "vulcan")
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
# Repository root when running cluv from a source checkout.
REPO_ROOT = Path(__file__).resolve().parents[2]


def init(path: Path | None = None, quiet: bool = False) -> None:
    """Initialize a new project with cluv.

    This does the following:

    * Runs `uv init --package --build-backend hatch --python 3.11` to initialize a new uv project
      in the current directory (if there isn't one already).
    * Adds a configuration for Cluv in the `[tool.cluv]` section of pyproject.toml, from the
      answers to a few questions. When `quiet` is True, or when stdin isn't a terminal, no question
      is asked and the default answers are used.
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

    # If it doesn't exist, add a cluv config section made from the answers to a few questions.
    check_cluv_config(pyproject_path, ask=not quiet and sys.stdin.isatty())
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
        "=>  Learn more on how to use cluv with the [bold]documentation[/bold] : https://mila-iqia.github.io/cluv/"
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
    """Run `uv init`, unless a pyproject.toml file already exists in the current directory."""
    if (Path.cwd() / "pyproject.toml").exists():
        console.print(
            "✅ uv: a project already exists (see pyproject.toml file). Skipping initialization.",
            style="green",
        )
        return

    command = ["uv", "init", "--package", "--build-backend", "hatch", "--python", "3.11"]
    console.log(" ".join(command))

    uv_init = subprocess.run(command, capture_output=True, text=True)
    if uv_init.returncode != 0:
        raise RuntimeError("Error occurred while initializing uv project: ", uv_init.stderr)
    console.print("✅ uv: project initialized.", style="green")


def check_cluv_config(pyproject_path: Path, ask: bool = False) -> None:
    """
    Check if the pyproject.toml file contains a cluv config.
    If not, add a config section made from the template and the answers to a few questions.
    When `ask` is False, the default answers are used.
    """
    if has_cluv_config(pyproject_path):
        console.print("✅ Project already have a cluv config in pyproject.toml.", style="green")
        return

    console.print(
        "No config found for [bold]cluv[/bold] in the pyproject.toml file. Adding config..."
    )
    template = _load_cluv_config_template()
    clusters = _cluster_names(template)
    project_root = pyproject_path.parent
    answers = default_answers(clusters, project_root.name)
    if ask:
        answers = ask_questions(clusters, answers)
    cluv_config = make_cluv_config(template, answers, _scratch_project_dir(project_root))

    console.print("Adding config for cluv tool :")
    add_cluv_config_section(pyproject_path, cluv_config)


@dataclasses.dataclass
class InitAnswers:
    """The answers to the questions of `cluv init`. An empty string or list means "not set"."""

    clusters: list[str]
    results_path: str
    # Only used when the Mila cluster isn't selected: the template has the accounts of Mila members.
    drac_accounts: list[str] = dataclasses.field(default_factory=list)
    paice_accounts: list[str] = dataclasses.field(default_factory=list)
    project_dir: str = ""
    data_source: str = ""
    datasets_path: str = ""


def default_answers(clusters: list[str], project_name: str) -> InitAnswers:
    """The answers used when no question is asked.

    The clusters are those with a `Host` entry in ~/.ssh/config, since cluv connects to a cluster
    with `ssh <cluster>`. When none of the clusters has an entry, all the clusters are kept.
    """
    ssh_hostnames = get_ssh_hostnames()
    return InitAnswers(
        clusters=[cluster for cluster in clusters if cluster in ssh_hostnames] or clusters,
        results_path=f"$SCRATCH/logs/{project_name}",
    )


def ask_questions(clusters: list[str], defaults: InitAnswers) -> InitAnswers:
    """Asks the questions of `cluv init` in the terminal, with `defaults` as default answers."""

    def ask_text(message: str, **kwargs) -> str:
        return questionary.text(message, **kwargs).unsafe_ask().strip()

    def ask_accounts(clusters_kind: str, example: str) -> list[str]:
        accounts = ask_text(
            f"Which Slurm accounts do you use on the {clusters_kind} clusters?",
            instruction=f"(e.g. {example}; separate accounts with commas; leave empty for none)",
        )
        return accounts.replace(",", " ").split()

    def required(text: str) -> bool | str:
        return bool(text.strip()) or "This value is required."

    answers = dataclasses.replace(defaults)
    answers.clusters = questionary.checkbox(
        "Which clusters do you want to use?",
        choices=[questionary.Choice(c, checked=c in defaults.clusters) for c in clusters],
        validate=lambda selected: bool(selected) or "Select at least one cluster.",
    ).unsafe_ask()
    if MILA_CLUSTER not in answers.clusters:
        if any(cluster not in PAICE_CLUSTERS for cluster in answers.clusters):
            answers.drac_accounts = ask_accounts("DRAC", "def-yourpi")
        if any(cluster in PAICE_CLUSTERS for cluster in answers.clusters):
            answers.paice_accounts = ask_accounts("PAICE", "aip-yourpi")
    answers.results_path = ask_text(
        "Where should the job results go on the clusters?",
        default=defaults.results_path,
        validate=required,
    )
    answers.project_dir = ask_text(
        "Where should the project go on the clusters?",
        instruction="(leave empty for the same path as on this machine, relative to $HOME)",
    )
    answers.data_source = ask_text(
        "Where are the datasets that cluv should copy to all the clusters?",
        instruction="(<cluster>:<path>, or a path on this machine; leave empty if there are none)",
    )
    if answers.data_source:
        answers.datasets_path = ask_text(
            "Where should the datasets go on the clusters?", validate=required
        )
    return answers


def make_cluv_config(template: str, answers: InitAnswers, scratch_project_dir: str) -> str:
    """Fills the template's `[tool.cluv]` section with the answers to the questions.

    `scratch_project_dir` replaces the template's `project_dir` on the clusters that can't run
    jobs from $HOME.
    """
    config = template.replace(f'"{DEFAULT_RESULTS_PATH}"', _toml_string(answers.results_path))
    config = config.replace(f'"{DEFAULT_SCRATCH_PROJECT_DIR}"', _toml_string(scratch_project_dir))
    for key in ("project_dir", "data_source", "datasets_path"):
        if value := getattr(answers, key):
            # Uncomment the template's line for that setting, with the answer as value.
            # A replacement string would have its backslashes interpreted by re.sub, so `line`
            # is returned by a function, which re.sub inserts as is.
            line = f"{key} = {_toml_string(value)}"
            config = re.sub(rf"^# {key} = .*$", lambda _: line, config, count=1, flags=re.M)

    # A `project_dir` set for one cluster overrides the global one. So the template's `project_dir`
    # of the clusters that can't run jobs from $HOME is only kept when the global one is in $HOME.
    project_in_home = not answers.project_dir or answers.project_dir.startswith(("$HOME", "~"))
    lines = []
    for section in _split_sections(config):
        cluster = _cluster_name(section)
        if cluster is None:
            lines += section
            continue
        if cluster not in answers.clusters:
            continue
        if not project_in_home:
            section = [line for line in section if not line.startswith("project_dir = ")]
        if MILA_CLUSTER not in answers.clusters:
            accounts = (
                answers.paice_accounts if cluster in PAICE_CLUSTERS else answers.drac_accounts
            )
            section = _set_accounts(section, accounts)
        lines += section
    return "\n".join(lines).rstrip() + "\n"


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
    elif symlink_path.exists():
        console.print(
            f"⚠️  Warning: {symlink_path} already exists and is not a symlink.", style="yellow"
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


def _split_sections(config: str) -> list[list[str]]:
    """Splits the lines of a TOML text in sections, each starting with a `[table]` line."""
    sections: list[list[str]] = []
    for line in config.splitlines():
        if line.startswith("[") or not sections:
            sections.append([])
        sections[-1].append(line)
    return sections


def _cluster_name(section: list[str]) -> str | None:
    """The cluster of a `[tool.cluv.clusters.<cluster>]` section, or None for another section."""
    match = re.fullmatch(r"\[tool\.cluv\.clusters\.([\w-]+)\]", section[0])
    return match.group(1) if match else None


def _cluster_names(config: str) -> list[str]:
    """The clusters that have a section in the config, in order."""
    return [cluster for section in _split_sections(config) if (cluster := _cluster_name(section))]


def _set_accounts(section: list[str], accounts: list[str]) -> list[str]:
    """Replaces the Slurm accounts in the section of a cluster.

    The template's `sbatch_args` of the clusters only set accounts, so these lines are replaced.
    """
    section = [line for line in section if not line.startswith("sbatch_args = ")]
    if accounts:
        tables = [f"{{ account = {_toml_string(account)} }}" for account in accounts]
        value = tables[0] if len(tables) == 1 else f"[{', '.join(tables)}]"
        section.insert(1, f"sbatch_args = {value}")
    return section


def _scratch_project_dir(project_root: Path) -> str:
    """Where to put the project on the clusters that can't run jobs from $HOME.

    That's `$SCRATCH/repos/<name of the git repo>`, followed by the path of the project in the git
    repo when the project is in a subfolder of the repo.
    """
    project_root = project_root.resolve()
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], cwd=project_root, capture_output=True, text=True
    ).stdout.strip()
    repo_root = Path(git_root) if git_root else project_root
    path_in_repo = project_root.relative_to(repo_root).as_posix()
    return str(PurePosixPath("$SCRATCH/repos", repo_root.name, path_in_repo))


def _toml_string(value: str) -> str:
    """Quotes the value as a TOML string. (A JSON string is also a valid TOML string.)"""
    return json.dumps(value, ensure_ascii=False)


def _get_script_templates_path() -> Path:
    """
    Get the path to the folder with the job script templates for cluv init, either from the source
    repository or from the installed package.
    """
    checked_paths = [REPO_ROOT / "scripts", PACKAGE_ROOT / "templates" / "scripts"]
    for script_templates_path in checked_paths:
        if script_templates_path.exists():
            return script_templates_path
    checked_paths_text = ", ".join(str(path) for path in checked_paths)
    raise RuntimeError(
        f"Couldn't find the script templates folder. Checked: {checked_paths_text}."
    )


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
