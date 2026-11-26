import asyncio
import dataclasses
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

import questionary

from cluv.cli.init_utils.cluster_checks import Status, check_clusters, has_ssh_entry
from cluv.cli.init_utils.template import (
    MILA_CLUSTER,
    PACKAGE_ROOT,
    REPO_ROOT,
    get_cluster_name,
    get_cluster_names,
    get_home_project_dir,
    load_cluv_config_template,
    get_sbatch_args_line,
    split_sections,
    to_toml_string,
)
from cluv.config import (
    CluvConfig,
    find_pyproject,
    has_cluv_config,
    load_cluv_config,
)
from cluv.utils import console, join_names

__all__ = ["init"]

SCRIPTS_DIR_PATH = "scripts"
DEFAULT_RESULTS_PATH = "$SCRATCH/logs/cluv"
# The template's `project_dir` on the clusters that can't run jobs from $HOME.
DEFAULT_SCRATCH_PROJECT_DIR = "$SCRATCH/repos/cluv"
DEFAULT_RESULTS_LINKNAME = "logs"
# The other clusters of the template, except Mila, are DRAC clusters.
PAICE_CLUSTERS = ("tamia", "killarney", "vulcan")


def init(path: Path | None = None, quiet: bool = False) -> None:
    """Initialize a new project with cluv.

    This does the following:

    * Runs `uv init --package --build-backend hatch --python 3.11` to initialize a new uv project
      in the current directory (if there isn't one already).
    * Adds a configuration for Cluv in the `[tool.cluv]` section of pyproject.toml, from the
      answers to a few questions. When `quiet` is True, or when stdin isn't a terminal, no question
      is asked and the default answers are used.
    * Adds job script templates in the `scripts/` directory of the project.
    * Checks each cluster of the config, and shows the results in a frame for each cluster, with
      how to fix each problem. Some checks run on the clusters: unless `quiet` is True or stdin
      isn't a terminal, `cluv init` offers to connect to the clusters that aren't connected yet.
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
    ask = not quiet and sys.stdin.isatty()
    check_cluv_config(pyproject_path, ask=ask)
    config = load_cluv_config(pyproject_path)

    # Check if project structure is correct
    console.print()
    console.print("Validating project structure...")

    # Check if the job script exists
    check_job_script(pyproject_path.parent, config.results_path)
    check_job_script_paths(pyproject_path.parent, config)

    # Check if the results path is correctly symlinked to scratch
    check_symlink_to_scratch(pyproject_path.parent, config.results_path, config.results_symlink)

    # Check the access to each cluster of the config.
    console.print()
    console.print("Checking the clusters...")
    reports = asyncio.run(check_clusters(config, pyproject_path.parent, ask=ask))

    # Show what the user can do next after the project setup
    console.print()
    if any(check.status is Status.FAIL for checks in reports.values() for check in checks):
        console.print(
            "Some clusters have problems: fix them (see above), then run [bold]cluv init[/bold] "
            "again to check them."
        )
    else:
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
    template = load_cluv_config_template()
    project_root = pyproject_path.parent
    scratch_project_dir = _scratch_project_dir(project_root)
    answers = default_answers(template, project_root.name)
    if ask:
        try:
            answers = ask_questions(
                template, answers, get_home_project_dir(project_root), scratch_project_dir
            )
        except (KeyboardInterrupt, EOFError):
            # Ctrl-C raises KeyboardInterrupt, and Ctrl-D raises EOFError.
            console.print("Cancelled: no cluv config was written.", style="yellow")
            sys.exit(1)
    cluv_config = make_cluv_config(template, answers, scratch_project_dir)

    console.print("Adding config for cluv tool :")
    add_cluv_config_section(pyproject_path, cluv_config)


@dataclasses.dataclass
class InitAnswers:
    """The answers to the questions of `cluv init`. An empty string or list means "not set"."""

    clusters: list[str]
    results_path: str
    # Whether the user has access to the Mila cluster. If so, the accounts of Mila members are
    # suggested on all the clusters.
    mila_access: bool = False
    # The Slurm accounts to use on each cluster.
    accounts: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    project_dir: str = ""
    data_source: str = ""
    datasets_path: str = ""


def default_answers(template: str, project_name: str) -> InitAnswers:
    """The answers used when no question is asked.

    The clusters are those with a `Host` entry in ~/.ssh/config, since cluv connects to a cluster
    with `ssh <cluster>`. When none of the clusters has an entry, all the clusters are kept. The
    user is assumed to have access to the Mila cluster when it is one of these clusters.
    """
    all_clusters = get_cluster_names(template)
    clusters = [cluster for cluster in all_clusters if has_ssh_entry(cluster)] or all_clusters
    mila_access = MILA_CLUSTER in clusters
    return InitAnswers(
        clusters=clusters,
        results_path=f"$SCRATCH/logs/{project_name}",
        mila_access=mila_access,
        accounts=_default_accounts(template, clusters, mila_access),
    )


def ask_questions(
    template: str, defaults: InitAnswers, home_project_dir: str, scratch_project_dir: str
) -> InitAnswers:
    """Asks the questions of `cluv init` in the terminal, with `defaults` as default answers.

    `home_project_dir` is the path of the project on this machine, starting with `$HOME`. Without
    `project_dir`, the project goes in that path on the clusters, so the question about
    `project_dir` shows that path while the answer is empty. On the clusters that can't run jobs
    from $HOME, the project goes in `scratch_project_dir` instead, unless `project_dir` is in
    $SCRATCH: a note before the question says so.
    """

    def ask_text(message: str, **kwargs) -> str:
        return questionary.text(message, **kwargs).unsafe_ask().strip()

    def path(text: str) -> bool | str:
        # cluv replaces the $VARIABLES in paths, but not `~`.
        return "~" not in text or "Write $HOME instead of ~."

    def required_path(text: str) -> bool | str:
        return path(text) if text.strip() else "This value is required."

    answers = dataclasses.replace(defaults)
    answers.mila_access = questionary.confirm(
        "Do you have access to the Mila cluster?", default=defaults.mila_access
    ).unsafe_ask()

    def selected_by_default(cluster: str) -> bool:
        if cluster == MILA_CLUSTER:
            return answers.mila_access
        return cluster in defaults.clusters

    answers.clusters = questionary.checkbox(
        "Which clusters do you want to use?",
        choices=[
            questionary.Choice(cluster, checked=selected_by_default(cluster))
            for cluster in get_cluster_names(template)
        ],
        validate=lambda selected: bool(selected) or "Select at least one cluster.",
    ).unsafe_ask()
    default_accounts = _default_accounts(template, answers.clusters, answers.mila_access)
    # One question asks for the accounts on all the clusters of the same kind (PAICE or DRAC) with
    # the same default accounts, since these clusters often share accounts. For the users who
    # have access to the Mila cluster, Narval gets its own question: the accounts of Mila members
    # on Narval differ from those on the other DRAC clusters.
    groups: dict[tuple[str, str], list[str]] = {}
    for cluster in answers.clusters:
        # Jobs on the Mila cluster usually don't need an account, so no question asks for one.
        if cluster != MILA_CLUSTER:
            default = ", ".join(default_accounts[cluster])
            groups.setdefault((_cluster_kind(cluster), default), []).append(cluster)
    answers.accounts = {}
    for (kind, default), clusters in groups.items():
        names = ", ".join(clusters)
        where = f"{kind} clusters ({names})" if len(clusters) > 1 else f"{kind} cluster {names}"
        accounts = ask_text(
            f"Which Slurm accounts do you use on the {where}?",
            default=default,
            instruction=_accounts_instruction(clusters[0]),
        )
        for cluster in clusters:
            answers.accounts[cluster] = accounts.replace(",", " ").split()
    answers.results_path = ask_text(
        "Where should the job results go on the clusters?",
        default=defaults.results_path,
        validate=required_path,
    )
    scratch_clusters = [
        cluster for cluster in _clusters_that_refuse_home(template) if cluster in answers.clusters
    ]
    if scratch_clusters:
        console.print(
            f"Since {join_names(scratch_clusters)} can't run jobs from $HOME, the project goes in "
            f"{scratch_project_dir} there, unless your answer is a path in $SCRATCH.",
            style="yellow",
        )
    answers.project_dir = ask_text(
        "Where should the project go on the clusters?",
        instruction="(leave empty for the same path as on this machine)",
        # prompt_toolkit shows the placeholder (in grey) while the input is empty, and never
        # returns it: an empty answer leaves `project_dir` commented out in the config.
        placeholder=[("fg:ansibrightblack", home_project_dir)],
        validate=path,
    )
    answers.data_source = ask_text(
        "Where are the datasets that cluv should copy to all the clusters?",
        instruction="(<cluster>:<path>, or a path on this machine; leave empty if there are none)",
        validate=path,
    )
    if answers.data_source:
        answers.datasets_path = ask_text(
            "Where should the datasets go on the clusters?", validate=required_path
        )
    return answers


def make_cluv_config(template: str, answers: InitAnswers, scratch_project_dir: str) -> str:
    """Fills the template's `[tool.cluv]` section with the answers to the questions.

    `scratch_project_dir` replaces the template's `project_dir` on the clusters that can't run
    jobs from $HOME.

    The clusters that aren't selected stay in the config, commented out, so that the user can
    uncomment them later.
    """
    config = template.replace(f'"{DEFAULT_RESULTS_PATH}"', to_toml_string(answers.results_path))
    config = config.replace(
        f'"{DEFAULT_SCRATCH_PROJECT_DIR}"', to_toml_string(scratch_project_dir)
    )
    for key in ("project_dir", "data_source", "datasets_path"):
        if value := getattr(answers, key):
            # Uncomment the template's line for that setting, with the answer as value.
            # A replacement string would have its backslashes interpreted by re.sub, so `line`
            # is returned by a function, which re.sub inserts as is.
            line = f"{key} = {to_toml_string(value)}"
            config = re.sub(rf"^# {key} = .*$", lambda _: line, config, count=1, flags=re.M)

    # A `project_dir` set for one cluster overrides the global one. For the clusters that can't
    # run jobs from $HOME, the template sets a `project_dir` in $SCRATCH. That cluster
    # `project_dir` is only removed when the global `project_dir` is in $SCRATCH too, because the
    # clusters could refuse the jobs with any other global `project_dir`.
    project_in_scratch = answers.project_dir.split("/")[0] in ("$SCRATCH", "${SCRATCH}")
    # No question asks for the accounts of the clusters that aren't selected, so these clusters
    # get the default answers: the accounts of Mila members for the users who have access to the
    # Mila cluster, no account for the other users.
    default_accounts = _default_accounts(
        template, get_cluster_names(template), answers.mila_access
    )
    lines = []
    for section in split_sections(config):
        cluster = get_cluster_name(section)
        if cluster is None:
            lines += section
            continue
        selected = cluster in answers.clusters
        if project_in_scratch:
            section = _remove_project_dir(section)
        accounts = answers.accounts.get(cluster, []) if selected else default_accounts[cluster]
        if not selected and not accounts and cluster != MILA_CLUSTER:
            # An example account shows where to write the user's account before uncommenting the
            # cluster. Jobs on the Mila cluster usually don't need an account.
            accounts = [_example_account(cluster)]
        # Only rewritten when the accounts changed, to keep the comment of the template's line.
        if accounts != _accounts(section):
            section = _set_accounts(section, accounts)
        lines += section if selected else _comment_out(section)
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


def check_job_script(project_root: Path, results_path: str) -> None:
    """
    Check if job script templates exist. If not, create them.
    The scripts are templates for users to submit jobs to Slurm with cluv.
    """
    project_root_for_script = get_home_project_dir(project_root)
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


def check_job_script_paths(project_root: Path, config: CluvConfig) -> None:
    """Checks that the default job scripts of the clusters (`job_script_path`) are in the project."""
    job_script_paths = {
        config.get_cluster_config(cluster).job_script_path for cluster in config.clusters_names
    }
    for job_script_path in sorted(path for path in job_script_paths if path is not None):
        if (project_root / os.path.expandvars(job_script_path)).is_file():
            console.print(f"✅ Job script of the config found: {job_script_path}", style="green")
        else:
            console.print(
                f"❌ Job script of the config not found: {job_script_path}. Fix "
                "`job_script_path` in pyproject.toml.",
                style="red",
            )


def _clusters_that_refuse_home(template: str) -> list[str]:
    """The clusters that can't run jobs from $HOME: the template sets their `project_dir`."""
    return [
        cluster
        for section in split_sections(template)
        if (cluster := get_cluster_name(section))
        and any(line.startswith("project_dir = ") for line in section)
    ]


def _default_accounts(
    template: str, clusters: list[str], mila_access: bool
) -> dict[str, list[str]]:
    """The default Slurm accounts on each cluster.

    The template has the accounts of Mila members, so they are only used for the users who have
    access to the Mila cluster, even when the Mila cluster isn't selected. For the other users,
    there is no default account.
    """
    if not mila_access:
        return {cluster: [] for cluster in clusters}
    template_accounts = {
        cluster: _accounts(section)
        for section in split_sections(template)
        if (cluster := get_cluster_name(section))
    }
    return {cluster: template_accounts[cluster] for cluster in clusters}


def _cluster_kind(cluster: str) -> str:
    """The kind of a cluster other than Mila: "PAICE" or "DRAC"."""
    return "PAICE" if cluster in PAICE_CLUSTERS else "DRAC"


def _accounts_instruction(cluster: str) -> str:
    """The instruction of the question about the Slurm accounts on the cluster.

    The instruction gives an example of account name.
    """
    instruction = "separate accounts with commas; leave empty for none"
    return f"(e.g. {_example_account(cluster)}; {instruction})"


def _example_account(cluster: str) -> str:
    """An example of Slurm account name on a PAICE or DRAC cluster."""
    return "aip-yourpi" if cluster in PAICE_CLUSTERS else "def-yourpi"


def _accounts(section: list[str]) -> list[str]:
    """The Slurm accounts in the section of a cluster."""
    return [
        account
        for line in section
        if line.startswith("sbatch_args = ")
        for account in re.findall(r'account = "([^"]*)"', line)
    ]


def _remove_project_dir(section: list[str]) -> list[str]:
    """Removes the `project_dir` line from the section of a cluster.

    The comment lines right above the `project_dir` line explain that line, so these comment lines
    are removed too.
    """
    lines: list[str] = []
    for line in section:
        if line.startswith("project_dir = "):
            while lines and lines[-1].startswith("#"):
                lines.pop()
        else:
            lines.append(line)
    return lines


def _set_accounts(section: list[str], accounts: list[str]) -> list[str]:
    """Replaces the Slurm accounts in the section of a cluster.

    The template's `sbatch_args` of the clusters only set accounts, so these lines are replaced.
    """
    section = [line for line in section if not line.startswith("sbatch_args = ")]
    if accounts:
        # Under the header and the `cluster_link` line.
        section.insert(2, get_sbatch_args_line(accounts))
    return section


def _comment_out(section: list[str]) -> list[str]:
    """Comments out the section of a cluster.

    The comment lines of the section get a second `#`, so that removing one `#` from each line
    gives back the section.
    """
    commented = []
    for line in section:
        if line.startswith("#"):
            commented.append(f"#{line}")
        elif line:
            commented.append(f"# {line}")
        else:
            commented.append(line)
    return commented


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
