import asyncio
import dataclasses
import json
import os
import re
import shlex
import subprocess
import sys
from enum import Enum
from pathlib import Path, PurePosixPath

import questionary
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cluv.cache import DisabledCluster, get_disabled_clusters
from cluv.cli.disable import format_remaining
from cluv.config import (
    ClusterConfig,
    CluvConfig,
    find_pyproject,
    has_cluv_config,
    load_cluv_config,
)
from cluv.remote import Remote, control_socket_is_running, get_ssh_options_for_host
from cluv.utils import console, current_cluster

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
TROUBLESHOOTING_URL = "https://mila-iqia.github.io/cluv/troubleshooting/"
# The checks that run on the clusters, for the messages about the clusters where they don't run.
REMOTE_CHECKS = "GitHub access, Slurm accounts and folders"
# How long the checks on a cluster can take, in seconds. A command that reads a file system of a
# cluster can stay blocked for hours when that file system doesn't answer.
REMOTE_CHECKS_TIMEOUT = 60
# The status of the clusters of the Alliance, which are the clusters of the template except Mila.
ALLIANCE_STATUS_URL = "https://status.alliancecan.ca/"
# Checks a folder on a cluster, after replacing the variables of its path, like $SCRATCH. The
# output is the state of the folder, its path, and the closest folder above it that exists. The
# lines are joined with `; `, since the login shell of the user (which runs bash) might not accept
# a command with many lines. The script doesn't run `exit`, which would make bash run the
# ~/.bash_logout of the user, and maybe print something after the output.
FOLDER_SCRIPT = r"""
set -u
path="{path}"
parent=$(dirname "$path")
while [ ! -e "$parent" ]; do parent=$(dirname "$parent"); done
if [ -d "$path" ]; then state=folder
elif [ -e "$path" ]; then state=file
elif [ -d "$parent" ] && [ -w "$parent" ]; then state=creatable
else state=blocked; fi
printf '%s\t%s\t%s\n' "$state" "$path" "$parent"
"""


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
    template = _load_cluv_config_template()
    project_root = pyproject_path.parent
    scratch_project_dir = _scratch_project_dir(project_root)
    answers = default_answers(template, project_root.name)
    if ask:
        try:
            answers = ask_questions(
                template, answers, _home_project_dir(project_root), scratch_project_dir
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
    all_clusters = _cluster_names(template)
    clusters = [cluster for cluster in all_clusters if _has_ssh_entry(cluster)] or all_clusters
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
            for cluster in _cluster_names(template)
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
            f"Since {_join(scratch_clusters)} can't run jobs from $HOME, the project goes in "
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
    config = template.replace(f'"{DEFAULT_RESULTS_PATH}"', _toml_string(answers.results_path))
    config = config.replace(f'"{DEFAULT_SCRATCH_PROJECT_DIR}"', _toml_string(scratch_project_dir))
    for key in ("project_dir", "data_source", "datasets_path"):
        if value := getattr(answers, key):
            # Uncomment the template's line for that setting, with the answer as value.
            # A replacement string would have its backslashes interpreted by re.sub, so `line`
            # is returned by a function, which re.sub inserts as is.
            line = f"{key} = {_toml_string(value)}"
            config = re.sub(rf"^# {key} = .*$", lambda _: line, config, count=1, flags=re.M)

    # A `project_dir` set for one cluster overrides the global one. For the clusters that can't
    # run jobs from $HOME, the template sets a `project_dir` in $SCRATCH. That cluster
    # `project_dir` is only removed when the global `project_dir` is in $SCRATCH too, because the
    # clusters could refuse the jobs with any other global `project_dir`.
    project_in_scratch = answers.project_dir.split("/")[0] in ("$SCRATCH", "${SCRATCH}")
    # No question asks for the accounts of the clusters that aren't selected, so these clusters
    # get the default answers: the accounts of Mila members for the users who have access to the
    # Mila cluster, no account for the other users.
    default_accounts = _default_accounts(template, _cluster_names(template), answers.mila_access)
    lines = []
    for section in _split_sections(config):
        cluster = _cluster_name(section)
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


class Status(Enum):
    """The result of a check, with its icon and its color."""

    OK = ("✔", "green")
    WARN = ("!", "yellow")
    FAIL = ("✘", "red")
    # The check wasn't done.
    SKIP = ("-", "dim")

    @property
    def icon(self) -> Text:
        return Text(self.value[0], style=f"bold {self.color}")

    @property
    def color(self) -> str:
        return self.value[1]


# How the summary of the checks names the clusters, from the worst result of their checks.
SUMMARY_LABELS = {
    Status.OK: "ready",
    Status.WARN: "with warnings",
    Status.SKIP: "not checked",
    Status.FAIL: "with problems",
}


@dataclasses.dataclass(frozen=True)
class Check:
    """The result of a check on a cluster.

    The hints say how to fix a problem, and `code` is a line to copy into a config file.
    """

    status: Status
    message: str
    hints: tuple[str, ...] = ()
    code: str = ""


async def check_clusters(
    config: CluvConfig, project_root: Path, ask: bool = False
) -> dict[str, list[Check]]:
    """Checks the access to each cluster of the config, and shows the results in a frame for each
    cluster. Returns the checks of each cluster.

    Some checks run on the clusters. When `ask` is True, the user is offered to connect to the
    clusters that aren't connected yet.
    """
    disabled = get_disabled_clusters()
    reports = {
        cluster: check_cluster(config, cluster, disabled) for cluster in config.clusters_names
    }
    if not reports:
        return reports
    # The clusters that cluv connects to with SSH.
    clusters = [
        cluster
        for cluster in reports
        if cluster != current_cluster() and cluster not in disabled and _has_ssh_entry(cluster)
    ]
    connections = await connect_to_clusters(config, clusters, ask)
    connected = [cluster for cluster in clusters if connections[cluster].status is Status.OK]
    with console.status(f"Checking {REMOTE_CHECKS} on the clusters..."):
        results = await asyncio.gather(
            *(check_on_cluster(config, cluster, project_root) for cluster in connected)
        )
    for cluster, connection in connections.items():
        reports[cluster].append(connection)
    for cluster, checks in zip(connected, results):
        reports[cluster] += checks
    if (here := current_cluster()) in reports:
        reports[here].append(Check(Status.SKIP, f"{REMOTE_CHECKS} not checked on this cluster"))

    for cluster, checks in reports.items():
        console.print(cluster_panel(cluster, checks))
    print_summary(reports)
    print_keep_alive_tip(config.clusters_names)
    return reports


def check_cluster(
    config: CluvConfig, cluster: str, disabled: dict[str, DisabledCluster]
) -> list[Check]:
    """Checks the SSH entry of the cluster, and whether the cluster is disabled."""
    if cluster == current_cluster():
        # cluv runs the commands of the current cluster without SSH.
        checks = [Check(Status.OK, "current cluster: no SSH connection needed")]
    else:
        checks = [check_ssh_entry(cluster, config.get_cluster_config(cluster).cluster_link)]
    if (disabled_cluster := disabled.get(cluster)) is not None:
        checks.append(check_disabled(cluster, disabled_cluster))
    return checks


def check_ssh_entry(cluster: str, cluster_link: str | None) -> Check:
    """Checks that ~/.ssh/config has an entry for the cluster, since cluv runs `ssh <cluster>`."""
    if _has_ssh_entry(cluster):
        return Check(Status.OK, "SSH entry in ~/.ssh/config")
    # `mila init` adds the entries of the Mila cluster and of the DRAC and PAICE clusters, which
    # are the clusters of the template.
    if cluster in _cluster_names(_load_cluv_config_template()):
        hints = [f"if you have access to {cluster}, run `mila init` to add it"]
    else:
        hints = [f"if you have access to {cluster}, add a `Host {cluster}` entry for it"]
    if cluster_link:
        hints.append(f"otherwise, see how to get access: {cluster_link}")
    return Check(Status.FAIL, "no entry in ~/.ssh/config", tuple(hints))


def check_disabled(cluster: str, disabled: DisabledCluster) -> Check:
    """Warns that the cluster is disabled with `cluv disable`, so cluv commands skip it."""
    period = (
        "indefinitely"
        if disabled.disabled_until is None
        else f"for {format_remaining(disabled.disabled_until)}"
    )
    return Check(
        Status.WARN,
        f"disabled {period}: cluv commands skip {cluster}",
        (f"run `cluv enable {cluster}` to use it again",),
    )


async def connect_to_clusters(
    config: CluvConfig, clusters: list[str], ask: bool
) -> dict[str, Check]:
    """Returns whether each cluster is connected, after connecting to the clusters.

    Each new connection can ask for a 2FA, so the clusters that aren't connected yet are only
    connected when `ask` is True and the user agrees.
    """
    running = await asyncio.gather(*(control_socket_is_running(cluster) for cluster in clusters))
    connections = {
        cluster: Check(Status.OK, "connected")
        for cluster, is_running in zip(clusters, running)
        if is_running
    }
    missing = [cluster for cluster in clusters if cluster not in connections]
    if missing and ask and await _ask_to_connect(missing):
        # One cluster at a time, so that the 2FA prompts of the clusters don't mix.
        for cluster in missing:
            cluster_link = config.get_cluster_config(cluster).cluster_link
            connections[cluster] = await _connect(cluster, cluster_link)
    for cluster in missing:
        connections.setdefault(
            cluster,
            Check(
                Status.SKIP,
                f"not connected: {REMOTE_CHECKS} not checked",
                (f"run `cluv login {cluster}`, then `cluv init` again",),
            ),
        )
    return connections


async def check_on_cluster(config: CluvConfig, cluster: str, project_root: Path) -> list[Check]:
    """The checks that run on the cluster, through the SSH connection to the cluster.

    Without a `project_dir`, the project goes in the same folder as on this machine, from $HOME.
    When the checks don't end within `REMOTE_CHECKS_TIMEOUT` seconds, they stop, and the cluster
    gets a warning instead.
    """
    remote = Remote(cluster)
    cluster_config = config.get_cluster_config(cluster)
    project_dir = cluster_config.project_dir or PurePosixPath(_home_project_dir(project_root))
    checks = [
        check_github(remote),
        check_accounts(remote, cluster_config),
        check_folder(remote, "project folder", "project_dir", project_dir),
        check_folder(remote, "results folder", "results_path", cluster_config.results_path),
    ]
    if cluster == config.data_source_cluster and config.data_source_path:
        # `cluv sync` copies the datasets from that folder to the other clusters.
        datasets = PurePosixPath(config.data_source_path)
        checks.append(
            check_folder(remote, "datasets to copy", "data_source", datasets, must_exist=True)
        )
    elif config.data_source and cluster_config.datasets_path:
        datasets_path = cluster_config.datasets_path
        checks.append(check_folder(remote, "datasets folder", "datasets_path", datasets_path))
    try:
        return list(await asyncio.wait_for(asyncio.gather(*checks), REMOTE_CHECKS_TIMEOUT))
    except TimeoutError:
        # The checks are cancelled, which stops their `ssh` commands.
        hints = [
            f"{cluster} may have a problem, such as a file system that doesn't answer: run "
            "`cluv init` again later"
        ]
        if cluster != MILA_CLUSTER and cluster in _cluster_names(_load_cluv_config_template()):
            hints.append(f"see the status of the Alliance clusters: {ALLIANCE_STATUS_URL}")
        return [
            Check(
                Status.WARN,
                f"no answer from {cluster} after {REMOTE_CHECKS_TIMEOUT} s: {REMOTE_CHECKS} not "
                "checked",
                tuple(hints),
            )
        ]


async def check_github(remote: Remote) -> Check:
    """Checks that the cluster connects to GitHub with SSH, with a key of the cluster or a key
    forwarded from this machine. `cluv sync` clones the project from GitHub on the cluster."""
    # `ssh -T git@github.com` exits with 1, even when the connection works.
    result = await remote.run(
        "ssh -T -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "
        "git@github.com",
        warn=True,
        hide=True,
        display=False,
    )
    if "successfully authenticated" in result.stderr:
        return Check(Status.OK, "SSH access to GitHub")
    return Check(
        Status.FAIL,
        "no SSH access to GitHub",
        (
            f"see how to set it up: {TROUBLESHOOTING_URL}"
            "#setting-up-ssh-access-to-github-from-the-clusters",
        ),
    )


async def check_accounts(remote: Remote, cluster_config: ClusterConfig) -> Check:
    """Checks the Slurm accounts of the cluster in the config (`sbatch_args`) against the accounts
    of the user on the cluster.

    When the config doesn't have all the accounts of the user, the check suggests a `sbatch_args`
    line with all of them: with many accounts, `cluv submit` submits a job with each account, and
    keeps the first job that starts.
    """
    cluster = remote.hostname
    accounts = [str(args["account"]) for args in cluster_config.sbatch_args if "account" in args]
    result = await remote.run(
        "bash --login -c 'sacctmgr --noheader --parsable2 show associations user=$USER "
        "format=account'",
        warn=True,
        hide=True,
        display=False,
    )
    associations = {
        line.strip()
        for line in result.stdout.splitlines()
        if re.fullmatch(r"[\w.-]+", line.strip())
    }
    if result.returncode != 0 or not associations:
        return Check(Status.SKIP, f"couldn't list your Slurm accounts on {cluster}")

    def account_name(association: str) -> str:
        # On the DRAC clusters, an account like `def-pi` has an association for the CPU jobs
        # (`def-pi_cpu`) and another one for the GPU jobs (`def-pi_gpu`).
        return re.sub(r"_[cg]pu$", "", association)

    user_accounts = sorted({account_name(association) for association in associations})
    unknown = [a for a in accounts if a not in associations and a not in user_accounts]
    others = [a for a in user_accounts if a not in {account_name(b) for b in accounts}]
    # The accounts of the config that are the user's, then the other accounts of the user.
    suggestion = _sbatch_args_line([a for a in accounts if a not in unknown] + others)
    where = f"in pyproject.toml, under [tool.cluv.clusters.{cluster}]"
    if unknown:
        return Check(
            Status.FAIL,
            f"not among your Slurm accounts on {cluster}: {', '.join(unknown)}",
            (f"your accounts: {', '.join(user_accounts)}. Set {where}:",),
            suggestion,
        )
    if accounts:
        message = f"Slurm account{'s' if len(accounts) > 1 else ''}: {', '.join(accounts)}"
        if others:
            hint = f"you also have {', '.join(others)}. To use all your accounts, set {where}:"
            return Check(Status.OK, message, (hint,), suggestion)
        return Check(Status.OK, message)
    if cluster == MILA_CLUSTER:
        return Check(Status.OK, "no Slurm account set (usually not needed on the Mila cluster)")
    if len(user_accounts) == 1:
        return Check(
            Status.OK, f"no Slurm account set (Slurm uses your only account, {user_accounts[0]})"
        )
    return Check(
        Status.WARN,
        f"no Slurm account set, but you have several on {cluster}: jobs may need one",
        (f"to use all your accounts, set {where}:",),
        suggestion,
    )


async def check_folder(
    remote: Remote, name: str, key: str, path: PurePosixPath, must_exist: bool = False
) -> Check:
    """Checks that the folder exists on the cluster, or that `cluv sync` can create it.

    `key` is the setting of the config with the path of the folder. When `must_exist` is True,
    the folder (or file) has to exist.
    """
    cluster = remote.hostname
    if key == "data_source":
        hints = ("fix `data_source` in pyproject.toml",)
    else:
        hints = (
            f"set `{key}` for {cluster} in pyproject.toml, under [tool.cluv.clusters.{cluster}]",
        )
    script = "; ".join(FOLDER_SCRIPT.format(path=path).strip().splitlines())
    result = await remote.run(
        f"bash --login -c {shlex.quote(script)}", warn=True, hide=True, display=False
    )
    lines = result.stdout.splitlines()
    fields = lines[-1].split("\t") if result.returncode == 0 and lines else []
    if len(fields) != 3:
        # With `set -u`, bash stops with an error when a variable of the path isn't defined.
        error_lines = [line.strip() for line in result.stderr.splitlines() if line.strip()]
        error = error_lines[-1] if error_lines else f"exit code {result.returncode}"
        return Check(Status.FAIL, f"{name}: can't check {path}: {error}", hints)
    state, folder, parent = fields
    if state == "folder" or (must_exist and state == "file"):
        return Check(Status.OK, f"{name}: {folder}")
    if must_exist:
        return Check(Status.FAIL, f"{name}: {folder} doesn't exist on {cluster}", hints)
    if state == "file":
        return Check(Status.FAIL, f"{name}: {folder} is a file, not a folder", hints)
    if state == "creatable":
        return Check(Status.OK, f"{name}: {folder} (cluv sync will create it)")
    return Check(Status.FAIL, f"{name}: can't create {folder}, no write access to {parent}", hints)


async def _ask_to_connect(clusters: list[str]) -> bool:
    """Asks whether to connect to the clusters now. Ctrl-C or Ctrl-D mean no."""
    try:
        return await questionary.confirm(
            f"Connect to {_join(clusters)} now, to check them? There may be a 2FA prompt for each "
            "cluster.",
            default=True,
        ).unsafe_ask_async()
    except (KeyboardInterrupt, EOFError):
        return False


async def _connect(cluster: str, cluster_link: str | None) -> Check:
    """Opens an SSH connection to the cluster, which stays open for the next cluv commands."""
    console.print(f"Connecting to {cluster}...")
    result = await Remote(cluster).run("echo OK", display=False, warn=True, hide=True)
    if "OK" in result.stdout:
        return Check(Status.OK, "connected")
    # The last line of the SSH errors says why the connection failed.
    error_lines = [line.strip() for line in result.stderr.splitlines() if line.strip()]
    error = error_lines[-1] if error_lines else f"ssh exited with {result.returncode}"
    hints = [f"run `ssh {cluster}` to see the details"]
    if cluster_link:
        hints.append(f"if you don't have access to {cluster} yet, see: {cluster_link}")
    return Check(Status.FAIL, f"connection failed: {error}", tuple(hints))


def cluster_panel(cluster: str, checks: list[Check]) -> Panel:
    """A frame with the result of each check of the cluster, in the color of the worst result.

    The title of the frame has the name of the cluster and the number of checks of each result.
    """
    counts = Text("  ").join(
        Text.assemble(status.icon, f" {count}")
        for status in Status
        if (count := sum(check.status is status for check in checks))
    )
    rows = Table.grid(padding=(0, 1))
    rows.add_column(width=1)
    rows.add_column()
    for check in checks:
        lines = [_text(check.message)]
        lines += [_text(f"→ {hint}", style="dim italic") for hint in check.hints]
        lines += [_text(f"  {line}", style="cyan") for line in check.code.splitlines()]
        rows.add_row(check.status.icon, Group(*lines))
    title = Text.assemble((cluster, "bold"), "  ", counts)
    return Panel(rows, title=title, title_align="left", border_style=_worst_status(checks).color)


def print_summary(reports: dict[str, list[Check]]) -> None:
    """Prints how many clusters are ready, and how many have warnings or problems."""
    statuses = [_worst_status(checks) for checks in reports.values()]
    counts = Text(" · ").join(
        Text(f"{statuses.count(status)} {label}", style=status.color)
        for status, label in SUMMARY_LABELS.items()
        if status in statuses
    )
    console.rule(Text.assemble("Clusters: ", counts))


def print_keep_alive_tip(clusters: list[str]) -> None:
    """Suggests the SSH options that keep the connections to the clusters open.

    A network or a cluster can close an SSH connection that stays idle for too long. With
    `ServerAliveInterval`, SSH sends a message to the cluster regularly, so the connection is never
    idle. `mila init` only sets that option for the Mila cluster.
    """
    clusters = [
        cluster
        for cluster in clusters
        if cluster != current_cluster()
        and _has_ssh_entry(cluster)
        and get_ssh_options_for_host(cluster).get("serveraliveinterval", "0") == "0"
    ]
    if clusters:
        console.print(
            "💡 Optional: a network can close an SSH connection that stays idle for a long time, "
            "and you would then need `cluv login` (and a 2FA) again. To keep the connections "
            f"open, add these lines to the `Host` entries of {_join(clusters)} in ~/.ssh/config:"
        )
        console.print("    ServerAliveInterval 120\n    ServerAliveCountMax 5", style="cyan")


def _has_ssh_entry(cluster: str) -> bool:
    """Whether the SSH config has an entry for the cluster.

    `ssh -G` reads the whole SSH config, with the files that it includes. Without an entry for the
    cluster, the host name that `ssh -G` gives is the name of the cluster.
    """
    return get_ssh_options_for_host(cluster).get("hostname", cluster).lower() != cluster.lower()


def _worst_status(checks: list[Check]) -> Status:
    """The worst result of the checks. A check that wasn't done is only worse than a success."""
    statuses = {check.status for check in checks}
    return next((s for s in (Status.FAIL, Status.WARN, Status.SKIP) if s in statuses), Status.OK)


def _text(text: str, style: str = "") -> Text:
    """The text of a line in a frame.

    A URL or a path that is too long for the frame continues on the next line instead of being
    cut. The URLs are links, which still work when they continue on the next line.
    """
    rich_text = Text(text, style=style, overflow="fold")
    for url in re.finditer(r"https?://\S+", text):
        rich_text.stylize(f"link {url.group()}", url.start(), url.end())
    return rich_text


def check_job_script(project_root: Path, results_path: str) -> None:
    """
    Check if job script templates exist. If not, create them.
    The scripts are templates for users to submit jobs to Slurm with cluv.
    """
    project_root_for_script = _home_project_dir(project_root)
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


def _clusters_that_refuse_home(template: str) -> list[str]:
    """The clusters that can't run jobs from $HOME: the template sets their `project_dir`."""
    return [
        cluster
        for section in _split_sections(template)
        if (cluster := _cluster_name(section))
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
        for section in _split_sections(template)
        if (cluster := _cluster_name(section))
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
        section.insert(2, _sbatch_args_line(accounts))
    return section


def _sbatch_args_line(accounts: list[str]) -> str:
    """The `sbatch_args` line of a cluster with these Slurm accounts.

    >>> _sbatch_args_line(["def-pi"])
    'sbatch_args = { account = "def-pi" }'
    >>> _sbatch_args_line(["def-pi", "rrg-pi-ad"])
    'sbatch_args = [{ account = "def-pi" }, { account = "rrg-pi-ad" }]'
    """
    tables = [f"{{ account = {_toml_string(account)} }}" for account in accounts]
    value = tables[0] if len(tables) == 1 else f"[{', '.join(tables)}]"
    return f"sbatch_args = {value}"


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


def _home_project_dir(project_root: Path) -> str:
    """The path of the project, with `$HOME` instead of the home directory.

    That's where the project goes on the clusters when `project_dir` isn't set. A project outside
    of the home directory keeps its absolute path.
    """
    try:
        return f"$HOME/{project_root.relative_to(Path.home()).as_posix()}"
    except ValueError:
        return str(project_root)


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


def _join(names: list[str]) -> str:
    """Joins names for a sentence: "a", "a and b", "a, b and c"."""
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


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
