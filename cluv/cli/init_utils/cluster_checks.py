"""The checks of the clusters at the end of `cluv init`.

Each cluster of the config gets a frame with the result of each check, and how to fix each problem.
"""

import asyncio
import dataclasses
import re
import shlex
from enum import Enum
from pathlib import Path, PurePosixPath

import questionary
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cluv.cache import DisabledCluster, get_disabled_clusters
from cluv.cli.disable import format_remaining
from cluv.cli.init_utils.template import (
    MILA_CLUSTER,
    get_cluster_names,
    get_home_project_dir,
    load_cluv_config_template,
    get_sbatch_args_line,
)
from cluv.config import ClusterConfig, CluvConfig
from cluv.remote import Remote, control_socket_is_running, get_ssh_options_for_host
from cluv.utils import console, current_cluster, join_names

TROUBLESHOOTING_URL = "https://mila-iqia.github.io/cluv/troubleshooting/"
# The checks that run on the clusters, for the messages about the clusters where they don't run.
REMOTE_CHECKS = "GitHub access, Slurm accounts and folders"
# How long the checks on a cluster can take, in seconds. A command that reads a file system of a
# cluster can stay blocked for hours when that file system doesn't answer.
REMOTE_CHECKS_TIMEOUT = 60
# The status of the clusters of the Alliance, which are the clusters of the template except Mila.
ALLIANCE_STATUS_URL = "https://status.alliancecan.ca/"
# The SSH options that make the SSH commands to a cluster share one connection, so that the 2FA of
# `cluv login` is the only one. The values are those of the troubleshooting page.
MULTIPLEXING_OPTIONS = {
    "ControlMaster": "auto",
    "ControlPath": "~/.ssh/cm-%r@%h:%p",
    "ControlPersist": "yes",
}
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

    The hints say how to fix a problem, and `code` has the lines to copy into a config file.
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
        if cluster != current_cluster() and cluster not in disabled and has_ssh_entry(cluster)
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
    """Checks the SSH entry of the cluster and its options, and whether the cluster is disabled."""
    if cluster == current_cluster():
        # cluv runs the commands of the current cluster without SSH.
        checks = [Check(Status.OK, "current cluster: no SSH connection needed")]
    else:
        checks = [check_ssh_entry(cluster, config.get_cluster_config(cluster).cluster_link)]
        if has_ssh_entry(cluster) and (multiplexing := check_multiplexing(cluster)) is not None:
            checks.append(multiplexing)
    if (disabled_cluster := disabled.get(cluster)) is not None:
        checks.append(check_disabled(cluster, disabled_cluster))
    return checks


def check_ssh_entry(cluster: str, cluster_link: str | None) -> Check:
    """Checks that ~/.ssh/config has an entry for the cluster, since cluv runs `ssh <cluster>`."""
    if has_ssh_entry(cluster):
        return Check(Status.OK, "SSH entry in ~/.ssh/config")
    # `mila init` adds the entries of the Mila cluster and of the DRAC and PAICE clusters, which
    # are the clusters of the template.
    if cluster in get_cluster_names(load_cluv_config_template()):
        hints = [f"if you have access to {cluster}, run `mila init` to add it"]
    else:
        hints = [f"if you have access to {cluster}, add a `Host {cluster}` entry for it"]
    if cluster_link:
        hints.append(f"otherwise, see how to get access: {cluster_link}")
    return Check(Status.FAIL, "no entry in ~/.ssh/config", tuple(hints))


def check_multiplexing(cluster: str) -> Check | None:
    """Warns when the SSH entry of the cluster doesn't make the SSH commands share one connection.

    Without `ControlMaster` and `ControlPath`, the `rsync` commands of `cluv sync` open their own
    connection. Without `ControlPersist`, a connection closes when the command that opened it ends,
    like the command of `cluv login`. A new connection can ask for a 2FA.
    """
    options = get_ssh_options_for_host(cluster)
    # `ssh -G` gives `controlmaster false` and `controlpersist no` when these options aren't set,
    # and no `controlpath`.
    missing = [
        option
        for option in MULTIPLEXING_OPTIONS
        if options.get(option.lower(), "no") in ("no", "false", "none")
    ]
    if not missing:
        return None
    where = f"the `Host` entry of {cluster} in ~/.ssh/config"
    # `mila init` adds these options to the entries of the clusters of the template.
    if cluster in get_cluster_names(load_cluv_config_template()):
        hint = f"run `mila init`, or add these lines to {where}:"
    else:
        hint = f"add these lines to {where}:"
    return Check(
        Status.WARN,
        f"SSH entry without {join_names(missing)}: cluv commands can ask for a 2FA again",
        (hint,),
        "\n".join(f"{option} {MULTIPLEXING_OPTIONS[option]}" for option in missing),
    )


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
    if missing and ask and await _ask_to_connect(missing, connected=list(connections)):
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
    project_dir = cluster_config.project_dir or PurePosixPath(get_home_project_dir(project_root))
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
        if cluster != MILA_CLUSTER and cluster in get_cluster_names(load_cluv_config_template()):
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
    of the user on the cluster. Without any account on the cluster, Slurm refuses the jobs of the
    user.

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
    if result.returncode != 0:
        return Check(Status.SKIP, f"couldn't list your Slurm accounts on {cluster}")
    if not associations:
        hints: list[str] = []
        if cluster_config.cluster_link:
            hints.append(f"to get access, see {cluster_config.cluster_link}")
        hints.append(
            f"to stop using {cluster}, remove [tool.cluv.clusters.{cluster}] from pyproject.toml, "
            f"or run `cluv disable {cluster}`"
        )
        return Check(
            Status.FAIL,
            f"you have no Slurm account on {cluster}: jobs can't run there",
            tuple(hints),
        )

    def account_name(association: str) -> str:
        # On the DRAC clusters, an account like `def-pi` has an association for the CPU jobs
        # (`def-pi_cpu`) and another one for the GPU jobs (`def-pi_gpu`).
        return re.sub(r"_[cg]pu$", "", association)

    user_accounts = sorted({account_name(association) for association in associations})
    unknown = [a for a in accounts if a not in associations and a not in user_accounts]
    others = [a for a in user_accounts if a not in {account_name(b) for b in accounts}]
    # The accounts of the config that are the user's, then the other accounts of the user.
    suggestion = get_sbatch_args_line([a for a in accounts if a not in unknown] + others)
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


async def _ask_to_connect(clusters: list[str], connected: list[str]) -> bool:
    """Asks whether to connect to the clusters now, after naming the clusters that are already
    connected. Ctrl-C raises KeyboardInterrupt and Ctrl-D raises EOFError, which stop `cluv init`.
    """
    if connected:
        console.print(f"Already connected to {join_names(connected)}.")
    return await questionary.confirm(
        f"Connect to {join_names(clusters)} now, to check them? There may be a 2FA prompt for "
        "each cluster.",
        default=True,
    ).unsafe_ask_async()


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
        and has_ssh_entry(cluster)
        and get_ssh_options_for_host(cluster).get("serveraliveinterval", "0") == "0"
    ]
    if clusters:
        console.print(
            "💡 Optional: a network can close an SSH connection that stays idle for a long time, "
            "and you would then need `cluv login` (and a 2FA) again. To keep the connections "
            f"open, add these lines to the `Host` entries of {join_names(clusters)} in ~/.ssh/config:"
        )
        console.print("    ServerAliveInterval 120\n    ServerAliveCountMax 5", style="cyan")


def has_ssh_entry(cluster: str) -> bool:
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
