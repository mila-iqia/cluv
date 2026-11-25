"""`cluv doctor`: run a list of checks on each cluster to diagnose setup issues."""

from __future__ import annotations

import asyncio
import dataclasses
import fnmatch
from enum import Enum
from pathlib import PurePosixPath

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cluv.cache import get_disabled_clusters
from cluv.cli.disable import format_remaining
from cluv.cli.sync import remote_test
from cluv.config import CluvConfig, get_cluv_config
from cluv.remote import Remote, control_socket_is_running, get_ssh_options_for_host
from cluv.ssh import get_ssh_hostnames
from cluv.utils import console, current_cluster, find_pyproject

__all__ = ["doctor"]


class Status(Enum):
    OK = ("✔", "green")
    WARN = ("!", "yellow")
    FAIL = ("✘", "red")
    SKIP = ("-", "dim")

    @property
    def icon(self) -> Text:
        return Text(self.value[0], style=f"bold {self.value[1]}")


@dataclasses.dataclass(frozen=True)
class Check:
    status: Status
    message: str
    hint: str = ""


async def doctor(clusters: list[str]) -> None:
    """Run checks on each cluster and suggest fixes for the ones that fail."""
    config = get_cluv_config()
    clusters = clusters or config.clusters_names
    with console.status(f"Checking {len(clusters)} cluster(s)..."):
        results = await asyncio.gather(*(check_cluster(config, c) for c in clusters))

    for cluster, checks in zip(clusters, results):
        console.print(cluster_panel(cluster, checks))

    worst = [worst_status(checks) for checks in results]
    summary = Text.assemble(
        (f"{worst.count(Status.OK)} healthy", "green"),
        "  ·  ",
        (f"{worst.count(Status.WARN)} with warnings", "yellow"),
        "  ·  ",
        (f"{worst.count(Status.FAIL)} failing", "red"),
    )
    console.rule(summary)
    if Status.FAIL in worst:
        raise SystemExit(1)


def worst_status(checks: list[Check]) -> Status:
    statuses = {check.status for check in checks}
    return next((s for s in (Status.FAIL, Status.WARN) if s in statuses), Status.OK)


def cluster_panel(cluster: str, checks: list[Check]) -> Panel:
    worst = worst_status(checks)
    counts = Text("  ").join(
        Text.assemble(status.icon, f" {n}")
        for status in Status
        if (n := sum(check.status is status for check in checks))
    )
    title = Text.assemble((cluster, "bold"), "  ", counts)

    if worst is Status.OK:
        body = Text.assemble(Status.OK.icon, f" all {len(checks)} checks passed")
    else:
        body = Table.grid(padding=(0, 1))
        body.add_column(width=1)
        body.add_column()
        for check in checks:
            message = Text(check.message, style="dim" if check.status is Status.SKIP else "")
            if check.hint:
                message = Group(message, Text(f"→ {check.hint}", style="dim italic"))
            body.add_row(check.status.icon, message)

    return Panel(body, title=title, title_align="left", border_style=worst.value[1])


async def check_cluster(config: CluvConfig, cluster: str) -> list[Check]:
    if cluster not in config.clusters_names:
        return [
            Check(
                Status.FAIL,
                "not configured in [tool.cluv.clusters]",
                f"add it to pyproject.toml (configured: {', '.join(config.clusters_names)})",
            )
        ]

    checks = [check_ssh_config_entry(cluster), check_ssh_config_options(cluster)]
    if (disabled := get_disabled_clusters().get(cluster)) is not None:
        remaining = (
            "indefinitely"
            if disabled.disabled_until is None
            else f"for {format_remaining(disabled.disabled_until)}"
        )
        checks.append(Check(Status.WARN, f"disabled {remaining}", f"run `cluv enable {cluster}`"))
    checks.append(check_job_script(config, cluster))

    if cluster == current_cluster():
        checks.append(Check(Status.SKIP, "remote checks skipped: this is the current cluster"))
        return checks

    if not await control_socket_is_running(cluster):
        checks.append(
            Check(Status.FAIL, "ControlMaster socket not running", f"run `cluv login {cluster}`")
        )
        checks.append(Check(Status.SKIP, "remote checks skipped: not connected"))
        return checks
    checks.append(Check(Status.OK, "ControlMaster socket is running"))

    remote = Remote(cluster)
    checks += await asyncio.gather(
        check_github_auth(remote),
        check_project_dir(config, remote),
        check_results_path(config, remote),
        check_accounts(config, remote),
    )
    return checks


def check_ssh_config_entry(cluster: str) -> Check:
    patterns = get_ssh_hostnames() - {"*"}
    if any(fnmatch.fnmatch(cluster, pattern) for pattern in patterns):
        return Check(Status.OK, f"~/.ssh/config has an entry for {cluster}")
    return Check(
        Status.FAIL,
        f"~/.ssh/config has no entry for {cluster}",
        f"add a `Host {cluster}` entry with its HostName and User",
    )


def check_ssh_config_options(cluster: str) -> Check:
    options = get_ssh_options_for_host(cluster)
    missing = []
    if options.get("controlmaster") not in ("auto", "yes"):
        missing.append("ControlMaster auto")
    if options.get("serveraliveinterval", "0") == "0":
        missing.append("ServerAliveInterval 60")
    if missing:
        return Check(
            Status.WARN,
            "SSH config could be improved",
            f"add `{'`, `'.join(missing)}` to the `Host {cluster}` entry",
        )
    return Check(Status.OK, "SSH config uses ControlMaster and ServerAliveInterval")


def check_job_script(config: CluvConfig, cluster: str) -> Check:
    job_script = config.get_cluster_config(cluster).job_script_path
    if job_script is None:
        return Check(Status.OK, "no default job_script_path set")
    if (find_pyproject().parent / job_script).is_file():
        return Check(Status.OK, f"job script {job_script} exists")
    return Check(
        Status.FAIL,
        f"job script {job_script} not found in the project",
        "fix `job_script_path` in the config",
    )


async def check_github_auth(remote: Remote) -> Check:
    # `ssh -T git@github.com` always exits with 1, even when authentication succeeds.
    result = await remote.run(
        "ssh -T -o BatchMode=yes -o StrictHostKeyChecking=accept-new git@github.com",
        warn=True,
        hide=True,
        display=False,
    )
    if "successfully authenticated" in result.stderr:
        return Check(Status.OK, "authenticated on GitHub")
    return Check(
        Status.FAIL,
        "not authenticated on GitHub",
        f"add an SSH key from {remote.hostname} to https://github.com/settings/keys, "
        f"or forward your local key with `ForwardAgent yes` in the `Host {remote.hostname}` entry",
    )


async def check_project_dir(config: CluvConfig, remote: Remote) -> Check:
    project_dir = config.get_cluster_config(remote.hostname).project_dir
    if project_dir is None:
        return Check(Status.OK, "project_dir not set (defaults to the local path under $HOME)")
    resolved = await _resolve(remote, project_dir)
    if resolved is None:
        return Check(Status.FAIL, f"project_dir {project_dir} has undefined env vars")
    if await remote_test("-d", resolved, remote):
        return Check(Status.OK, f"project_dir {project_dir} exists")
    return Check(
        Status.WARN,
        f"project_dir {project_dir} doesn't exist yet",
        f"run `cluv sync {remote.hostname}`",
    )


async def check_results_path(config: CluvConfig, remote: Remote) -> Check:
    results_path = config.get_cluster_config(remote.hostname).results_path
    resolved = await _resolve(remote, results_path)
    if resolved is None:
        return Check(Status.FAIL, f"results_path {results_path} has undefined env vars")
    return Check(Status.OK, f"results_path {results_path} resolves to {resolved}")


async def check_accounts(config: CluvConfig, remote: Remote) -> Check:
    accounts = {
        str(args["account"])
        for args in config.get_cluster_config(remote.hostname).sbatch_args
        if args.get("account")
    }
    if not accounts:
        return Check(Status.OK, "no account set in sbatch_args")
    output = await remote.get_output(
        "sacctmgr --noheader --parsable2 show associations user=$USER format=account", warn=True
    )
    available = set(output.split())
    if not available:
        return Check(Status.WARN, "could not list your Slurm accounts with sacctmgr")
    # On DRAC, `def-prof` maps to the `def-prof_cpu` and `def-prof_gpu` associations.
    invalid = sorted(
        a for a in accounts if not any(b == a or b.startswith(f"{a}_") for b in available)
    )
    if invalid:
        return Check(
            Status.FAIL,
            f"account(s) {', '.join(invalid)} not in your allocations on {remote.hostname}",
            f"available: {', '.join(sorted(available))}",
        )
    return Check(Status.OK, f"account(s) {', '.join(sorted(accounts))} are valid")


async def _resolve(remote: Remote, path: PurePosixPath) -> PurePosixPath | None:
    """Expand the env vars in `path` on the remote, or return None if one of them is undefined."""
    if "$" not in str(path):
        return path
    result = await remote.run(
        f"bash --login -c 'set -u; echo {path}'", warn=True, hide=True, display=False
    )
    return PurePosixPath(result.stdout.strip()) if result.returncode == 0 else None
