from __future__ import annotations

import asyncio
import functools
import logging
import os
import re
import shlex
import shutil
import subprocess
import textwrap
from pathlib import Path, PurePosixPath
from typing import Literal

# TODO: Figure out what the issues are with the console output
import milatools.cli
import milatools.utils.parallel_progress

# Reuse some code milatools. Could also extract it here to remove the dependency.
from milatools.utils.parallel_progress import (
    AsyncTaskFn,
    ReportProgressFn,
    run_async_tasks_with_progress_bar,
)

from cluv.cli.login import get_remote_without_2fa_prompt, login
from cluv.config import (
    CluvConfig,
    find_pyproject,
    get_config,
)
from cluv.job import get_datasets_path
from cluv.remote import Remote, get_ssh_options_for_host, run
from cluv.utils import console, console_lock, current_cluster

milatools.cli.console = console
milatools.utils.parallel_progress.console = console
logger = logging.getLogger(__name__)

__all__ = ["sync", "install_uv", "clone_project", "fetch_results"]


# TODO: Control the 'hide' and 'display' / etc using the --verbose flag value, in addition to the loglevel.
# TODO: Pipe the commands and their outputs / stderr to separate files for each cluster, so people can easily inspect
# what might have gone wrong. Also include a message at the end like "Check <logs_dir>/{cluster}.log for details."


async def sync(
    clusters: list[str] | None = None,
    uv_sync_args: list[str] | None = None,
    sync_datasets: bool = True,
) -> list[Remote]:
    """Synchronizes the current project across clusters.

    - Synchronizes code across all clusters.
    - Gathers results on the "main" cluster (mila)
    - Does `uv sync` that cluster as well
        - (Important so that jobs can be run in OFFLINE mode)

    Parameters:
        clusters: List of SSH hostnames of the target clusters. If empty, will attempt to sync
            with all clusters in the config that we have an active SSH connection to.

    Returns:
        A list of Remote objects corresponding to the clusters that were synced with.

    How it could work (proof-of-concept)
    - Checks git state
    - Push to github
        - TODO: Check syncing without github.
    - Over SSH, does a git fetch on all remote clusters
    - Gathers results from all other clusters to the Mila cluster using rsync.
    """
    # TODO: Figure out which Slurm cluster we're currently on. Assuming mila for now.
    here = current_cluster()
    if clusters and here in clusters:
        clusters.remove(here)

    # When no cluster is passed, sync with clusters for which we have an active SSH connection.
    all_remotes = await get_active_remotes()
    if clusters:
        remotes = await login(clusters)
    elif not all_remotes:
        raise RuntimeError(
            "[red]Not currently connected to any Slurm cluster.[/red] "
            "Use `cluv login` to login and create reusable connections."
        )
    else:
        remotes = all_remotes.copy()
        clusters = [remote.hostname for remote in all_remotes]

    if "GITHUB_ACTIONS" not in os.environ:
        # NOTE: Skip this step in the GitHub CI, since the commit is already pushed (and we have errors).
        await run(("git", "push"), hide=False)

    # TODO: Do we raise an error if we fail to connect to a given cluster?
    # TODO: Add an --ignore flag to ignore some clusters?
    console.log(f"[green]Synchronizing with the following clusters:[/green] {clusters}")

    tasks: list[AsyncTaskFn] = []
    task_descriptions: list[str] = []
    for remote in remotes:
        tasks.append(functools.partial(sync_task_function, remote=remote))
        task_descriptions.append(f"{here or 'local'} -> {remote.hostname}")

    config = get_config()

    token = console_lock.set(asyncio.Lock())
    if (
        sync_datasets
        and config.data_source  # cluster:path
        and (source_cluster := config.data_source.split(":", 1)[0]) != here
    ):
        _source_host, _, source_path = config.data_source.partition(":")
        # Fetch the data from the source cluster and copy it to the local datasets_path.
        source_remote = next((r for r in all_remotes if r.hostname == source_cluster), None)
        if not source_remote:
            raise RuntimeError(
                f"[red]Unable to sync datasets, need a connection to the source cluster "
                f"({source_cluster})[/red]. Current connections: {[r.hostname for r in all_remotes]}\n"
                f"Use `cluv login {source_cluster}` to create a reusable connection to the "
                f"source cluster."
            )
        local_datasets_path = get_datasets_path()
        if not local_datasets_path:
            raise RuntimeError(
                "`cluv.datasets_path` must be set in the Cluv config section of pyproject.toml to "
                "sync datasets between clusters."
            )
        await _pull_datasets(source_remote, source_path, local_datasets_path)

    await run_async_tasks_with_progress_bar(
        async_task_fns=tasks,
        task_descriptions=task_descriptions,
        overall_progress_task_description="[green]Syncing project",
    )
    console_lock.reset(token)

    return remotes


async def get_active_remotes() -> list[Remote]:
    """Returns the Remotes for each cluster which has an active SSH connection."""
    clusters = get_config().clusters_names
    connections = await asyncio.gather(
        *(get_remote_without_2fa_prompt(cluster) for cluster in clusters)
    )
    remotes = [conn for conn in connections if conn]  # keep the active connections.
    return remotes


async def sync_task_function(report_progress: ReportProgressFn, remote: Remote):
    """Syncs a single cluster, and reports progress using the provided `report_progress` function."""
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()

    def _update_progress(progress: int, status: str, total: int):
        info = textwrap.shorten(status, 50, placeholder="...")
        report_progress(progress=progress, total=total, info=info)

    num_tasks = 5 if config.data_source else 4

    _update_progress(0, "Checking/Installing UV", num_tasks)
    await install_uv(remote)

    _update_progress(1, "Setting up project", num_tasks)
    await clone_project(remote)

    _update_progress(2, "Running 'uv sync'", num_tasks)
    await remote.run(f"bash --login -c 'uv --directory={project_path} sync --quiet'")

    _update_progress(3, "Fetching results", num_tasks)
    await fetch_results(remote, config)

    if config.data_source:
        _update_progress(4, "Syncing datasets", num_tasks)
        here = current_cluster()
        local_dataset_path = (config.get_cluster_config(here) if here else config).datasets_path
        if not local_dataset_path:
            raise RuntimeError("data_source is set, so dataset_path should also be set!")
        local_dataset_path = Path(os.path.expandvars(local_dataset_path))

        await _push_datasets_to_remote(local_dataset_path, remote, config)

    _update_progress(num_tasks, "Done", num_tasks)


async def install_uv(remote: Remote):
    # todo: These parts are common. No need to do them for each cluster. Not a big deal though.
    if not shutil.which("uv"):
        logger.error(
            "`uv` is not installed on this machine. Please install `uv` to ensure it's installed on the remote clusters as well."
        )
        # TODO: Do we want to just install it for them instead? (we already do it on the clusters, why not?)
        raise RuntimeError("`uv` is not installed on this machine.")

    # Get the version of `uv` used here, and install the same version everywhere.
    uv_version_here = (
        # uv --version outputs e.g. 'uv 0.11.0 (aarch64-unknown-linux-gnu)'.
        subprocess.getoutput("uv --version").strip().split()[1]
    )
    logger.info(
        f"[green]Using uv version {uv_version_here} everywhere, since this is the version on this machine.[/green]"
    )

    uv_path = await remote.get_output("bash -l -c 'which uv'", warn=True, hide=True, display=False)
    uv_path = uv_path.strip()
    cluster_doesnt_have_uv = not uv_path
    if cluster_doesnt_have_uv:
        logger.info(f"Installing uv on {remote.hostname}.")
        await remote.run("curl -LsSf https://astral.sh/uv/install.sh | sh")

    uv_version = await remote.get_output("bash -l -c 'uv --version'", hide=True, display=False)
    uv_version = uv_version.strip().split()[1]

    uv_version_is_different = uv_version.strip() != uv_version_here
    if uv_version_is_different:
        logger.info(f"Updating uv to version {uv_version_here} on the {remote.hostname} cluster.")
        await remote.run(f"bash -l -c 'uv self update {uv_version_here}'", hide=True)


async def clone_project(remote: Remote):
    """Setup the project repo on all the remote clusters.

    New idea:
    - Assume GitHub. Push to GitHub if needed. Clone from github on the remotes.
    - Worry about authentication later, just raise an error if need be for now.
    """
    # TODO: This git info is shared, but currently repeatedly executed for each cluster.
    # Could be done only once.
    current_git_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD").strip()
    safe_current_git_branch = shlex.quote(current_git_branch)
    detached_head = current_git_branch == "HEAD"
    if not detached_head:
        try:
            git_remote_name = subprocess.check_output(
                ["git", "config", "--get", f"branch.{current_git_branch}.remote"],
                text=True,
            ).strip()
        except subprocess.CalledProcessError:
            git_remote_name = ""
    else:
        git_remote_name = "origin"
    if not git_remote_name:
        git_remote_name = "origin"
    github_repo_url = subprocess.getoutput(
        f"git config --get remote.{git_remote_name}.url"
    ).strip()
    if not github_repo_url:
        raise RuntimeError(
            f"Could not determine Git remote URL from remote '{git_remote_name}'. "
            "Make sure your git remote is configured."
        )

    # TODO: Scp the ~/.git-credentials file if needed?
    # Or configure the config credential-helper to store first?

    # Get the path to the root of the git repository
    git_root_path = PurePosixPath(
        subprocess.getoutput("git rev-parse --show-toplevel").strip()
    ).relative_to(Path.home())

    # If the project isn't cloned yet, clone it.
    _is_cloned_on_cluster = (
        await remote.run(
            f"test -d {git_root_path}",
            warn=True,
            hide=True,
            display=False,
        )
    ).returncode == 0
    if not _is_cloned_on_cluster:
        logger.debug(f"Project isn't cloned yet on {remote.hostname}.")
        await remote.run(f"git clone {github_repo_url} {git_root_path}", hide=True)
    await remote.run(f"git -C {git_root_path} fetch --all --prune", hide=True)
    if detached_head:
        github_head_ref = os.environ.get("GITHUB_HEAD_REF", "").strip()
        if github_head_ref:
            if (
                not re.fullmatch(r"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*", github_head_ref)
                or ".." in github_head_ref
            ):
                raise RuntimeError(f"Invalid GITHUB_HEAD_REF value: {github_head_ref!r}")
            safe_head_ref = shlex.quote(github_head_ref)
            safe_tracking_ref = shlex.quote(f"{git_remote_name}/{github_head_ref}")
            safe_remote_name = shlex.quote(git_remote_name)
            await remote.run(
                f"git -C {git_root_path} checkout -B {safe_head_ref} {safe_tracking_ref}",
                hide=False,
            )
            await remote.run(
                f"git -C {git_root_path} pull {safe_remote_name} {safe_head_ref}", hide=False
            )
            return
        current_git_commit = subprocess.getoutput("git rev-parse HEAD").strip()
        safe_current_git_commit = shlex.quote(current_git_commit)
        await remote.run(
            f"git -C {git_root_path} checkout --detach {safe_current_git_commit}", hide=False
        )
    else:
        await remote.run(f"git -C {git_root_path} checkout {safe_current_git_branch}", hide=False)
        await remote.run(f"git -C {git_root_path} pull", hide=False)


async def _pull_datasets(source_remote: Remote, source_path: str, local_datasets_path: Path):
    """Pull from source to the locally-resolved datasets_path."""
    # Resolve the env vars on the remote.
    source_host = source_remote.hostname
    source_path = await source_remote.get_output(f"echo {source_path}")
    if "$" in str(local_datasets_path):
        # Important to stop here if there is $SCRATCH in the datasets_path and it is not set on
        # this machine.
        raise RuntimeError(
            f"Cannot resolve datasets_path '{local_datasets_path}' on this machine: "
            f"there are unknown environment variables in the path.\n"
            f"To avoid copying the datasets from {source_remote.hostname} to this machine, run "
            f"`cluv sync` from {source_remote.hostname}, or use the "
            f"`--no-sync-datasets` flag when running `uv sync` from this machine."
        )

    local_datasets_path.mkdir(parents=True, exist_ok=True)
    console.log(
        f"[green]Pulling datasets:[/green] {source_host}:{source_path} -> {local_datasets_path}"
    )
    source_path = await source_remote.get_output(f"echo {source_path}")
    await run(
        (
            "rsync",
            "--archive",
            "--verbose",
            "--compress",
            "--copy-links",
            "--chmod=u+w",
            "--exclude=.git",
            "--exclude=.datalad",
            f"{source_host}:{source_path}/",
            f"{local_datasets_path}/",
        ),
        _display=True,
    )


async def _push_datasets_to_remote(local_source: Path, remote: Remote, config: CluvConfig):
    """Push dataset from a local path to the remote cluster's datasets_path."""
    datasets_path_template = str(config.get_cluster_config(remote.hostname).datasets_path)
    resolved_path = (
        await remote.get_output(
            f"bash -l -c 'echo {datasets_path_template}'", hide=True, display=False
        )
    ).strip()
    await remote.run(f"mkdir -p {resolved_path}", hide=True)
    await run(
        (
            "rsync",
            "--archive",
            "--verbose",
            "--compress",
            "--copy-links",
            "--chmod=u+w",
            "--exclude=.git",
            "--exclude=.datalad",
            f"{local_source}/",
            f"{remote.hostname}:{resolved_path}/",
        ),
        _display=True,
    )


async def fetch_results(remote: Remote, config: CluvConfig):
    """Fetches results from a remote cluster to local using rsync via the results symlink."""
    results_path_here = Path(os.path.expandvars(config.results_path))
    results_path_here.mkdir(parents=True, exist_ok=True)
    # Keep it as a string since it might contain env vars that have to be resolved on the remote.
    results_path_on_cluster = str(config.get_cluster_config(remote.hostname).results_path)
    results_path_on_cluster = remote.get_output(
        f"echo {results_path_on_cluster}", hide=False, display=True
    )
    await run(
        (
            "rsync",
            "--archive",
            "--verbose",
            "--compress",
            "--copy-links",
            "--chmod=u+w",
            f"{remote.hostname}:{results_path_on_cluster}",
            str(results_path_here),
        ),
        warn=True,
        hide=False,
    )


async def create_results_dir_with_symlink_to_scratch(
    remote: Remote, results_symlink: str, results_path: str
):
    """On the remote, create results_path and symlink project/<results_symlink> -> results_path.

    results_path may contain env vars (e.g. $SCRATCH); they are resolved via the remote login shell.
    """
    project_dir = find_pyproject().parent
    project_dir_relative_to_home = project_dir.relative_to(Path.home())
    symlink_path = project_dir_relative_to_home / results_symlink

    # Resolve env vars (e.g. $SCRATCH) in results_path using the remote login shell.
    resolved_path = (
        await remote.get_output(
            f"bash --login -c 'echo {results_path}'", hide=True, warn=True, display=False
        )
    ).strip()
    if not resolved_path:
        logger.warning(
            f"Could not resolve results_path '{results_path}' on {remote.hostname}. Skipping symlink."
        )
        return

    # Create the target directory if it doesn't already exist.
    if not await remote_test("-d", resolved_path, remote):
        result = await remote.run(f"mkdir -p {resolved_path}", warn=True, hide=True)
        if result.returncode != 0:
            logger.warning(
                f"Failed to create {resolved_path} on {remote.hostname}. "
                f"Results will be stored in {symlink_path}, which may fill up $HOME."
            )
            await remote.run(f"mkdir -p {symlink_path}", warn=True, hide=True)
            return

    # If a symlink already exists at the path (valid or broken), nothing to do.
    if await remote_test("-L", symlink_path, remote):
        return

    # If a real file/directory exists there, warn — the user may be filling up $HOME.
    if await remote_test("-e", symlink_path, remote):
        logger.warning(
            f"{symlink_path} on {remote.hostname} is a real directory, not a symlink. "
            f"You may end up filling up $HOME. Consider replacing it with a symlink to {resolved_path}."
        )
        return

    # Nothing at the path yet — create the symlink.
    result = await remote.run(
        f"ln -s -T {resolved_path} {symlink_path}",
        warn=True,
        hide=True,
    )
    if result.returncode != 0:
        logger.warning(
            f"Failed to create symlink {symlink_path} -> {resolved_path} on {remote.hostname}."
        )


async def remote_test(flag: Literal["-d", "-e", "-L"], path: str | Path, remote: Remote) -> bool:
    """Returns True if `test {flag} {path}` succeeds on the remote."""
    result = await remote.run(f"test {flag} {path}", warn=True, hide=True)
    return result.returncode == 0


def get_loglevel():
    return logging.getLogger("cluv").getEffectiveLevel()


async def host_uses_controlmaster(hostname: str) -> bool:
    applied_options_for_host = get_ssh_options_for_host(hostname)
    return applied_options_for_host.get("controlmaster", "no").lower() != "no"
