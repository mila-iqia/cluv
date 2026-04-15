from __future__ import annotations

import asyncio
import functools
import logging
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
from cluv.config import find_pyproject, get_config
from cluv.remote import Remote, get_ssh_options_for_host, run
from cluv.utils import console, current_cluster

milatools.cli.console = console
milatools.utils.parallel_progress.console = console
logger = logging.getLogger(__name__)


# TODO: Control the 'hide' and 'display' / etc using the --verbose flag value, in addition to the loglevel.
# TODO: Pipe the commands and their outputs / stderr to separate files for each cluster, so people can easily inspect
# what might have gone wrong. Also include a message at the end like "Check <logs_dir>/{cluster}.log for details."


async def sync(
    clusters: list[str] | None = None, uv_sync_args: list[str] | None = None
) -> list[Remote]:
    """Synchronizes the current project across clusters.

    - Synchronizes code across all clusters.
    - Gathers results on the "main" cluster (mila)
    - Does `uv sync` that cluster as well
        - (Important so that jobs can be run in OFFLINE mode)

    ## How it could work (proof-of-concept)
    - Checks git state
    - Push to github
        - TODO: Check syncing without github.
    - Over SSH, does a git fetch on all remote clusters
    - Gathers results from all other clusters to the Mila cluster using rsync.
    """
    # TODO: Figure out which Slurm cluster we're currently on. Assuming mila for now.
    this_cluster = current_cluster()
    # When no cluster is passed, sync with clusters for which we have an active SSH connection.
    if not clusters:
        clusters = get_config().clusters
        if this_cluster and this_cluster in clusters:
            clusters.remove(this_cluster)
        connections = await asyncio.gather(
            *(get_remote_without_2fa_prompt(cluster) for cluster in clusters)
        )
        remotes = [conn for conn in connections if conn]
        if not remotes:
            console.log(
                "[red]Not currently connected to any Slurm cluster.[/red] "
                "Use `cluv login` to login and create reusable connections."
            )
            return []
        clusters = [remote.hostname for remote in remotes]
    else:
        remotes = await login(clusters)

    # Git push first?
    await run(("git", "push"), hide=False)

    # TODO: Do we raise an error if we fail to connect to a given cluster?
    # TODO: Add an --ignore flag to ignore some clusters?
    console.log(f"[green]Synchronizing with the following clusters:[/green] {clusters}")

    tasks: list[AsyncTaskFn] = []
    task_descriptions: list[str] = []
    for remote in remotes:
        tasks.append(functools.partial(sync_task_function, remote=remote))
        task_descriptions.append(f"{this_cluster or 'local'} -> {remote.hostname}")

    await run_async_tasks_with_progress_bar(
        async_task_fns=tasks,
        task_descriptions=task_descriptions,
        overall_progress_task_description="[green]Syncing project",
    )
    return remotes


async def sync_task_function(
    report_progress: ReportProgressFn,
    remote: Remote,
):
    """Syncs a single cluster, and reports progress using the provided `report_progress` function."""
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()

    def _update_progress(progress: int, status: str, total: int):
        info = textwrap.shorten(status, 50, placeholder="...")
        report_progress(progress=progress, total=total, info=info)

    num_tasks = 4 if config.results_path else 3

    _update_progress(0, "Checking/Installing UV", num_tasks)
    await install_uv(remote)

    _update_progress(1, "Setting up project", num_tasks)
    await clone_project(remote, project_path)

    _update_progress(2, "Running 'uv sync'", num_tasks)
    await remote.run(f"bash -l -c 'uv --directory={project_path} sync --quiet'")

    if config.results_path:
        _update_progress(3, "Fetching results", num_tasks)
        await fetch_results(remote, config.results_path)

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


async def clone_project(remote: Remote, project_path: PurePosixPath):
    """Setup the project repo on all the remote clusters.

    New idea:
    - Assume GitHub. Push to GitHub if needed. Clone from github on the remotes.
    - Worry about authentication later, just raise an error if need be for now.

    """
    # TODO: This git info is shared, but currently repeatedly executed for each cluster.
    # Could be done only once.
    current_git_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD").strip()
    git_remote_name = subprocess.getoutput(
        f"git config --get branch.{current_git_branch}.remote"
    ).strip()
    github_repo_url = subprocess.getoutput(
        f"git config --get remote.{git_remote_name}.url"
    ).strip()

    # TODO: Scp the ~/.git-credentials file if needed?
    # Or configure the config credential-helper to store first?

    # If the project isn't cloned yet, clone it.
    _is_cloned_on_cluster = (
        await remote.run(
            f"test -d {project_path}",
            warn=True,
            hide=True,
            display=False,
        )
    ).returncode == 0
    if not _is_cloned_on_cluster:
        logger.debug(f"Project isn't cloned yet on {remote.hostname}.")
    await remote.run(f"git clone {github_repo_url} {project_path}", hide=True)
    await remote.run(f"git -C {project_path} fetch --all --prune", hide=True)
    await remote.run(f"git -C {project_path} checkout {current_git_branch}", hide=False)
    await remote.run(f"git -C {project_path} pull")


async def fetch_results(remote: Remote, results_path: Path | str):
    """Fetches results from all remote clusters to the current (mila for now) cluster using rsync."""
    results_path = Path(results_path)
    assert not results_path.is_absolute()
    project_dir = find_pyproject().parent

    results_path_relative_to_home = (project_dir / results_path).relative_to(Path.home())

    # TODO: to simplify, for now we assume that the results are stored in a directory directly under the project directory.
    # A directory with the same name (e.g. logs) is created in $SCRATCH.
    # This could cause some confusion if there are multiple projects with a `logs` directory, since we'd see the logs
    # from different projects in the same place. To fix this, for now we use `$SCRATCH/logs/{project_name}` as the `logs` dir.

    # Create the results directory if it doesn't exist.
    # TODO: Create that result directory as a symlink to a dir in $SCRATCH?

    results_path.mkdir(parents=True, exist_ok=True)

    await create_results_dir_with_symlink_to_scratch(remote, results_path)
    await run(
        # Using --full-form flags (not -avz) for better readability.
        (
            "rsync",
            "--archive",
            "--verbose",
            "--compress",
            "--copy-links",
            f"{remote.hostname}:{results_path_relative_to_home}",
            str((Path.home() / results_path_relative_to_home).parent),
            # shlex.split(
            #     f"rsync --archive --verbose --compress --copy-links "
            #     f"{remote.hostname}:{results_path_relative_to_home} {(Path.home() / results_path_relative_to_home).parent}"
            # )
        ),
        warn=True,
        hide=False,
    )


async def create_results_dir_with_symlink_to_scratch(remote: Remote, results_path: Path):
    """On the remote, symlink ~/<project>/<results_path> -> $SCRATCH/<results_path>/<project_name>.

    This keeps large outputs out of $HOME and in $SCRATCH where storage limits are more generous.
    """
    project_dir = find_pyproject().parent
    project_dir_relative_to_home = project_dir.relative_to(Path.home())
    symlink_path = project_dir_relative_to_home / results_path

    # On some clusters (e.g. Vulcan), $SCRATCH is only defined in login shells.
    scratch = (
        await remote.get_output("bash -l -c 'echo $SCRATCH'", hide=True, warn=True, display=False)
    ).strip()
    if not scratch:
        logger.warning(f"Remote {remote.hostname} does not have $SCRATCH defined.")
        return

    scratch_dir = f"{scratch}/{results_path}/{project_dir.name}"

    # Create the target directory in $SCRATCH if it doesn't already exist.
    if not await remote_test("-d", scratch_dir, remote):
        result = await remote.run(f"mkdir -p {scratch_dir}", warn=True, hide=True)
        if result.returncode != 0:
            logger.warning(
                f"Failed to create {scratch_dir} on {remote.hostname}. "
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
            f"{symlink_path} on {remote.hostname} is a real directory, not a symlink to $SCRATCH. "
            f"You may end up filling up $HOME. Consider replacing it with a symlink to {scratch_dir}."
        )
        return

    # Nothing at the path yet — create the symlink.
    result = await remote.run(
        f"ln -s -T {scratch_dir} {symlink_path}",
        warn=True,
        hide=True,
    )
    if result.returncode != 0:
        logger.warning(
            f"Failed to create symlink {symlink_path} -> {scratch_dir} on {remote.hostname}."
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
