from __future__ import annotations

import argparse
import asyncio
import functools
import logging
import shutil
import subprocess
import textwrap
from pathlib import Path, PurePosixPath
from typing import Literal

import rich_argparse

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

logger = logging.getLogger(__name__)


# TODO: Control the 'hide' and 'display' / etc using the --verbose flag value, in addition to the loglevel.
# TODO: Pipe the commands and their outputs / stderr to separate files for each cluster, so people can easily inspect
# what might have gone wrong. Also include a message at the end like "Check <logs_dir>/{cluster}.log for details."


def add_sync_args(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    cluster_choices = get_config().clusters
    sync_parser = subparsers.add_parser(
        "sync",
        help="Synchronizes the current project across clusters.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    sync_parser.add_argument(
        "clusters",
        choices=cluster_choices if cluster_choices else None,
        nargs="*",
        default=None,
        metavar="<cluster>",
        help=(
            "The cluster(s) to synchronize with. "
            "Leave empty to synchronize with all currently logged in clusters. "
            "Use a comma to separate multiple clusters."
        ),
    )
    # TODO: Try to add a 'remainder' arg to pass extra args to `uv sync` on the remote cluster, but it seems to be a bit tricky.
    # sync_parser.add_argument(
    #     "--",
    #     dest="_",
    #     # type=str,
    #     # help="The arguments to pass to `uv sync` on the remote cluster.",
    #     # dest=argparse.SUPPRESS,
    # )
    # sync_parser.add_argument(
    #     "--",
    #     dest="uv_sync_args",
    #     # type=str,
    #     # metavar="<uv sync arguments>",
    #     help="The arguments to pass to `uv sync` on the remote cluster.",
    #     nargs=argparse.REMAINDER,
    # )
    sync_parser.set_defaults(func=sync)
    return sync_parser


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
    # Other approach: Do each step for all clusters before moving to the next step.
    remotes = await login(clusters)

    await install_uv(remotes)
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()
    await clone_project(remotes, project_path)
    await asyncio.gather(
        *(remote.run(f"bash -l -c 'uv --directory={project_path} sync'") for remote in remotes)
    )
    if config.results_path:
        await fetch_results(remotes, config.results_path)


async def sync_task_function(
    report_progress: ReportProgressFn,
    remote: Remote,
):
    """Syncs a single cluster, and reports progress using the provided `report_progress` function."""
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()
    remotes = [remote]

    def _update_progress(progress: int, status: str, total: int):
        info = textwrap.shorten(status, 50, placeholder="...")
        report_progress(progress=progress, total=total, info=info)

    num_tasks = 4 if config.results_path else 3

    _update_progress(0, "Checking/Installing UV", num_tasks)
    await install_uv(remotes)

    _update_progress(1, "Setting up project", num_tasks)
    await clone_project(remotes, project_path)

    _update_progress(2, "Running 'uv sync'", num_tasks)
    await asyncio.gather(
        *(
            remote.run(f"bash -l -c 'uv --directory={project_path} sync --quiet'")
            for remote in remotes
        )
    )
    if config.results_path:
        _update_progress(3, "Fetching results", num_tasks)
        await fetch_results(remotes, config.results_path)

    _update_progress(num_tasks, "Done", num_tasks)


async def install_uv(remotes: list[Remote]):
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

    uv_paths = await asyncio.gather(
        *(
            remote.get_output("bash -l -c 'which uv'", warn=True, hide=True, display=False)
            for remote in remotes
        )
    )
    uv_paths = [uv_path.strip() for uv_path in uv_paths]
    clusters_without_uv = [
        remote.hostname for remote, uv_path in zip(remotes, uv_paths) if not uv_path
    ]
    if clusters_without_uv:
        logger.info(f"Installing uv on the following clusters: {clusters_without_uv}")
    await asyncio.gather(
        *(
            remote.run("curl -LsSf https://astral.sh/uv/install.sh | sh")
            for remote in remotes
            if remote.hostname in clusters_without_uv
        )
    )
    uv_versions = await asyncio.gather(
        *(
            remote.get_output("bash -l -c 'uv --version'", hide=True, display=False)
            for remote in remotes
        )
    )
    uv_versions = [uv_version.strip().split()[1] for uv_version in uv_versions]
    remotes_with_different_uv_versions = [
        remote
        for remote, version in zip(remotes, uv_versions)
        if version.strip() != uv_version_here
    ]
    if remotes_with_different_uv_versions:
        logger.info(
            f"Updating uv to version {uv_version_here} on the following clusters: {[remote.hostname for remote in remotes_with_different_uv_versions]}"
        )
        await asyncio.gather(
            *(
                remote.run(f"bash -l -c 'uv self update {uv_version_here}'", hide=True)
                for remote in remotes_with_different_uv_versions
            )
        )


async def clone_project(remotes: list[Remote], project_path: PurePosixPath):
    """Setup the project repo on all the remote clusters.

    New idea:
    - Assume GitHub. Push to GitHub if needed. Clone from github on the remotes.
    - Worry about authentication later, just raise an error if need be for now.

    """
    current_git_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD").strip()
    git_remote_name = subprocess.getoutput(
        f"git config --get branch.{current_git_branch}.remote"
    ).strip()
    github_repo_url = subprocess.getoutput(
        f"git config --get remote.{git_remote_name}.url"
    ).strip()

    # TODO: Scp the ~/.git-credentials file if needed?
    # Or configure the config credential-helper to store first?

    # For each cluster where the project isn't cloned yet, clone it.
    _clusters_without_clones_result = await asyncio.gather(
        *(
            remote.run(
                f"test -d {project_path}",
                warn=True,
                hide=True,
                display=False,
            )
            for remote in remotes
        )
    )
    clusters_without_clones = [
        remote
        for remote, result in zip(remotes, _clusters_without_clones_result)
        if result.returncode != 0
    ]
    logger.debug(
        f"Clusters where the project isn't cloned yet: {[remote.hostname for remote in clusters_without_clones]}   "
    )
    await asyncio.gather(
        *(
            remote.run(f"git clone {github_repo_url}.git {project_path}", hide=True)
            for remote in clusters_without_clones
        )
    )

    await asyncio.gather(
        *(
            remote.run(f"git -C {project_path} fetch --all --prune", hide=True)
            for remote in remotes
        )
    )

    # TODO: Look into why the command still has the Controlpath explicitly there, even if the ssh config already has it.
    await asyncio.gather(
        *(
            remote.run(
                f"git -C {project_path} checkout {current_git_branch}",
                # warn=True,
                hide=False,
            )
            for remote in remotes
        )
    )
    await asyncio.gather(*(remote.run(f"git -C {project_path} pull") for remote in remotes))


async def fetch_results(remotes: list[Remote], results_path: Path | str):
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

    await asyncio.gather(
        *(create_results_dir_with_symlink_to_scratch(remote, results_path) for remote in remotes)
    )

    await asyncio.gather(
        *(
            run(
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
            for remote in remotes
        )
    )


async def create_results_dir_with_symlink_to_scratch(remote: Remote, results_path: Path):
    project_dir = find_pyproject().parent
    project_dirname = project_dir.name
    project_dir_relative_to_home = project_dir.relative_to(Path.home())
    _results_dir_relative_to_project = str(results_path)
    # On some clusters (for example Vulcan), $SCRATCH is only defined after the .bashrc and such are loaded (login shells).
    # This is why we have the `bash -c -l` surrounding the command.
    scratch = (
        await remote.get_output("bash -c -l 'echo $SCRATCH'", hide=True, warn=True, display=False)
    ).strip()
    if not scratch:
        logger.warning(
            f"[orange]Remote {remote.hostname} does not have $SCRATCH defined?![/orange]"
        )
        return

    if await test("-d", f"{scratch}/{results_path}/{project_dirname}", remote):
        result = await remote.run(
            f"mkdir -p {scratch}/{results_path}/{project_dirname}", warn=True, hide=True
        )
        if result.returncode != 0:
            logger.warning(
                f"[orange]Failed to create directory {scratch}/{results_path}/{project_dirname} on {remote.hostname}.\n"
                f"Results will be saved in the project directory ({project_dir}/{results_path}) which might not be ideal![/orange]"
            )
            await remote.run(f"mkdir -p {project_dir_relative_to_home}/{results_path}", warn=True)
            return
    # Check if {project_dir}/{results_path} exists.
    # If it doesn't exist, create a symlink.

    if not await test("-e", project_dir_relative_to_home / results_path, remote):
        # Doesn't exist, create a symlink.
        await remote.run(
            f"ln -s -T {scratch}/{results_path}/{project_dirname} "
            f"{project_dir_relative_to_home}/{results_path}",
            # TODO: Still getting an error that the link exists. Weird.
            warn=True,
        )
        return

    # It does exist. Is it a symlink? If not, warn the user, they might be filling up their HOME without realizing it!
    if not await test("-L", project_dir_relative_to_home / results_path, remote):
        logger.warning(
            f"[red]{project_dir_relative_to_home / results_path} on {remote.hostname} (the output directory) "
            f"is not a symlink to $SCRATCH!\n"
            f"Please beware that you might quickly end up filling up your $HOME! Consider instead creating a symlink to scratch![/red]"
        )
        return


async def file_exists(remote: Remote, path: str | Path) -> bool:
    return await test("-e", path, remote)


async def test(type: Literal["-d", "-e", "-L"], path: str | Path, remote: Remote):
    """Returns whether `ssh test {type} {path}` success on the remote."""
    result = await remote.run(f"test {type} {path}", warn=True, hide=True)
    return result.returncode == 0


def get_loglevel():
    return logging.getLogger("cluv").getEffectiveLevel()


async def host_uses_controlmaster(hostname: str) -> bool:
    applied_options_for_host = get_ssh_options_for_host(hostname)
    return applied_options_for_host.get("controlmaster", "no").lower() != "no"
