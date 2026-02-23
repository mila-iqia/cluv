import asyncio
import functools
import logging
import subprocess
import sys
import textwrap
from pathlib import Path, PurePosixPath

import milatools.cli
import rich.console
from milatools.utils.local_v2 import LocalV2
from milatools.utils.parallel_progress import (
    AsyncTaskFn,
    ReportProgressFn,
    run_async_tasks_with_progress_bar,
)
from milatools.utils.remote_v2 import (
    RemoteV2,
)

from cluv.cli.login import login
from cluv.config import find_pyproject, get_config

logger = logging.getLogger(__name__)

milatools.cli.console = rich.console.Console(record=True, file=sys.stdout)
console = rich.console.Console()


async def sync(clusters: list[str] = []):
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
    clusters = clusters or get_config().clusters
    # TODO: Figure out which Slurm cluster we're currently on. Assuming mila for now.
    this_cluster = "mila"
    if this_cluster in clusters:
        clusters.remove(this_cluster)

    # Git push first?
    await LocalV2.run_async("git push", hide=False)

    # TODO: Do we raise an error if we fail to connect to a given cluster?
    # TODO: Add an --ignore flag to ignore some clusters?
    console.log(f"[green]Synchronizing with the following clusters:[/green] {clusters}")

    tasks: list[AsyncTaskFn] = []
    task_descriptions: list[str] = []
    remotes = await login(clusters)
    for remote in remotes:
        tasks.append(functools.partial(sync_task_function, remote=remote))
        task_descriptions.append(f"{this_cluster} -> {remote.hostname}")

    await run_async_tasks_with_progress_bar(
        async_task_fns=tasks,
        task_descriptions=task_descriptions,
        overall_progress_task_description="[green]Syncing project",
    )
    return
    # Other approach: Do each step for all clusters before moving to the next step.
    remotes = await login(clusters)

    await install_uv(remotes)
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()
    await clone_project(remotes, project_path)
    await asyncio.gather(
        *(
            remote.run_async(f"bash -l -c 'uv --directory={project_path} sync'")
            for remote in remotes
        )
    )
    if config.results_path:
        await fetch_results(remotes, config.results_path)


async def sync_task_function(
    report_progress: ReportProgressFn,
    remote: RemoteV2,
):
    """Syncs a single cluster, and reports progress using the provided `report_progress` function."""
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()

    def _update_progress(progress: int, status: str, total: int):
        info = textwrap.shorten(status, 50, placeholder="...")
        report_progress(progress=progress, total=total, info=info)

    _update_progress(0, "Logging in", 5)
    remotes = [remote]

    _update_progress(1, "Installing UV", 5)
    await install_uv(remotes)

    _update_progress(3, "Setting up project", 5)
    await clone_project(remotes, project_path)

    _update_progress(4, "Running 'uv sync'", 5)
    await asyncio.gather(
        *(
            remote.run_async(f"bash -l -c 'uv --directory={project_path} sync --quiet'")
            for remote in remotes
        )
    )
    if config.results_path:
        _update_progress(5, "Fetching results", 6)
        await fetch_results(remotes, config.results_path)


async def install_uv(remotes: list[RemoteV2]):
    uv_paths = await asyncio.gather(
        *(
            remote.get_output_async(
                "bash -l -c 'which uv'", warn=True, hide=True, display=False
            )
            for remote in remotes
        )
    )
    uv_paths = [uv_path.strip() for uv_path in uv_paths]
    clusters_without_uv = [
        remote.hostname for remote, uv_path in zip(remotes, uv_paths) if not uv_path
    ]
    logger.info(f"Installing uv on the following clusters: {clusters_without_uv}")
    await asyncio.gather(
        *(
            remote.run_async("curl -LsSf https://astral.sh/uv/install.sh | sh")
            for remote in remotes
            if remote.hostname in clusters_without_uv
        )
    )


async def clone_project(remotes: list[RemoteV2], project_path: PurePosixPath):
    """Setup the project repo on all the remote clusters.

    New idea:
    - Assume GitHub. Push to GitHub if needed. Clone from github on the remotes.
    - Worry about authentication later, just raise an error if need be for now.

    """
    current_git_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD").strip()
    github_repo_url = subprocess.getoutput("git config --get remote.origin.url").strip()

    # TODO: Scp the ~/.git-credentials file if needed?
    # Or configure the config credential-helper to store first?

    # For each cluster where the project isn't cloned yet, clone it.
    _clusters_without_clones_result = await asyncio.gather(
        *(
            remote.run_async(
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
            remote.run_async(f"git clone {github_repo_url} {project_path}")
            for remote in clusters_without_clones
        )
    )

    await asyncio.gather(
        *(
            remote.run_async(f"git -C {project_path} fetch --all --prune")
            for remote in remotes
        )
    )

    # TODO: Look into why the command still has the Controlpath explicitly there, even if the ssh config already has it.
    await asyncio.gather(
        *(
            remote.run_async(
                f"git -C {project_path} checkout {current_git_branch}",
                # warn=True,
                hide=False,
            )
            for remote in remotes
        )
    )
    await asyncio.gather(
        *(remote.run_async(f"git -C {project_path} pull") for remote in remotes)
    )


async def fetch_results(remotes: list[RemoteV2], results_path: str):
    """Fetches results from all remote clusters to the current (mila for now) cluster using rsync."""
    await asyncio.gather(
        *(
            remote.run_async(
                # Use --full-form flags (not -avz) for better readability.
                f"rsync --archive --verbose --compress {remote.hostname}:{results_path} {results_path}",
                warn=True,
                hide=False,
            )
            for remote in remotes
        )
    )
