import asyncio
import functools
import logging
import subprocess
import sys
import textwrap
from pathlib import Path, PurePosixPath

import milatools.cli
import rich.console
from cluv.config import find_pyproject, get_config
from milatools.utils.local_v2 import LocalV2
from milatools.utils.parallel_progress import (
    AsyncTaskFn,
    ReportProgressFn,
    run_async_tasks_with_progress_bar,
)
from milatools.utils.remote_v2 import (
    RemoteV2,
    control_socket_is_running_async,
    get_controlpath_for,
)

logger = logging.getLogger(__name__)

milatools.cli.console = rich.console.Console(record=True, file=sys.stdout)


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
    if "mila" in clusters:
        clusters.remove("mila")

    # TODO: Do we raise an error if we fail to connect to a given cluster?
    # TODO: Add an --ignore flag to ignore some clusters?
    logger.info(f"[green]Synchronizing with the following clusters:[/green] {clusters}")

    tasks: list[AsyncTaskFn] = []
    task_descriptions: list[str] = []

    this_cluster = "mila"  # TODO
    for cluster in clusters:
        tasks.append(functools.partial(sync_task_function, cluster=cluster))
        task_descriptions.append(f"{this_cluster} -> {cluster}")

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
    cluster: str,
):
    """Syncs a single cluster, and reports progress using the provided `report_progress` function."""
    project_path = PurePosixPath(find_pyproject().parent.relative_to(Path.home()))
    config = get_config()

    def _update_progress(progress: int, status: str, total: int):
        info = textwrap.shorten(status, 50, placeholder="...")
        report_progress(progress=progress, total=total, info=info)

    _update_progress(0, "Logging in", 5)
    clusters = [cluster]
    remotes = await login(clusters)

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


async def login(clusters: list[str]) -> list[RemoteV2]:
    """Create an SSH connection with the given clusters, reusing existing connections when possible to avoid triggering 2FA prompts."""
    connections = await asyncio.gather(
        *(_get_remote_without_2fa_prompt(cluster) for cluster in clusters)
    )
    # For any cluster we don't have an active connection to, connect
    logger.info(
        f"Already connected to the following clusters: {[remote.hostname for remote in connections if remote]}"
    )
    logger.info(
        f"Will attempt to connect to the following clusters: {[cluster for cluster, remote in zip(clusters, connections) if not remote]}"
    )
    # Need to do each thing sequentially to avoid triggering multiple 2FA prompts at the same time.
    return [
        remote if remote is not None else (await RemoteV2.connect(cluster))
        for cluster, remote in zip(clusters, connections)
    ]


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

    IDEA:
    - Setup a bare git repo on each cluster at {project_path}.git
    - Setup a git remote on this machine for each cluster pointing to that new bare repo.
    - When running jobs, push to the cluster's bare repo
    Then, since we also want the code to be materialized on the remote clusters at {project_path},
    not just in a bare repo, we can do one of the following:
    - Setup a post-receive hook that checks out the code to {project_path}, or
    - Just do a `git clone {project_path}.git {project_path}` over SSH, and then remember to do a push here and a git fetch there.
    """
    _git_remotes = subprocess.getoutput("git remote").splitlines()

    current_git_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD").strip()

    clusters_missing_a_remote = [
        remote for remote in remotes if remote.hostname not in _git_remotes
    ]
    logger.info(
        f"Will setup a git remote for the following clusters: {clusters_missing_a_remote}"
    )

    # TODO: Also add a remote for GitHub in the cloned repos at {project_path} on the remote clusters!
    # Also, maybe use a name like mila-cluv instead of origin for the name of the {project_path}.git remote?

    for login_node in clusters_missing_a_remote:
        LocalV2.run(
            (
                "git",
                "remote",
                "add",
                login_node.hostname,
                f"{login_node.hostname}:{project_path}.git",
            ),
        )
        # TODO: main/master default branch name seems to cause an issue on trillium and vulcan.
        # We might need to figure out which is used in the current project and use that.
        initial_branch = (
            "master" if "master" in subprocess.getoutput("git branch") else "main"
        )
        await login_node.run_async(
            # BUG: on Nibi, we get command not found: 'git' unless we do bash -l -c!
            f"bash -l -c 'mkdir -p {project_path}.git && "
            f"git init --bare {project_path}.git --initial-branch={initial_branch}'",
            display=True,
        )
        # NOTE: Seems okay to push to that remote (the bare {project}.git), this doesn't delete other branches there.
        LocalV2.run(("git", "push", login_node.hostname, current_git_branch))
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
    if clusters_without_clones:
        logger.debug(
            f"Clusters where the project isn't cloned yet: {[remote.hostname for remote in clusters_without_clones]}   "
        )
        await asyncio.gather(
            *(
                remote.run_async(f"git clone {project_path}.git {project_path}")
                for remote in clusters_without_clones
            )
        )
    else:
        await asyncio.gather(
            *(
                # TODO: The project might already be cloned on some clusters.
                remote.run_async(
                    f"git clone {project_path}.git {project_path} --branch={current_git_branch}",
                    warn=True,
                    hide=True,
                )
                for remote in remotes
            )
        )

    # Setup post-receive hook maybe?
    # TODO: Look into setting up a git hook on the remote, it could be useful
    # to run things or checkout the repo automatically when we push.

    # await asyncio.gather(
    #     *(
    #         # Add a pre-receive hook in that bare remote repo, so that it always checks out stuff.
    #         remote.run_async(
    #             f"""mkdir -p {project_path}.git/hooks && ""echo '#!/bin/bash\n"
    #             f"git --work-tree={project_path} --git-dir={project_path}.git checkout -f\n' > {project_path}.git/hooks/post-receive && "
    #             f"chmod +x {project_path}.git/hooks/post-receive""",
    #             display=True,
    #         )
    #         for remote in remotes
    #     )
    # )

    for remote in remotes:
        LocalV2.run(("git", "push", remote.hostname, current_git_branch))

    await asyncio.gather(
        *(
            remote.run_async(
                f"git -C {project_path} fetch --all --prune",
                # warn=True,
                hide=False,
            )
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


async def _get_remote_without_2fa_prompt(cluster_hostname: str) -> RemoteV2 | None:
    """Returns the Remote object for a given cluster if we already have a connection to it.

    If we don't already have a connection, this will not block for 2FA, and will return None.
    """
    remote = RemoteV2(cluster_hostname, _start_control_socket=False)
    active = await control_socket_is_running_async(
        cluster_hostname,
        control_path=get_controlpath_for(
            cluster_hostname, ssh_config_path=Path.home() / ".ssh" / "config"
        ),
    )
    if active:
        # It's active, so we can "connect" (this just sets _started=True since it's
        # already running)
        # NOTE: This is a bit weird.
        remote._started = True
        return remote
    return None
