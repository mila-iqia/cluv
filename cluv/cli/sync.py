from __future__ import annotations

import asyncio
import datetime
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

import milatools.cli
import milatools.utils.parallel_progress

# Reuse some code milatools. Could also extract it here to remove the dependency.
from milatools.utils.parallel_progress import (
    AsyncTaskFn,
    ReportProgressFn,
    run_async_tasks_with_progress_bar,
)

from cluv.cache import ProjectStateOnCluster, read_cache, write_cache
from cluv.cli.login import get_remote_without_2fa_prompt, login
from cluv.config import CluvConfig, find_pyproject, get_cluv_config, load_cluv_config
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
    here = current_cluster()
    if clusters and here in clusters:
        clusters.remove(here)

    config = get_cluv_config()

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

    per_cluster_new_runs: list[list[Path]] = await run_async_tasks_with_progress_bar(
        async_task_fns=tasks,
        task_descriptions=task_descriptions,
        overall_progress_task_description="[green]Syncing project",
    )
    console_lock.reset(token)

    # Display a consolidated summary of all newly-synced runs across all clusters.
    cwd = Path.cwd()
    for remote, new_runs in zip(remotes, per_cluster_new_runs):
        if new_runs:
            console.print(f"[green]Newly synced runs from [bold]{remote.hostname}[/bold]:[/green]")
            for run_path in sorted(new_runs):
                try:
                    display_path = run_path.relative_to(cwd)
                except ValueError:
                    display_path = run_path
                console.print(f"  {display_path}")

    return remotes


async def get_active_remotes() -> list[Remote]:
    """Returns the Remotes for each cluster which has an active SSH connection."""
    clusters = get_cluv_config().clusters_names
    if (this_cluster := current_cluster()) and this_cluster in clusters:
        clusters.remove(this_cluster)
    connections = await asyncio.gather(
        *(get_remote_without_2fa_prompt(cluster) for cluster in clusters)
    )
    remotes = [conn for conn in connections if conn]  # keep the active connections.
    return remotes


async def sync_task_function(report_progress: ReportProgressFn, remote: Remote) -> list[Path]:
    """Syncs a single cluster, and reports progress using the provided `report_progress` function."""
    config = get_cluv_config()
    cluster = remote.hostname
    cluster_config = config.get_cluster_config(remote.hostname)
    project_path = cluster_config.project_dir
    if project_path is None:
        if find_pyproject().parent.is_relative_to(Path.home()):
            project_path = PurePosixPath(
                "$HOME" / find_pyproject().parent.relative_to(Path.home())
            )
        else:
            raise RuntimeError(
                f"Project path is not set for cluster {cluster!r} in the Cluv config, and the "
                f"project root ({find_pyproject().parent}) is not under $HOME. "
                f"Please set `cluv.project_dir` in the Cluv config section of pyproject.toml."
            )
    project_path = await expandvars(remote, project_path)

    def _update_progress(progress: int, status: str, total: int):
        info = textwrap.shorten(status, 50, placeholder="...")
        report_progress(progress=progress, total=total, info=info)

    num_tasks = 5 if config.data_source else 4

    cache = read_cache()
    project_state = cache.project_states.setdefault(cluster, ProjectStateOnCluster())

    def _save():
        assert cache.project_states[cluster] is project_state
        write_cache(cache)

    _update_progress(0, "Checking/Installing UV", num_tasks)
    await install_uv(remote, project_state)
    _save()

    _update_progress(1, "Setting up project", num_tasks)
    await clone_project(remote, project_path=project_path, project_state=project_state)
    _save()

    _update_progress(2, "Running 'uv sync'", num_tasks)
    await run_uv_sync(remote, project_path, project_state)
    _save()

    _update_progress(3, "Fetching results", num_tasks)
    new_runs = await fetch_results(remote, config)

    if config.data_source:
        _update_progress(4, "Syncing datasets", num_tasks)
        here = current_cluster()
        local_dataset_path = (config.get_cluster_config(here) if here else config).datasets_path
        if not local_dataset_path:
            raise RuntimeError("data_source is set, so dataset_path should also be set!")
        local_dataset_path = Path(os.path.expandvars(local_dataset_path))
        await _push_datasets_to_remote(local_dataset_path, remote, config, project_state)
        _save()

    _update_progress(num_tasks, "Done", num_tasks)
    return new_runs


async def expandvars(remote: Remote, path: str | PurePosixPath) -> PurePosixPath:
    """Same idea as `os.path.expandvars`, but for a path on a remote machine. Just uses `echo`."""
    if "$" not in str(path):
        return PurePosixPath(path)
    return PurePosixPath(
        (
            await remote.get_output(
                f"bash --login -c 'echo {path}'", hide=True, warn=True, display=False
            )
        ).strip()
    )


async def run_uv_sync(
    remote: Remote, project_path: PurePosixPath, project_state: ProjectStateOnCluster
):
    current_git_commit = subprocess.getoutput("git rev-parse HEAD").strip()

    if project_state.last_uv_sync_git_commit == current_git_commit:
        logger.info(
            f"uv sync was already run for the current commit ({current_git_commit}) on "
            f"{remote.hostname}. Skipping uv sync."
        )
        return
    await remote.run(f"bash --login -c 'uv --directory={project_path} sync --quiet'")
    project_state.last_uv_sync_git_commit = current_git_commit


async def install_uv(remote: Remote, project_state: ProjectStateOnCluster):
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
    logger.debug(
        f"[green]Using uv version {uv_version_here} everywhere, since this is the version on this machine.[/green]"
    )
    if project_state.uv_version == uv_version_here:
        logger.info(
            f"uv version {uv_version_here} is already installed on {remote.hostname}, skipping."
        )
        return

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

    project_state.uv_version = uv_version_here


def _is_github_pr_ref(github_ref: str) -> bool:
    """Checks if this value (from the GITHUB_REF environment variable) is a GitHub PR ref."""
    return re.fullmatch(r"refs/pull/[0-9]+/(merge|head)", github_ref) is not None


async def clone_project(
    remote: Remote, project_path: PurePosixPath, project_state: ProjectStateOnCluster
):
    """Setup the project repo on all the remote clusters.

    New idea:
    - Assume GitHub. Push to GitHub if needed. Clone from github on the remotes.
    - Worry about authentication later, just raise an error if need be for now.
    """
    current_git_commit = subprocess.getoutput("git rev-parse HEAD").strip()

    # In the case of a subproject (like the examples in the cluv repo), these are different!
    local_project_root = find_pyproject().parent
    local_repo_dir = Path(subprocess.getoutput("git rev-parse --show-toplevel").strip())

    if local_project_root == local_repo_dir:
        cluster_repo_dir = project_path
    elif not local_repo_dir.is_relative_to(Path.home()):
        # Try to find the directory where the project should be cloned on the cluster
        # by reading the pyproject.toml at the repo root. Hopefully it has a cluv config with project_dir set.
        cluster_repo_dir = None
        if (local_repo_dir / "pyproject.toml").exists():
            cluster_repo_dir = (
                load_cluv_config(local_repo_dir / "pyproject.toml")
                .get_cluster_config(remote.hostname)
                .project_dir
            )
        if not cluster_repo_dir:
            raise RuntimeError(
                f"Can't tell where to clone the current git repository on {remote.hostname}, "
                f"because the project isn't under $HOME, and there is no `project_dir` in the "
                f"subproject or in the root pyproject.toml."
            )
    else:
        cluster_repo_dir = PurePosixPath("$HOME" / local_repo_dir.relative_to(Path.home()))
        cluster_repo_dir = await expandvars(remote, cluster_repo_dir)

    if project_state.checked_out_git_commit == current_git_commit:
        logger.info(
            f"Project is already at commit {current_git_commit} on {remote.hostname}. Skipping."
        )
        return

    # TODO: This git info is shared, but currently repeatedly executed for each cluster.
    # Could be done only once.
    current_git_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD").strip()
    detached_head = current_git_branch == "HEAD"

    git_remote_name = "origin"
    if not detached_head:
        git_remote_name = subprocess.check_output(
            ["git", "config", "--get", f"branch.{current_git_branch}.remote"],
            text=True,
        ).strip()
        git_remote_name = shlex.quote(git_remote_name)

    github_repo_url = subprocess.getoutput(
        f"git config --get remote.{git_remote_name}.url"
    ).strip()
    if not github_repo_url:
        raise RuntimeError(
            f"Could not determine Git remote URL from remote '{git_remote_name}'. "
            "Make sure your git remote is configured."
        )

    # We want to use git with ssh -o StrictHostKeyChecking=accept-new to facilitate first
    # communication with GitHub (notably on clusters that default to StrictHostKeyChecking=yes
    # rather than ask), which can be configured with the GIT_SSH_COMMAND environment variable.
    gitenv = {"GIT_SSH_COMMAND": "ssh -o StrictHostKeyChecking=accept-new"}

    # If the project isn't cloned yet, clone it.
    if not await remote_test("-d", cluster_repo_dir, remote):
        logger.info(f"Project isn't cloned yet on {remote.hostname}.")
        await remote.run(f"git clone {github_repo_url} {cluster_repo_dir}", hide=True, env=gitenv)

    # Doesn't matter if we run git fetch/pull/checkout in the repo root or the subdirectory,
    # no need to use `cluster_repo_dir` here.
    await remote.run(f"git -C {project_path} fetch --all --prune", hide=True, env=gitenv)

    if not detached_head:
        # Simplest case. We're on a branch, life is good.
        await remote.run(
            f"git -C {project_path} checkout {current_git_branch}", hide=False, env=gitenv
        )
        await remote.run(f"git -C {project_path} pull", hide=False, env=gitenv)

        # Set the checked out commit for that project on that cluster. This will be written to the
        # cache to avoid unnecessary syncs later.
        project_state.checked_out_git_commit = current_git_commit
        return

    # Detached head (not on a branch), for example in a CI run on GitHub (pull request/push/release)

    github_head_ref = os.environ.get("GITHUB_HEAD_REF", "").strip()
    # Quote in case there are spaces or other weird characters perhaps embedded in the branch name,
    # to avoid command injection vulnerabilities. We also check for some weird characters in the
    # branch name later on, but this is just in case.
    github_head_ref = shlex.quote(github_head_ref)

    # From the GitHub docs:
    # https://docs.github.com/en/actions/reference/workflows-and-actions/variables
    #     GITHUB_HEAD_REF: "The head ref or source branch of the pull request in a workflow run.
    #      This property is only set when the event that triggers a workflow run is either
    #      pull_request or pull_request_target. For example, feature-branch-1."

    if not github_head_ref:
        # Push on master, for example after merging a PR.
        await remote.run(
            f"git -C {project_path} checkout --detach {current_git_commit}",
            hide=False,
            env=gitenv,
        )
        project_state.checked_out_git_commit = current_git_commit
        return

    # GITHUB_HEAD_REF is set, because we're in a pull request CI run.
    if (
        not re.fullmatch(r"[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*", github_head_ref)
        or ".." in github_head_ref
    ):
        raise RuntimeError(f"Invalid GITHUB_HEAD_REF value: {github_head_ref!r}")

    github_ref = os.environ.get("GITHUB_REF", "").strip()
    github_ref = shlex.quote(github_ref)
    """The PR ref on the base repo (e.g. 'refs/pull/72/merge') when run by GitHub Actions for a PR.

    Unlike the PR head branch, this ref exists on the base repo even when the PR comes
    from a fork, so the project clones on the clusters can fetch it from their remote.

    GitHub docs: "The fully-formed ref of the branch or tag that triggered the workflow run."
    """

    if _is_github_pr_ref(github_ref):
        # The head branch of a PR from a fork doesn't exist on the base repo, so
        # fetch the PR ref instead and create the branch from FETCH_HEAD.
        await remote.run(
            f"git -C {project_path} fetch {git_remote_name} {github_ref}",
            hide=False,
            env=gitenv,
        )
        await remote.run(
            f"git -C {project_path} checkout -B {github_head_ref} FETCH_HEAD",
            hide=False,
            env=gitenv,
        )
        project_state.checked_out_git_commit = current_git_commit
        return

    # GITHUB_REF was not a PR ref, so it could be a release or a tag? Or a branch that exists on the
    # base repo?
    # TODO: Use code coverage to check if/when we hit this case.

    safe_tracking_ref = shlex.quote(f"{git_remote_name}/{github_head_ref}")
    await remote.run(
        f"git -C {project_path} checkout -B {github_head_ref} {safe_tracking_ref}",
        hide=False,
        env=gitenv,
    )
    await remote.run(
        f"git -C {project_path} pull {git_remote_name} {github_head_ref}",
        hide=False,
        env=gitenv,
    )
    project_state.checked_out_git_commit = current_git_commit


async def _pull_datasets(source_remote: Remote, source_path: str, local_datasets_path: Path):
    """Pull from source to the locally-resolved datasets_path."""
    # Resolve the env vars on the remote.
    source_host = source_remote.hostname
    if "$" in source_path:
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
    if "$" in source_path:
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


async def _push_datasets_to_remote(
    local_source: Path, remote: Remote, config: CluvConfig, project_state: ProjectStateOnCluster
):
    """Push dataset from a local path to the remote cluster's datasets_path."""
    last_datasets_dir_edit_time = datetime.datetime.fromtimestamp(local_source.stat().st_mtime)

    # Skip if we pushed after the last edit to the local source path.
    if (
        last_push_datasets_time := project_state.last_pushed_datasets
    ) and last_push_datasets_time > last_datasets_dir_edit_time:
        logger.info(
            f"Datasets at {local_source} were already pushed to {remote.hostname} and have not "
            f"changed since. Skipping."
        )
        return
    datasets_path_template = str(config.get_cluster_config(remote.hostname).datasets_path)
    resolved_path = (
        await remote.get_output(
            f"bash --login -c 'echo {datasets_path_template}'", hide=True, display=False
        )
        if "$" in datasets_path_template
        else datasets_path_template
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
    last_push_datetime = datetime.datetime.now()
    project_state.last_pushed_datasets = last_push_datetime


async def fetch_results(remote: Remote, config: CluvConfig) -> list[Path]:
    """Fetches results from a remote cluster to local using rsync via the results symlink.

    Returns the list of newly-synced run directories (those that did not exist locally before
    the rsync ran).
    """
    results_path_here = Path(os.path.expandvars(config.results_path))
    results_path_here.mkdir(parents=True, exist_ok=True)

    # Snapshot the runs already present locally before syncing.
    existing_runs: set[Path] = (
        {p for p in results_path_here.iterdir() if p.is_dir()}
        if results_path_here.exists()
        else set()
    )

    # Resolve any environment variables in the results_path on the remote before rsync, otherwise
    # it would try to fetch results from a literal $SCRATCH/... folder, which doesn't exist.
    results_path_on_cluster = str(config.get_cluster_config(remote.hostname).results_path)
    results_path_on_cluster = await expandvars(remote, results_path_on_cluster)

    project_path_on_cluster = config.get_cluster_config(remote.hostname).project_dir
    project_path_on_cluster = project_path_on_cluster or PurePosixPath(
        find_pyproject().parent.relative_to(Path.home())
    )
    project_path_on_cluster = await expandvars(remote, project_path_on_cluster)
    # Optional, but useful if it isn't already set up: Create a symlink at project_root/<symlink_name>
    # that points to the results_path (usually in $SCRATCH). This works with the example job script
    # templates, which have `--output=logs/%j/slurm-%j.out` (relative to the project root).
    await create_results_dir_with_symlink_to_scratch(
        remote,
        project_dir=project_path_on_cluster,
        results_symlink=config.results_symlink,
        results_path=results_path_on_cluster,
    )

    await run(
        (
            "rsync",
            "--archive",
            "--verbose",
            "--compress",
            "--copy-links",
            "--chmod=u+w",
            f"{remote.hostname}:{results_path_on_cluster}/",
            f"{results_path_here}/",
        ),
        warn=True,
        hide=False,
    )

    if not results_path_here.exists():
        return []
    return sorted({p for p in results_path_here.iterdir() if p.is_dir()} - existing_runs)


async def create_results_dir_with_symlink_to_scratch(
    remote: Remote, project_dir: PurePosixPath, results_symlink: str, results_path: PurePosixPath
):
    """On the remote, create results_path and symlink project/<results_symlink> -> results_path.

    results_path may contain env vars (e.g. $SCRATCH); they are resolved via the remote login shell.
    """
    # Env vars should have been resolved by now.
    assert "$" not in str(results_path)
    assert "$" not in str(project_dir)
    symlink_path = project_dir / results_symlink

    # Create the target directory if it doesn't already exist.
    if not await remote_test("-d", results_path, remote):
        result = await remote.run(f"mkdir -p {results_path}", warn=True, hide=True)
        if result.returncode != 0:
            logger.warning(
                f"Failed to create {results_path} on {remote.hostname}. "
                f"Results will be stored in {symlink_path}, which may fill up $HOME."
            )
            await remote.run(f"mkdir -p {symlink_path}", warn=True, hide=True)
            return

    # If a symlink already exists at the path (valid or broken), nothing to do.
    if await remote_test("-L", symlink_path, remote):
        return

    # If a real file/directory exists there, warn, the user may be filling up $HOME.
    if await remote_test("-e", symlink_path, remote):
        logger.warning(
            f"{symlink_path} on {remote.hostname} is a real directory, not a symlink. "
            f"You may end up filling up $HOME. Consider replacing it with a symlink to {results_path}."
        )
        return

    # Nothing at the path yet, create the symlink.
    result = await remote.run(
        f"ln -s -T {results_path} {symlink_path}",
        warn=True,
        hide=True,
    )
    if result.returncode != 0:
        logger.warning(
            f"Failed to create symlink {symlink_path} -> {results_path} on {remote.hostname}: {result.stderr}\n"
        )


async def remote_test(
    flag: Literal["-d", "-e", "-L"], path: str | PurePosixPath, remote: Remote
) -> bool:
    """Returns True if `test {flag} {path}` succeeds on the remote."""
    result = await remote.run(f"test {flag} {path}", warn=True, hide=True)
    return result.returncode == 0


def get_loglevel():
    return logging.getLogger("cluv").getEffectiveLevel()


async def host_uses_controlmaster(hostname: str) -> bool:
    applied_options_for_host = get_ssh_options_for_host(hostname)
    return applied_options_for_host.get("controlmaster", "no").lower() != "no"
