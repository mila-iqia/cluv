from __future__ import annotations

import asyncio
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path, PurePosixPath

from cluv.cache import save_job
from cluv.cli.submit_utils.chunking import chunking_update_sbatch_ars
from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_cluv_config
from cluv.remote import Remote
from cluv.slurm import FAILED_JOB_STATES
from cluv.utils import console

DEFAULT_CHUNCK_TIME = "3:00:00"
RUNNING_JOB_STATES = ["PENDING", "RUNNING"]
logger = logging.getLogger(__name__)

__all__ = ["submit"]


async def submit(
    cluster: str,
    job_script: Path,
    chunking: bool,
    sbatch_args: list[str],
    program_args: list[str],
) -> int | None:
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets `GIT_COMMIT` and any
    environment variables configured in `[tool.cluv.env]` / `[tool.cluv.clusters.<name>.env]`,
    then calls `sbatch` on the remote.

    `sbatch_args` are forwarded as flags to `sbatch`; `program_args` are passed to
    the job script.


    Parameters:
        cluster: SSH hostname of the target cluster. Can be set to "first" to launch the job on all clusters and keep only the first one to starts.
        job_script: Path to the job script to submit, relative to the project root.
        chunking: TODO
        sbatch_args: List of additional flags to pass to `sbatch`.
        program_args: List of arguments to pass to the job script, for example `["python", "main.py"]`.

    Returns:
        The job ID of the submitted job or None if the sbatch command fails.

    Examples:

    ```python
    submit(
        "mila",
        "scripts/job.sh",
        sbatch_args=["--time=00:00:10"],
        program_args=["python", "--version"],
    )
    ```
    """
    # Check git is clean locally (untracked files are fine) and capture current commit hash.
    git_commit = ensure_clean_git_state()

    if cluster == "first":
        return await submit_first(job_script, sbatch_args, program_args, git_commit, chunking)

    # Sync.
    remotes = await sync(clusters=[cluster])

    # Run the sbatch command over SSH.
    remote = remotes[0]
    result = await sbatch(remote, job_script, sbatch_args, program_args, git_commit, chunking)

    if result.returncode != 0:
        console.print(f"[red] Error during sbatch : {result.stderr}[/red]")
        return None

    job_id = int(result.stdout.strip())
    save_job(job_id, cluster, str(job_script), git_commit, sbatch_args, program_args)

    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        f"Use `ssh {cluster} sacct -j {job_id}` to view its status."
    )

    return job_id


async def submit_first(
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    chunking: bool,
) -> int | None:
    """Submit the job on all clusters, and wait until one of them starts.
    Once one starts, cancel the others.
    """
    # Sync with all clusters with an existing connections.
    remotes = await sync()
    clusters_to_remote = {remote.hostname: remote for remote in remotes}

    # Submit the job on all the clusters
    sbatch_results = await asyncio.gather(
        *[
            sbatch(remote, job_script, sbatch_args, program_args, git_commit, chunking)
            for remote in remotes
        ],
        return_exceptions=True,
    )

    # Get the results of the sbatch command. We expect an int (the job id) or the exception
    # if the command failed on the remote cluster.
    console.print("Jobs submitted on the clusters:")
    cluster_to_jobid: dict[str, int] = {}
    for cluster, result in zip(clusters_to_remote.keys(), sbatch_results):
        if isinstance(result, BaseException):
            console.print(
                f"    - [bold]{cluster}[/bold]: error when trying to use remote, [red]{result}[/red]"
            )
        else:
            if result.returncode == 0:
                job_id = int(result.stdout.strip())
                cluster_to_jobid[cluster] = job_id
                console.print(f"    - [bold]{cluster}[/bold]: job {job_id}")
            else:
                console.print(
                    f"    - [bold]{cluster}[/bold]: no job, [red]{result.stderr.strip()}[/red]"
                )

    if len(cluster_to_jobid) == 0:
        console.print("No job submitted on clusters. See errors above.")
        return None

    # Wait for a job to start on a cluster.
    # If the wait is interrupted, cancel all jobs.
    start_cluster: str | None = None
    start_job_id: int | None = None
    wait_time = 2  # seconds; grows up to 20s
    try:
        with console.status("Waiting for a job to start..."):
            while start_cluster is None:
                failed_clusters: list[str] = []
                for cluster, remote in clusters_to_remote.items():
                    job_id = cluster_to_jobid.get(cluster)
                    if job_id is None:
                        continue
                    job_status = await get_job_status(remote, job_id)

                    if job_status in RUNNING_JOB_STATES:
                        start_cluster = cluster
                        start_job_id = job_id
                        break
                    elif job_status in FAILED_JOB_STATES:
                        console.print(
                            f"Job {job_id} on cluster {cluster} ended with status {job_status}."
                        )
                        failed_clusters.append(cluster)

                # Stop the wait if a job is running
                if start_cluster is not None:
                    break

                # Remove clusters with failed jobs
                for cluster in failed_clusters:
                    del cluster_to_jobid[cluster]

                # Stop the wait if all the jobs failed
                if not cluster_to_jobid:
                    console.log("All submitted jobs have ended without starting. Exiting.")
                    return None

                await asyncio.sleep(wait_time)
                wait_time = min(wait_time * 2, 20)
        console.log(
            f"Job {start_job_id} on cluster {start_cluster} is running. Cancelling the other jobs...\n",
            f"Use `ssh {start_cluster} sacct -j {start_job_id}` to view its status.",
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.log("Interrupted by user. Cancelling all jobs...")
    finally:
        await cancel_all_jobs(clusters_to_remote, cluster_to_jobid, start_cluster)

    if start_job_id is not None and start_cluster is not None:
        save_job(
            start_job_id, start_cluster, str(job_script), git_commit, sbatch_args, program_args
        )

    return start_job_id


def ensure_clean_git_state() -> str:
    """
    Check git is clean locally and return the current commit hash.
    """
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines and not (os.environ.get("SKIP_CLEAN_GIT_CHECK", "0") == "1"):
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]",
        )
        sys.exit(1)

    # In GitHub Actions PR jobs we can be on a detached merge commit that doesn't exist on
    # the synced remote checkout. Prefer the branch tip commit in that case.
    current_branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
    ).strip()
    if current_branch == "HEAD" and os.environ.get("GITHUB_ACTIONS"):
        github_head_ref = os.environ.get("GITHUB_HEAD_REF", "").strip()
        if github_head_ref:
            remote_head_ref = f"origin/{github_head_ref}"
            remote_head_result = subprocess.run(
                ["git", "rev-parse", "--verify", remote_head_ref],
                capture_output=True,
                text=True,
            )
            if remote_head_result.returncode == 0:
                return remote_head_result.stdout.strip()
            console.log(
                f"[yellow]Could not resolve {remote_head_ref}. Falling back to local HEAD commit.[/yellow]"
            )

    # Capture current commit hash.
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    job_chunking: bool,
) -> str:
    """
    Generate the command to submit the job via sbatch on the remote cluster, with the appropriate env vars set.
    """
    # Resolve remote job script path.
    project_root = find_pyproject().parent
    project_root_relative_to_home = project_root.relative_to(Path.home())
    if not job_script.is_absolute():
        job_script = job_script.absolute()
    remote_job_script = f"~/{project_root_relative_to_home}/{job_script.relative_to(project_root)}"

    # Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)
    env_vars: dict[str, str] = {**config.env}
    env_vars.update(cluster_config.env)

    # Prefix the job name with "cluv-" so it is easy to identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    in_job_packing = False

    assert not in_job_packing, "todo"
    # might contain unresolved env vars.
    cluster_results_path = PurePosixPath(cluster_config.results_path)
    # TODO: Use the `get_run_id` function with the placeholder job id %j and task index %t:

    if job_chunking:
        assert not in_job_packing, "can't do both right now."
        env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%A/slurm-%A_%a.out"
        sbatch_args = chunking_update_sbatch_ars(sbatch_args, env_vars, job_script)
    elif in_job_packing:
        env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%j_%t/slurm-%j_%t.out"
    else:
        env_vars["SBATCH_OUTPUT"] = f"{cluster_results_path}/{cluster}_%j/slurm-%j.out"

    output_from_cluv = env_vars["SBATCH_OUTPUT"]
    if (
        output_from_file := next(
            (
                line
                for line in job_script.read_text().splitlines()
                if line.strip().startswith("#SBATCH") and "--output" in line
            ),
            None,
        )
    ) and output_from_file != output_from_cluv:
        logger.warning(
            UserWarning(
                f"[yellow]⚠️ The job script {job_script} contains an SBATCH --output directive "
                f"which will be overwritten by cluv, to facilitate the syncing of results.\n"
                f"Consider using cluv in your Python script to decide where to store results. "
                f"Take a look a the pytorch example of the Cluv repo for more info.[/yellow]"
            )
        )

    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={project_root_relative_to_home} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


async def sbatch(
    remote: Remote,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    chunking: bool,
) -> subprocess.CompletedProcess[str]:
    """Submit the job via sbatch on the remote cluster, and return the job id."""
    remote_cmd = get_sbatch_command(
        remote.hostname, job_script, sbatch_args, program_args, git_commit, chunking
    )

    return await remote.run(remote_cmd, display=True, warn=True, hide=True)


async def get_job_status(remote: Remote, job_id: int) -> str:
    """Get the status of the job with the given id on the remote cluster."""
    sacct_command = f"sacct -j {job_id} --format=State --noheader --allocations"
    return await remote.get_output(sacct_command)


async def cancel_job(remote: Remote, job_id: int) -> str:
    """Cancel the job with the given id on the remote cluster."""
    scancel_command = f"scancel {job_id}"
    output = await remote.get_output(scancel_command)
    console.print(f"Cancelled job {job_id} on cluster {remote.hostname}.")
    return output


async def cancel_all_jobs(
    remotes: dict[str, Remote], cluster_to_jobid: dict[str, int], keep_cluster: str | None
) -> None:
    """Cancel all jobs in cluster_to_jobid on their respective remotes."""
    await asyncio.gather(
        *[
            cancel_job(remotes[cluster], job_id)
            for cluster, job_id in cluster_to_jobid.items()
            if cluster != keep_cluster
        ]
    )
