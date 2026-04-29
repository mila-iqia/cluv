from __future__ import annotations

import asyncio
import shlex
import subprocess
import sys
from pathlib import Path
from time import sleep

from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.remote import Remote
from cluv.utils import console

# TODO : Logger


async def submit(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
) -> int | None:
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets GIT_COMMIT and any
    SBATCH_* env vars configured in [tool.cluv.slurm] / [tool.cluv.clusters.<name>],
    then calls sbatch on the remote.

    sbatch_args are forwarded as flags to sbatch; program_args are passed to
    the job script. main() extracts program_args from argv before argparse runs,
    since argparse strips '--' before REMAINDER sees it.
    """
    # Check git is clean locally (untracked files are fine) and capture current commit hash.
    git_commit = ensure_clean_git_state()

    if cluster == "first":
        return await submit_first(job_script, sbatch_args, program_args, git_commit)

    # Sync.
    remotes = await sync(clusters=[cluster])

    # Submit the sbatch command.
    remote = remotes[0]
    job_id = await sbatch(remote, job_script, sbatch_args, program_args, git_commit)

    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        f"Use `ssh {cluster} sacct -j {job_id}` to view its status."
    )

    return job_id
    # return the job id?


async def submit_first(
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> int | None:
    """Submit the job on all clusters, and wait until one of them starts.
    Once one starts, cancel the others.
    """
    # Sync with all clusters with an existing connections.
    remotes = await sync()
    clusters_to_remote = {remote.hostname: remote for remote in remotes}

    # Submit the job on all the clusters
    job_ids = await asyncio.gather(
        *(
            sbatch(
                remote,
                job_script,
                sbatch_args,
                program_args,
                git_commit,
            )
            for remote in remotes
        ),
        return_exceptions=True,
    )

    print(job_ids)

    # Get the results of the sbatch command. We expect an int (the job id) or the exception
    # if the command failed on the remote cluster.
    console.log("Try to submit jobs to the following clusters:")
    cluster_to_jobid: dict[str, int] = {}
    for cluster, result in zip(clusters_to_remote.keys(), job_ids):
        if isinstance(result, int):
            cluster_to_jobid[cluster] = result
            console.log(f"  - {cluster}: job {result}")
        elif isinstance(result, Exception):
            console.log(f"[red]  - {cluster}: error, {result}/[red]")

    if len(cluster_to_jobid) == 0:
        console.log("No job submitted on clusters. See errors above.")
        return None

    # Wait for a job to start on a cluster.
    # If the wait is interrupted, cancel all jobs.
    start_cluster: str | None = None
    start_job_id: int | None = None
    try:
        with console.status("Waiting for a job to start..."):
            while start_cluster is None:
                # TODO : Replace by asyncio.gather to check clusters in parallel
                for cluster, remote in clusters_to_remote.items():
                    job_id = cluster_to_jobid.get(cluster)
                    if job_id is None:
                        continue
                    job_status = await get_job_status(remote, job_id)

                    # TODO : What if jobs failed ? Need to remember the status of each cluster ?
                    if job_status in ["RUNNING", "COMPLETED"]:
                        start_cluster = cluster
                        start_job_id = job_id
                # TODO : make the waiting time gradually increase to avoid making the user wait too long when the jobs are short
                sleep(20)
        console.log(
            f"Job {start_job_id} on cluster {start_cluster} is running. Cancelling the other jobs..."
        )
    except KeyboardInterrupt:
        console.log("Interrupted by user. Cancelling all jobs...")
        await cancel_all_jobs(clusters_to_remote, cluster_to_jobid, None)

    await cancel_all_jobs(clusters_to_remote, cluster_to_jobid, start_cluster)

    return start_job_id


def ensure_clean_git_state() -> str:
    """
    Check git is clean locally and return the current commit hash.
    """
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines:
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]",
        )
        sys.exit(1)

    # Capture current commit hash.
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> str:
    """
    Generate the command to submit the job via sbatch on the remote cluster, with the appropriate env vars set.
    """
    # Resolve remote job script path.
    project_path = find_pyproject().parent.relative_to(Path.home())
    remote_job_script = f"~/{project_path}/{job_script}"

    # Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    config = get_config()
    env_vars: dict[str, str] = {**config.slurm}
    env_vars.update(config.cluster_configs.get(cluster, {}))

    # Prefix the job name with "cluv-" so it is easy to identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={project_path} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


async def sbatch(
    remote: Remote,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> int:
    """Submit the job via sbatch on the remote cluster, and return the job id."""
    cluster = remote.hostname

    remote_cmd = get_sbatch_command(
        cluster, Path(job_script), sbatch_args, program_args, git_commit
    )

    console.print(f"Submitting job on [bold]{cluster}[/bold].")

    return int(await remote.get_output(remote_cmd))


async def get_job_status(remote: Remote, job_id: int) -> str:
    """Get the status of the job with the given id on the remote cluster."""
    sacct_command = f"sacct -j {job_id} --format=State --noheader --parsable2 | head -1"
    return await remote.get_output(sacct_command)


async def cancel_job(remote: Remote, job_id: int) -> str:
    """Cancel the job with the given id on the remote cluster."""
    scancel_command = f"scancel {job_id}"
    output = await remote.get_output(scancel_command)
    console.log(f"Cancelled job {job_id} on cluster {remote.hostname}.")
    return output


async def cancel_all_jobs(
    remotes: dict[str, Remote], cluster_to_jobid: dict[str, int], keep_cluster: str | None
) -> None:
    """Cancel all jobs in cluster_to_jobid on their respective remotes."""
    # TODO : Replace by asyncio.gather to delete jobs in parallel
    for cluster, job_id in cluster_to_jobid.items():
        if cluster == keep_cluster:
            continue
        remote = remotes[cluster]
        await cancel_job(remote, job_id)
