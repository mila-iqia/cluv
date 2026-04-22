from __future__ import annotations

import asyncio
import datetime
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

from cluv.cli.sync import sync
from cluv.config import find_pyproject, get_config
from cluv.remote import Remote
from cluv.utils import console


async def submit_chain(
    cluster: str,
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
    num_jobs_in_chain: int = 10,
):
    job_id = await submit_job_to_cluster(
        cluster=cluster,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
    )
    for n in range(num_jobs_in_chain - 1):
        job_id = await submit_job_to_cluster(
            cluster=cluster,
            job_script=job_script,
            sbatch_args=sbatch_args
            + ["--kill-on-invalid-dep=yes", f"--dependency=afterok:{job_id}"],
            program_args=program_args,
        )


async def submit(
    cluster: str,
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
):
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets GIT_COMMIT and any
    SBATCH_* env vars configured in [tool.cluv.slurm] / [tool.cluv.clusters.<name>],
    then calls sbatch on the remote.

    sbatch_args are forwarded as flags to sbatch; program_args are passed to
    the job script. main() extracts program_args from argv before argparse runs,
    since argparse strips '--' before REMAINDER sees it.
    """
    if cluster == "auto":
        return await submit_auto(job_script, sbatch_args, program_args)
    if cluster == "first":
        return await submit_auto(job_script, sbatch_args, program_args)

    else:
        job_id = await submit_job_to_cluster(
            cluster,
            job_script,
            sbatch_args,
            program_args,
        )
        console.log(
            f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
            f"Use `ssh {cluster} sacct -j {job_id}` to view its status."
        )


async def submit_auto(
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
):
    """Use the --test-only flag of sbatch to find the cluster with the earliest estimated start time,
    then submit the job there.
    """
    # Check git is clean locally (untracked files are fine).
    git_commit = ensure_clean_git_state()
    # Sync with all clusters with an existing connections.
    remotes = await sync()
    clusters = [r.hostname for r in remotes]

    job_ids_and_estimated_starttimes = await asyncio.gather(
        *(
            run_sbatch_command(
                remote,
                job_script,
                sbatch_args + ["--test-only"],
                program_args,
                git_commit=git_commit,
            )
            for remote in remotes
        ),
        return_exceptions=True,
    )
    cluster_to_jobid_and_starttime: dict[str, tuple[int, datetime.datetime]] = {
        cluster: (result[0], starttime)
        for cluster, result in zip(clusters, job_ids_and_estimated_starttimes)
        if isinstance(result, tuple) and (starttime := result[1]) is not None
    }
    console.log("Estimated start times for each cluster:")
    for cluster, (job_id, starttime) in cluster_to_jobid_and_starttime.items():
        console.log(
            f"- {cluster}: job {job_id} to start at {starttime} ({starttime - datetime.datetime.now()} from now.)"
        )

    cluster_with_earliest_startime = min(
        cluster_to_jobid_and_starttime, key=lambda c: cluster_to_jobid_and_starttime[c][1]
    )
    remote = next(r for r in remotes if r.hostname == cluster_with_earliest_startime)
    return await run_sbatch_command(
        remote, job_script, sbatch_args, program_args, git_commit=git_commit
    )


async def submit_first(
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
):
    """TODO: submit the job on all clusters, and wait until one of them starts.
    Once one starts, cancel the others.
    """
    # Check git is clean locally (untracked files are fine).
    git_commit = ensure_clean_git_state()
    # Sync with all clusters with an existing connections.
    remotes = await sync()
    clusters = [r.hostname for r in remotes]

    job_ids_and_Nones = await asyncio.gather(
        *(
            run_sbatch_command(
                remote,
                job_script,
                sbatch_args,
                program_args,
                git_commit=git_commit,
            )
            for remote in remotes
        ),
        return_exceptions=True,
    )
    cluster_to_jobid: dict[str, int] = {
        cluster: result[0]
        for cluster, result in zip(clusters, job_ids_and_Nones)
        if isinstance(result, tuple)
    }
    console.log("Submitted jobs to the following clusters:")
    for cluster, job_id in cluster_to_jobid.items():
        console.log(f"- {cluster}: job {job_id}")

    raise NotImplementedError("Wait until one of the jobs starts, then cancel the others.")


async def submit_job_to_cluster(
    cluster: str,
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
) -> tuple[int, datetime.datetime | None]:
    """Submit a SLURM job on a remote cluster.

    Enforces a clean git state, syncs the project, sets GIT_COMMIT and any
    SBATCH_* env vars configured in [tool.cluv.slurm] / [tool.cluv.clusters.<name>],
    then calls sbatch on the remote.

    sbatch_args are forwarded as flags to sbatch; program_args are passed to
    the job script. main() extracts program_args from argv before argparse runs,
    since argparse strips '--' before REMAINDER sees it.
    """
    # Check git is clean locally (untracked files are fine).
    git_commit = ensure_clean_git_state()

    # Sync.
    remote = (await sync(clusters=[cluster]))[0]
    return await run_sbatch_command(
        remote,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        git_commit=git_commit,
    )


async def run_sbatch_command(
    remote: Remote,
    job_script: str,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> tuple[int, datetime.datetime | None]:
    cluster = remote.hostname
    remote_cmd = get_sbatch_command(
        cluster, Path(job_script), sbatch_args, program_args, git_commit
    )
    # Submit.
    if "--test-only" in sbatch_args:
        console.print(f"Testing a job submission on [bold]{cluster}[/bold].")
        completed_process = await remote.run(remote_cmd)
        return get_job_id_and_starttime_from_stderr(completed_process.stderr)

    console.print(f"Submitting job on [bold]{cluster}[/bold].")
    job_id = int(await remote.get_output(remote_cmd))
    return job_id, None


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
) -> str:
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


def ensure_clean_git_state():
    git_status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    dirty_lines = [line for line in git_status.stdout.splitlines() if not line.startswith("??")]
    if dirty_lines and not (os.environ.get("SKIP_CLEAN_GIT_CHECK", "0") == "1"):
        console.print(
            "[red]Working directory is dirty. Please commit your changes before submitting.[/red]",
        )
        sys.exit(1)

    # Capture current commit hash.
    git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    return git_commit


def get_job_id_and_starttime_from_stderr(stderr: str) -> tuple[int, datetime.datetime]:
    r"""Gets the job ID from the stderr output of sbatch --test-only, which looks like:

    >>> get_job_id_and_starttime_from_stderr("sbatch: Job 10759317 to start at 2026-04-21T16:55:36 using 1 processors on nodes rc32407 in partition cpubase_bycore_b1\n")
    (10759317, datetime.datetime(2026, 4, 21, 16, 55, 36))
    """
    match = re.search(r"sbatch: Job (\d+) to start at", stderr)
    if not match:
        raise ValueError(f"Could not parse job ID from sbatch output: {stderr}")
    job_id = int(match.group(1))

    # Remove the rest of the message (that we don't need).
    if "a using" in stderr:
        # Weird output on the Mila cluster:
        stderr = stderr[: stderr.index(" a using")]
    else:
        # Remove the rest of the message (that we don't need).
        stderr = stderr[: stderr.index(" using")]

    starttime_estimate = datetime.datetime.strptime(
        stderr,
        f"sbatch: Job {job_id} to start at %Y-%m-%dT%H:%M:%S",
    )
    return job_id, starttime_estimate
