from __future__ import annotations

import argparse
import asyncio
import datetime
import math
import re
from pathlib import Path

from cluv.cli.submit import ensure_clean_git_state, sbatch, submit
from cluv.cli.sync import sync
from cluv.config import get_config
from cluv.utils import console

MAX_OVERHEAD_RATIO = 0.10
"""
Maximum fraction of the job's time that can be spent in overhead from resuming from a checkpoint.
"""


async def smart_submit(
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    wait_for_job_start: bool = False,
    job_setup_time: datetime.timedelta = datetime.timedelta(minutes=10),
):
    """Smart job submission to a Slurm cluster.

    - Automatically finds the _best_ cluster to use for your job
        - Uses the `--test-only` flag of `sbatch` to find the cluster with the earliest start time

    - If enabled, automatically splits the job into chunks so it gets scheduled much faster.

    **Your job *must* implement checkpointing to use this feature!**
    """

    # if wait_for_job_start:
    if wait_for_job_start:
        return await submit_first(job_script, sbatch_args, program_args)

    # return await submit_auto(job_script, sbatch_args, program_args)


async def submit_autochunk(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    min_chunk_time: datetime.timedelta = datetime.timedelta(hours=3),
    job_setup_time: datetime.timedelta = datetime.timedelta(minutes=10),
):
    """Split the job into chunks automagically."""
    num_chunks_needed, chunk_length = determine_job_chunk_parameters(
        cluster, sbatch_args, job_script, min_chunk_time, job_setup_time
    )
    sbatch_args = replace_time_arg(sbatch_args, chunk_length)
    return await submit_chain(
        cluster, job_script, sbatch_args, program_args, num_jobs_in_chain=num_chunks_needed
    )


async def submit_chain(
    cluster: str,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    num_jobs_in_chain: int = 10,
):
    job_ids: list[int] = []
    job_id = await submit(cluster, job_script, sbatch_args, program_args)
    job_ids.append(job_id)
    for n in range(num_jobs_in_chain - 1):
        job_args = sbatch_args + ["--kill-on-invalid-dep=yes", f"--dependency=afterok:{job_id}"]
        job_id = await submit(cluster, job_script, job_args, program_args)
        job_ids.append(job_id)
    return job_ids


def determine_job_chunk_parameters(
    cluster: str,
    sbatch_args: list[str],
    job_script: Path,
    min_chunk_time: datetime.timedelta = datetime.timedelta(hours=3),
    job_setup_time: datetime.timedelta = datetime.timedelta(minutes=10),
) -> tuple[int, datetime.timedelta]:
    """Determine how many job chunks to use (and of what length)."""

    job_requested_time = (
        _get_time_from_sbatch_args(sbatch_args)
        or get_config().get_env_vars(cluster).get("SBATCH_TIME")
        or _get_time_from_job_script_header(job_script)
        # TODO: Inspect the previous job time, or even call claude?
        # or _find_previous_similar_job_duration(job_script, sbatch_args, program_args)
    )
    if not job_requested_time:
        raise ValueError(
            "Could not find a --time value for the job, which is required for auto-chunking!"
        )
    job_requested_time = parse_time_arg(job_requested_time)

    # The chunk size selected should be at least the minimum length given or 3 hours.
    # If the job setup time is significant, then the chunks should be larger to reduce overhead.
    # TODO: Maybe just submit with --test-only with different variants to check in each case?
    chunk_time = max(
        datetime.timedelta(hours=3), min_chunk_time, job_setup_time / MAX_OVERHEAD_RATIO
    )

    # Divide while also taking the setup/checkpointing overhead into account.
    num_chunks_needed = math.ceil(
        (job_requested_time - job_setup_time) / (chunk_time - job_setup_time)
    )
    return num_chunks_needed, chunk_time


def parse_time_arg(time_val: str | None) -> datetime.timedelta:
    if time_val is None:
        raise ValueError(
            "Could not find a --time value for the job, which is necessary for auto-chunking."
        )
    # Parse the sbatch `--time` arg format into a timedelta.
    # it can be something like this: "1-12:30:00" (1 day, 12 hours, 30 minutes)
    # or "12:30:00" (12 hours, 30 minutes), or "30:00" (30 minutes).
    match = re.match(r"(?:(\d+)-)?(\d{1,2}):(\d{2}):(\d{2})", time_val)
    if not match:
        raise ValueError(f"Could not parse --time value: {time_val}")

    return datetime.timedelta(
        days=int(match.group(1) or 0),
        hours=int(match.group(2)),
        minutes=int(match.group(3)),
        seconds=int(match.group(4)),
    )


def replace_time_arg(sbatch_args: list[str], new_time: datetime.timedelta) -> list[str]:
    """Replace the --time arg in the sbatch args with a new value.

    >>> replace_time_arg(['--time=1:00:00', '--partition=main'], datetime.timedelta(minutes=30))
    ['--time=0:30:00', '--partition=main']

    >>> replace_time_arg(['--time', '1:00:00', '--partition=main'], datetime.timedelta(minutes=30))
    ['--time', '0:30:00', '--partition=main']

    >>> replace_time_arg(['-t', '1:00:00', '--partition=main'], datetime.timedelta(minutes=30))
    ['-t', '0:30:00', '--partition=main']

    >>> replace_time_arg(['-t', '1:00:00', '--partition=main', '--time=1:00:00'], datetime.timedelta(minutes=30))
    ['-t', '0:30:00', '--partition=main', '--time=0:30:00']
    """
    new_time_str = str(new_time)
    new_args = []
    skip_next = False
    for i, arg in enumerate(sbatch_args):
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("--time=") or arg.startswith("-t="):
            new_args.append(f"--time={new_time_str}")
        elif arg == "--time" or arg == "-t":
            new_args.append(arg)
            new_args.append(new_time_str)
            skip_next = True
        else:
            new_args.append(arg)
    if not any(a.startswith("--time") or a.startswith("-t") for a in sbatch_args):
        new_args.append(f"--time={new_time_str}")
    return new_args


def _get_time_from_job_script_header(job_script: Path) -> str | None:
    for line in job_script.read_text().splitlines():
        if line.startswith("#SBATCH") and "--time=" in line:
            # This is a line like "#SBATCH --time=1:00:00"
            return line[line.index("--time=") + len("--time=") :].split()[0]
        if not line.strip().startswith("#"):
            # Stop parsing once we reach the first non-comment line.
            return


def _get_time_from_sbatch_args(sbatch_args: list[str]) -> str | None:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("-t", "--time", dest="time", default=None)
    parsed_args, _ = parser.parse_known_args(sbatch_args)
    return getattr(parsed_args, "time", None)


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
            sbatch(
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
    return await sbatch(remote, job_script, sbatch_args, program_args, git_commit=git_commit)


async def submit_first(
    job_script: Path,
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
            sbatch(
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
