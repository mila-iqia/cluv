from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
import sys
from pathlib import Path

from cluv.cli.sync import sync
from cluv.config import ClusterConfig, RetryConfig, find_pyproject, get_config
from cluv.remote import Remote
from cluv.utils import console


# SLURM states a job cannot transition out of. Anything else (PENDING, RUNNING,
# COMPLETING, SUSPENDED, REQUEUED, RESIZING, STAGE_OUT, unknown future states)
# is treated as transient so wait-loops default to "keep polling" on unknowns.
TERMINAL_JOB_STATES = [
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
    "SPECIAL_EXIT",
]
FAILED_JOB_STATES = ["FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED"]

# SBATCH_* env vars that have an equivalent `sbatch` CLI flag. We translate
# these to flags before invoking sbatch because some clusters (notably DRAC)
# re-source their site profile inside `bash --login -c`, which can clobber
# SBATCH_* env defaults before sbatch reads them; CLI flags are parsed by
# sbatch directly and survive the login shell. Any SBATCH_* key not in this
# table falls through as a plain env var (preserving existing behavior).
SBATCH_ENV_TO_FLAG: dict[str, str] = {
    "SBATCH_ACCOUNT": "--account",
    "SBATCH_CONSTRAINT": "--constraint",
    "SBATCH_CPUS_PER_TASK": "--cpus-per-task",
    "SBATCH_ERROR": "--error",
    "SBATCH_GRES": "--gres",
    "SBATCH_JOB_NAME": "--job-name",
    "SBATCH_MEM": "--mem",
    "SBATCH_NODES": "--nodes",
    "SBATCH_NTASKS": "--ntasks",
    "SBATCH_OUTPUT": "--output",
    "SBATCH_PARTITION": "--partition",
    "SBATCH_QOS": "--qos",
    "SBATCH_RESERVATION": "--reservation",
    "SBATCH_TIME": "--time",
}

# How often to poll sacct while waiting for a terminal state in the retry loop.
RETRY_POLL_INTERVAL_S = 10


def _split_env_for_sbatch(env_vars: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    """Translate known SBATCH_* env vars into `sbatch` CLI flags; pass the rest through."""
    sbatch_flags: list[str] = []
    remaining: dict[str, str] = {}
    for key, value in env_vars.items():
        flag = SBATCH_ENV_TO_FLAG.get(key)
        if flag is None:
            remaining[key] = value
        else:
            sbatch_flags.append(f"{flag}={shlex.quote(str(value))}")
    return sbatch_flags, remaining


__all__ = ["submit"]


async def submit(
    cluster: str,
    job_script: Path,
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

    cluv_config = get_config()

    if cluster == "first":
        if cluv_config.retry is not None:
            console.print(
                "[red]`cluv submit first` cannot be combined with [tool.cluv.retry]. "
                "See open-question 3 in the OOM-aware resubmit proposal.[/red]"
            )
            return None
        return await submit_first(job_script, sbatch_args, program_args, git_commit)

    # Sync.
    remotes = await sync(clusters=[cluster])

    # Run the sbatch command over SSH.
    remote = remotes[0]
    result = await sbatch(remote, job_script, sbatch_args, program_args, git_commit)

    if result.returncode != 0:
        console.print(f"[red] Error during sbatch : {result.stderr}[/red]")
        return None

    job_id = int(result.stdout.strip())

    console.log(
        f"Successfully submitted job {job_id} on the {cluster} cluster.\n"
        f"Use `ssh {cluster} sacct -j {job_id}` to view its status."
    )

    if cluv_config.retry is None:
        return job_id

    return await _retry_on_oom(
        remote=remote,
        job_id=job_id,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        git_commit=git_commit,
        retry=cluv_config.retry,
    )


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
    sbatch_results = await asyncio.gather(
        *[
            sbatch(
                remote,
                job_script,
                sbatch_args,
                program_args,
                git_commit,
            )
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

                    if job_status and job_status not in TERMINAL_JOB_STATES:
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
                wait_time = min(wait_time*2, 20)
        console.log(
            f"Job {start_job_id} on cluster {start_cluster} is running. Cancelling the other jobs...\n",
            f"Use `ssh {start_cluster} sacct -j {start_job_id}` to view its status.",
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.log("Interrupted by user. Cancelling all jobs...")
    finally:
        await cancel_all_jobs(clusters_to_remote, cluster_to_jobid, start_cluster)

    return start_job_id


async def _wait_terminal(remote: Remote, job_id: int) -> str:
    """Poll sacct until `job_id` reaches a state in `TERMINAL_JOB_STATES`."""
    while True:
        raw = await get_job_status(remote, job_id)
        # sacct may report states like "CANCELLED by 1234"; the first word is enough.
        state = raw.split()[0] if raw else ""
        if state and state in TERMINAL_JOB_STATES:
            return state
        await asyncio.sleep(RETRY_POLL_INTERVAL_S)


async def _retry_on_oom(
    remote: Remote,
    job_id: int,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    retry: RetryConfig,
) -> int | None:
    """OOM-aware resubmit loop layered on top of the single-cluster `submit()` path.

    Polls sacct for `job_id` until terminal. On `OUT_OF_MEMORY`, asks
    `salvo.policy.apply_oom` for the next memory ask, mutates the env-var dict
    passed to `sbatch`, and resubmits. On any other terminal state, returns the
    current `job_id`. Bounded by `retry.max_hops` and by `FailStep` in the policy.
    """
    # Import lazily so users who don't opt in don't pay for pysalvo at import time.
    from salvo.job.spec import JobSpec
    from salvo.policy import OomContext, apply_oom

    env_overrides: dict[str, str] = {}
    hop = 0
    # Track current memory ask through hops. None means "rely on cluster default";
    # in that case bump_mem still works because JobSpec defaults to 4G.
    current_mem = env_overrides.get("SBATCH_MEM") or _initial_mem(remote.hostname)

    while hop < retry.max_hops:
        state = await _wait_terminal(remote, job_id)
        if state != "OUT_OF_MEMORY":
            return job_id

        max_rss_mb = await get_max_rss_mb(remote, job_id)
        spec = JobSpec(
            name="cluv-retry",
            cmd=["sbatch"],
            mem=current_mem,
            on_oom=retry.on_oom,
        )
        new_spec, action = apply_oom(spec, OomContext(kind="cpu", max_rss_mb=max_rss_mb))
        if new_spec is None:
            console.log(f"OOM policy terminated after hop {hop}: {action}")
            return job_id

        hop += 1
        current_mem = new_spec.mem
        env_overrides["SBATCH_MEM"] = current_mem
        env_overrides["CLUV_HOP"] = f"{hop}/{retry.max_hops}"
        console.log(
            f"hop {hop}/{retry.max_hops}: resubmitting on {remote.hostname} with mem={current_mem}"
        )
        result = await sbatch(
            remote, job_script, sbatch_args, program_args, git_commit, env_overrides
        )
        if result.returncode != 0:
            console.print(f"[red]resubmit hop {hop} failed: {result.stderr}[/red]")
            return None
        job_id = int(result.stdout.strip())
        console.log(f"hop {hop}/{retry.max_hops}: submitted as job {job_id}")

    console.log(f"max_hops={retry.max_hops} reached; last job id is {job_id}")
    return job_id


def _initial_mem(cluster: str) -> str:
    """Best-effort read of the configured `SBATCH_MEM` for `cluster`.

    Falls back to "4G" (matching `JobSpec`'s default) when nothing is set, so the
    policy parser has a number to multiply.
    """
    config = get_config()
    merged = {**config.env, **config.clusters.get(cluster, ClusterConfig()).env}
    return merged.get("SBATCH_MEM", "4G")


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
    env_overrides: dict[str, str] | None = None,
) -> str:
    """
    Generate the command to submit the job via sbatch on the remote cluster, with the appropriate env vars set.

    `env_overrides`, when set, is applied after the global + per-cluster `env` merge so
    the retry loop can bump `SBATCH_MEM` / set `CLUV_HOP` between hops without
    touching the on-disk config.
    """
    # Resolve remote job script path.
    project_path = find_pyproject().parent.relative_to(Path.home())
    remote_job_script = f"~/{project_path}/{job_script}"

    # Build env var dict: global SBATCH_* defaults merged with per-cluster overrides.
    config = get_config()
    env_vars: dict[str, str] = {**config.env}
    env_vars.update(config.clusters.get(cluster, ClusterConfig()).env)

    # Prefix the job name with "cluv-" so it is easy to identify cluv-submitted jobs in sacct.
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    if env_overrides:
        env_vars.update(env_overrides)

    sbatch_flags, env_remaining = _split_env_for_sbatch(env_vars)
    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_remaining.items())
    sbatch_flags_str = " ".join(sbatch_flags)
    sbatch_args_str = " ".join(shlex.quote(f) for f in sbatch_args)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={project_path} "
        f"{sbatch_flags_str} {sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


async def sbatch(
    remote: Remote,
    job_script: Path,
    sbatch_args: list[str],
    program_args: list[str],
    git_commit: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Submit the job via sbatch on the remote cluster, and return the job id."""
    cluster = remote.hostname

    remote_cmd = get_sbatch_command(
        cluster, job_script, sbatch_args, program_args, git_commit, env_overrides
    )
    return await remote.run(remote_cmd, display=True, warn=True, hide=True)


async def get_job_status(remote: Remote, job_id: int) -> str:
    """Get the status of the job with the given id on the remote cluster."""
    # --parsable2 prevents sacct from truncating wider state names to 10 chars
    # (e.g. "OUT_OF_ME+" for OUT_OF_MEMORY); we want the full canonical string.
    sacct_command = f"sacct -j {job_id} --format=State --noheader --allocations --parsable2"
    return await remote.get_output(sacct_command)


async def get_max_rss_mb(remote: Remote, job_id: int) -> int | None:
    """Read peak RSS across all steps of `job_id` from sacct, in MiB.

    Returns None if sacct reports no parseable value. `MaxRSS` is a per-step
    metric (the allocation row is blank), so this walks every row and keeps the
    max. Used to populate `salvo.policy.OomContext.max_rss_mb`; `apply_oom` is
    designed to also work with None and fall back to the multiplicative factor.
    """
    sacct_command = f"sacct -j {job_id} --format=MaxRSS --noheader --units=M --parsable2"
    output = await remote.get_output(sacct_command)
    values: list[int] = []
    for line in output.splitlines():
        raw = line.strip().rstrip("M")
        if not raw:
            continue
        try:
            values.append(int(float(raw)))
        except ValueError:
            continue
    return max(values) if values else None


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
