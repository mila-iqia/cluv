import asyncio
import dataclasses
import datetime
import logging
import os
import shlex
from pathlib import Path, PurePosixPath
from typing import Literal

import rich.table
import rich.text
from rich.live import Live

from cluv.cache import ProjectStateOnCluster, read_cache, write_cache
from cluv.cli.login import login
from cluv.cli.submit import ensure_clean_git_state, sbatch_args_from_dict
from cluv.cli.submit_utils.chunking import get_time_from_job_script_header
from cluv.cli.sync import (
    _push_datasets_to_remote,
    clone_project,
    expandvars,
    fetch_results,
    get_active_remotes,
    install_uv,
    pull_datasets_if_needed,
    run_git_push_if_needed,
    run_uv_sync,
)
from cluv.config import SbatchArgs, get_cluv_config
from cluv.remote import Remote, run
from cluv.slurm import FAILED_JOB_STATES, parse_slurm_time, run_saccts
from cluv.utils import console, current_cluster, find_pyproject

logger = logging.getLogger(__name__)
JobId = str
JobState = str


class JobSubmissionFailed(Exception):
    """Raised when a job submission fails."""


@dataclasses.dataclass(frozen=True, unsafe_hash=True, kw_only=True)
class Submission:
    """One job to submit: on which cluster, over which connection, and with which sbatch flags."""

    cluster: str

    remote: Remote | None
    """Remote used to run `sbatch`, or `None` to run it on the current cluster."""

    job_script: Path

    sbatch_args: SbatchArgs
    """The sbatch flags from the config to use, i.e. one of the cluster's allocations."""

    program_args: list[str]

    sbatch_command: str

    num_chunks: int | None


@dataclasses.dataclass(frozen=True, unsafe_hash=True, kw_only=True)
class Job(Submission):
    """A Job on a Slurm cluster. This object is returned by `cluv submit`."""

    job_id: int
    submitted_at: datetime.datetime


@dataclasses.dataclass
class JobRow:
    """Live, mutable view of one `Submission`'s progress.

    Tracks a submission from before it's even synced (``state="SYNCING"``, no job id yet),
    through submission (``job_id`` known, state polled from `sacct`), to running and,
    possibly, cancellation.

    A single flat list of these -- covering every submission, on every cluster, for one
    `submit()` call -- is all a live display needs to render the whole picture, from a plain
    `rich.Live` table (see `render_job_table` below) up to, eventually, a table shared across
    several concurrent `submit`/`submit_first` calls (`rich.Live` only supports one live region
    per console, so that would fuse several such lists together instead of replacing this one).
    """

    submission: Submission
    state: JobState = "SYNCING"
    job_id: int | None = None
    submitted_at: datetime.datetime | None = None
    error: str | None = None

    @property
    def cluster(self) -> str:
        return self.submission.cluster

    def as_job(self) -> Job | None:
        """A `Job` snapshot of this row, once it has actually been submitted."""
        if self.job_id is None or self.submitted_at is None:
            return None
        return Job(
            cluster=self.submission.cluster,
            remote=self.submission.remote,
            job_script=self.submission.job_script,
            sbatch_args=self.submission.sbatch_args,
            program_args=self.submission.program_args,
            sbatch_command=self.submission.sbatch_command,
            num_chunks=self.submission.num_chunks,
            job_id=self.job_id,
            submitted_at=self.submitted_at,
        )


def _state_style(state: JobState) -> str:
    if state.startswith(("RUNNING", "COMPLETED", "CANCELLED")):
        return "green"
    if state.startswith(("SYNCING", "SUBMITTING", "PENDING", "UNKNOWN")):
        return "yellow"
    return "red"


def render_job_table(rows: list[JobRow], *, cancelling: bool = False) -> rich.table.Table:
    """Render the current state of every submission as a single table.

    A plain `rich.Live` for now; the natural place to plug in a registry that fuses several
    concurrent `submit`/`submit_first` calls' rows into one shared live region later on.
    """
    title = "Waiting for jobs to cancel..." if cancelling else "Submitting jobs..."
    table = rich.table.Table("Cluster", "Job ID", "Status", title=title)
    for row in rows:
        table.add_row(
            row.cluster,
            str(row.job_id) if row.job_id is not None else "-",
            rich.text.Text(row.state, style=_state_style(row.state)),
        )
    return table


def _group_by_cluster(rows: list[JobRow]) -> dict[str, list[JobRow]]:
    grouped: dict[str, list[JobRow]] = {}
    for row in rows:
        grouped.setdefault(row.cluster, []).append(row)
    return grouped


async def submit(
    cluster: str,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    autocommit: bool,
    chunking: int | None,
    _skip_sync: bool = False,
) -> Job | None:
    """Submit a job to the given cluster (or all clusters if `cluster=="first"`),
    and return the Job object if successful.

    Returns None if the submission failed.
    """
    git_commit = ensure_clean_git_state(autocommit=autocommit)
    cluster_to_remote = await get_cluster_to_remote(cluster)

    rows = [
        JobRow(submission=submission)
        for cluster_name, remote in cluster_to_remote.items()
        for submission in get_submissions(
            cluster_name,
            remote,
            job_script=job_script,
            sbatch_args=sbatch_args,
            program_args=program_args,
            chunking=chunking,
            git_commit=git_commit,
        )
    ]

    if not _skip_sync:
        await sync_common_part(list(cluster_to_remote.values()))

    found_running_job = asyncio.Event()
    tasks = [
        asyncio.create_task(
            submit_to_cluster(
                cluster_name,
                remote,
                rows=[row for row in rows if row.cluster == cluster_name],
                found_running_job=found_running_job,
                _skip_sync=_skip_sync,
            )
        )
        for cluster_name, remote in cluster_to_remote.items()
    ]

    cancelling = False

    def _render() -> rich.table.Table:
        return render_job_table(rows, cancelling=cancelling)

    with Live(get_renderable=_render, console=console, refresh_per_second=1):
        first_running_row = await wait_for_first_running_job(
            rows, cluster_to_remote, tasks, found_running_job
        )
        if first_running_row is None:
            console.log("All job submissions have failed! Exiting.")
            return None

        cancelling = True
        other_rows = [
            row for row in rows if row is not first_running_row and row.job_id is not None
        ]
        await wait_for_jobs_to_cancel(other_rows, cluster_to_remote)

    console.print(
        f"[green]Job {first_running_row.job_id} on cluster {first_running_row.cluster} is "
        f"running.[/green]"
    )
    return first_running_row.as_job()


async def wait_for_first_running_job(
    rows: list[JobRow],
    cluster_to_remote: dict[str, Remote | None],
    tasks: list[asyncio.Task],
    found_running_job: asyncio.Event,
    max_wait_time_seconds: int = 60,
) -> JobRow | None:
    """Poll `sacct` until one submitted job starts running, or every submission has failed.

    Mutates `rows` in place with the latest known job id / state, so a live display can render
    them at any point during this wait. Sets `found_running_job` the moment a job starts, so
    that clusters which haven't submitted their own jobs yet can skip doing so.

    Returns the row for the job that started, or None if every submission ended up failing.
    """
    delay = 1
    while True:
        submitted = [row for row in rows if row.job_id is not None]
        by_cluster = _group_by_cluster(submitted)
        if by_cluster:
            states_per_cluster = await asyncio.gather(
                *(
                    run_saccts(cluster_to_remote[cluster], [row.job_id for row in cluster_rows])
                    for cluster, cluster_rows in by_cluster.items()
                )
            )
            for cluster_rows, states in zip(by_cluster.values(), states_per_cluster):
                for row, state in zip(cluster_rows, states):
                    row.state = state
                    if state.startswith(("RUNNING", "COMPLETED")):
                        found_running_job.set()
                        return row

        all_failed = bool(submitted) and all(
            row.state.startswith(tuple(FAILED_JOB_STATES)) for row in submitted
        )
        if all(task.done() for task in tasks) and (not submitted or all_failed):
            return None

        await asyncio.sleep(delay)
        delay = min(delay * 2, max_wait_time_seconds)


async def wait_for_jobs_to_cancel(
    rows: list[JobRow],
    cluster_to_remote: dict[str, Remote | None],
    max_wait_time_seconds: int = 60,
) -> None:
    """Cancel every (already-submitted) job in `rows`, and wait until they're all done."""
    to_cancel = [row for row in rows if not row.state.startswith(("CANCELLED", "COMPLETED"))]
    if not to_cancel:
        return

    await run_scancel(to_cancel)

    delay = 1
    while to_cancel:
        by_cluster = _group_by_cluster(to_cancel)
        states_per_cluster = await asyncio.gather(
            *(
                run_saccts(cluster_to_remote[cluster], [row.job_id for row in cluster_rows])
                for cluster, cluster_rows in by_cluster.items()
            )
        )
        for cluster_rows, states in zip(by_cluster.values(), states_per_cluster):
            for row, state in zip(cluster_rows, states):
                if state.startswith("CANCELLED") or state == "FAILED":
                    # "CANCELLED by <uid>", and a stray "FAILED" job step on some clusters
                    # (while the rest of the job is "CANCELLED"), both just mean cancelled.
                    state = "CANCELLED"
                row.state = state

        to_cancel = [
            row for row in to_cancel if not row.state.startswith(("CANCELLED", "COMPLETED"))
        ]
        if to_cancel:
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_wait_time_seconds)

    console.log(f"Cancelled {len(rows)} other job submission(s).")


async def run_scancel(rows: list[JobRow]) -> None:
    """Cancel the (already-submitted) jobs behind `rows`, grouped by remote."""
    if not rows:
        return
    by_remote: dict[Remote | None, list[JobRow]] = {}
    for row in rows:
        by_remote.setdefault(row.submission.remote, []).append(row)

    async def cancel(remote: Remote | None, cluster_rows: list[JobRow]) -> None:
        job_ids = [row.job_id for row in cluster_rows]
        scancel_command = f"scancel {' '.join(map(str, job_ids))}"
        if remote is not None:
            await remote.get_output(scancel_command, hide=True)
        else:
            await run(tuple(shlex.split(scancel_command)), hide=True)

    await asyncio.gather(
        *(cancel(remote, cluster_rows) for remote, cluster_rows in by_remote.items())
    )


async def submit_to_cluster(
    cluster: str,
    remote: Remote | None,
    rows: list[JobRow],
    found_running_job: asyncio.Event,
    _skip_sync: bool = False,
) -> None:
    """Sync then submit every submission for one cluster, in parallel."""
    if not _skip_sync:
        await sync_per_cluster_part(remote)

    if found_running_job.is_set():
        # If a job has already started on another cluster, we don't need to submit more jobs.
        console.log(
            f"Skipping submission of jobs to cluster {cluster} because a job "
            f"has already started on another cluster."
        )
        for row in rows:
            row.state = "SKIPPED"
        return

    for row in rows:
        row.state = "SUBMITTING"

    results = await asyncio.gather(
        *(submit_job(row.submission) for row in rows),
        return_exceptions=True,
    )

    assert len(results) == len(rows)
    for row, result in zip(rows, results):
        if isinstance(result, Job):
            row.job_id = result.job_id
            row.submitted_at = result.submitted_at
            row.state = "PENDING"
        elif isinstance(result, JobSubmissionFailed):
            row.error = str(result)
            row.state = "FAILED"
            console.log(f"[red]{result}[/red]")
        else:
            assert isinstance(result, BaseException)
            raise result


def get_submissions(
    cluster: str,
    remote: Remote | None,
    *,
    job_script: Path | None,
    sbatch_args: list[str],
    program_args: list[str],
    chunking: int | None,
    git_commit: str,
) -> list[Submission]:
    """Expand the possible job configurations for a cluster. Returns a list of `Submission` objects.

    Does *not* do the actual job submission with `sbatch`.
    """
    submissions: list[Submission] = []
    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)
    job_resources_options = cluster_config.sbatch_args

    if job_script is None:
        if cluster_config.job_script_path is None:
            raise ValueError(
                f"No job script specified for cluster {cluster!r}, and no default job script "
                f"path set in the config."
            )
        job_script = Path(os.path.expandvars(str(cluster_config.job_script_path)))
    if not job_script.exists():
        raise ValueError(
            f"The job script ({job_script}) does not exist on this machine. Even though it "
            f"can be customized per cluster, it needs to exist locally, since cluv needs to "
            f"read its header to infer sbatch defaults."
        )

    for job_resources in job_resources_options:
        job_sbatch_args: SbatchArgs = merge_sbatch_args(
            from_config=job_resources, from_cli=sbatch_args
        )
        num_chunks, job_sbatch_args = apply_chunking(
            job_sbatch_args, job_script=job_script, chunking=chunking
        )
        sbatch_command = get_sbatch_command(
            cluster,
            job_script=job_script,
            sbatch_args=job_sbatch_args,
            program_args=program_args,
            git_commit=git_commit,
        )
        submissions.append(
            Submission(
                cluster=cluster,
                remote=remote,
                job_script=job_script,
                sbatch_args=job_sbatch_args,
                program_args=program_args,
                sbatch_command=sbatch_command,
                num_chunks=num_chunks,
            )
        )
    return submissions


def merge_sbatch_args(from_config: SbatchArgs, from_cli: list[str]) -> SbatchArgs:
    """Merge the sbatch args from the config and from the CLI, with CLI args taking precedence."""
    merged = dict(from_config)
    index = 0
    while index < len(from_cli):
        flag = from_cli[index]
        if flag.startswith("--"):
            key, _, value = flag[2:].partition("=")
            merged[key] = value if value else True
            index += 1
        elif flag.startswith("-"):
            key = flag[1:]
            has_separate_value = index + 1 < len(from_cli) and not from_cli[index + 1].startswith(
                "-"
            )
            if has_separate_value:
                merged[key] = from_cli[index + 1]
                index += 2
            else:
                merged[key] = True
                index += 1
        else:
            raise ValueError(f"Not a valid sbatch flag: {flag!r}")
    return merged


def apply_chunking(
    sbatch_args: SbatchArgs, job_script: Path | None, chunking: int | None
) -> tuple[int | None, SbatchArgs]:
    """Split a job into consecutive chunks of `chunking` hours each, if requested.

    Returns the number of chunks (`None` when `chunking` is `None`, i.e. chunking is disabled)
    and `sbatch_args` updated with the `--time`/`--array` directives needed to run them.
    """
    if chunking is None:
        return None, sbatch_args

    time_limit = sbatch_args.get("time") or sbatch_args.get("t")
    if not time_limit and job_script is not None:
        time_limit = get_time_from_job_script_header(job_script)
    if not time_limit:
        raise ValueError(
            "Could not find a time value for the job, which is required for chunking."
        )

    total_hours = parse_slurm_time(str(time_limit)).total_seconds() / 3600
    n_chunks = max(int((total_hours + chunking - 1) // chunking), 1)
    logger.info(f"Chunking job into {n_chunks} smaller jobs of {chunking} hours each.")

    sbatch_args = {k: v for k, v in sbatch_args.items() if k not in ("time", "t")}
    sbatch_args["time"] = f"{chunking}:00:00"
    sbatch_args["array"] = f"0-{n_chunks - 1}%1"
    return n_chunks, sbatch_args


def get_sbatch_command(
    cluster: str,
    job_script: Path,
    sbatch_args: SbatchArgs,
    program_args: list[str],
    git_commit: str,
) -> str:
    """Generate the command to submit the job via `sbatch` on `cluster`, with the appropriate
    sbatch arguments, environment variables and paths set.
    """
    local_project_dir = find_pyproject().parent
    local_job_script = job_script if job_script.is_absolute() else local_project_dir / job_script
    job_script_relative_path = local_job_script.relative_to(local_project_dir)

    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)
    remote_project_dir = cluster_config.project_dir or (
        PurePosixPath("$HOME") / local_project_dir.relative_to(Path.home())
    )
    remote_job_script = PurePosixPath(remote_project_dir) / job_script_relative_path

    env_vars: dict[str, str] = {**config.env, **cluster_config.env}
    base_name = env_vars.get("SBATCH_JOB_NAME") or Path(job_script).stem
    env_vars["SBATCH_JOB_NAME"] = f"cluv-{base_name}"
    env_vars["GIT_COMMIT"] = git_commit

    sbatch_flags = sbatch_args_from_dict(sbatch_args)
    if not any("--output" in flag for flag in sbatch_flags):
        # Chunked (job array) jobs need `%A`/`%a` (array job id / task id) instead of `%j`.
        if "array" in sbatch_args:
            env_vars["SBATCH_OUTPUT"] = (
                f"{cluster_config.results_path}/{cluster}_%A/slurm-%A_%a.out"
            )
        else:
            env_vars["SBATCH_OUTPUT"] = f"{cluster_config.results_path}/{cluster}_%j/slurm-%j.out"

        header_output = next(
            (
                line
                for line in local_job_script.read_text().splitlines()
                if line.strip().startswith("#SBATCH") and "--output" in line
            ),
            None,
        )
        if header_output is not None:
            logger.warning(
                f"[yellow]The job script {job_script} sets {header_output.strip()!r}, which "
                f"will be overridden by cluv's SBATCH_OUTPUT so that results can be synced "
                f"back. Consider using cluv in your Python script to decide where to store "
                f"results instead.[/yellow]"
            )

    env_vars_prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_vars.items())
    sbatch_args_str = shlex.join(sbatch_flags)
    program_args_str = shlex.join(program_args)

    return (
        f"bash --login -c '{env_vars_prefix} sbatch --parsable --chdir={remote_project_dir} "
        f"{sbatch_args_str} {remote_job_script} {program_args_str}'"
    )


async def submit_job(submission: Submission) -> Job:
    """Does the actual sbatch call.

    Raise a `JobSubmissionFailed` if the job submission fails for some reason.
    """
    if submission.remote is not None:
        result = await submission.remote.run(submission.sbatch_command, warn=True, hide=True)
    else:
        result = await run(tuple(shlex.split(submission.sbatch_command)), warn=True, hide=True)

    if result.returncode != 0:
        raise JobSubmissionFailed(
            f"Failed to submit job on cluster {submission.cluster}: "
            f"{result.stderr or result.stdout}"
        )

    return Job(
        cluster=submission.cluster,
        remote=submission.remote,
        job_script=submission.job_script,
        sbatch_args=submission.sbatch_args,
        program_args=submission.program_args,
        sbatch_command=submission.sbatch_command,
        num_chunks=submission.num_chunks,
        job_id=int(result.stdout.strip()),
        submitted_at=datetime.datetime.now(),
    )


async def new_sync(clusters: list[str] | None = None):
    cluster_to_remote = await get_cluster_to_remote(clusters)
    remotes = list(cluster_to_remote.values())
    await sync_common_part(remotes)
    await asyncio.gather(*[sync_per_cluster_part(remote) for remote in remotes])


async def sync_common_part(cluster_remotes: list[Remote | None]) -> None:
    """Sync steps that only need to happen once, regardless of how many clusters we submit to:
    push the local commit, and pull the dataset from its source cluster if needed.
    """
    config = get_cluv_config()
    remotes = [remote for remote in cluster_remotes if remote is not None]
    await run_git_push_if_needed()
    await pull_datasets_if_needed(current_cluster(), config, remotes)


async def sync_per_cluster_part(cluster_remote: Remote | None) -> None:
    """Sync steps specific to one cluster: install uv, clone/update the project, `uv sync`,
    fetch back new results, and push datasets to it if needed.

    Does nothing when `cluster_remote` is None (submitting from the current cluster itself),
    since there's nothing to sync to it.
    """
    if cluster_remote is None:
        return

    remote = cluster_remote
    cluster = remote.hostname
    config = get_cluv_config()
    cluster_config = config.get_cluster_config(cluster)

    project_path = cluster_config.project_dir
    if project_path is None:
        local_project_dir = find_pyproject().parent
        if not local_project_dir.is_relative_to(Path.home()):
            raise RuntimeError(
                f"Project path is not set for cluster {cluster!r} in the Cluv config, and the "
                f"project root ({local_project_dir}) is not under $HOME. Please set "
                f"`cluv.project_dir` in the Cluv config section of pyproject.toml."
            )
        project_path = PurePosixPath("$HOME") / local_project_dir.relative_to(Path.home())
    project_path = await expandvars(remote, project_path)

    project_state = read_cache().project_states.get(cluster) or ProjectStateOnCluster()

    def _save() -> None:
        cache = read_cache()
        cache.project_states[cluster] = project_state
        write_cache(cache)

    await install_uv(remote, project_state)
    _save()
    await clone_project(remote, project_path=project_path, project_state=project_state)
    _save()
    await run_uv_sync(remote, project_path, project_state)
    _save()
    new_runs = await fetch_results(remote, config, project_state)
    _save()
    if new_runs:
        console.log(f"[green]Fetched {len(new_runs)} new run(s) from {cluster}.[/green]")

    if config.data_source:
        here = current_cluster()
        if ":" not in config.data_source:
            local_dataset_path = Path(os.path.expandvars(config.data_source))
        else:
            local_dataset_path = (
                config.get_cluster_config(here) if here else config
            ).datasets_path
            if not local_dataset_path:
                raise RuntimeError("data_source is set, so datasets_path should also be set!")
            local_dataset_path = Path(os.path.expandvars(str(local_dataset_path)))
        await _push_datasets_to_remote(local_dataset_path, remote, config, project_state)
        _save()


async def get_cluster_to_remote(
    cluster: Literal["first"] | str | list[str] | None,
) -> dict[str, Remote | None]:
    cluster_to_remote: dict[str, Remote | None] = {
        remote.hostname: remote for remote in (await get_active_remotes())
    }
    if here := current_cluster():
        cluster_to_remote[here] = None
    if cluster == "first" or cluster is None:
        return cluster_to_remote

    clusters = [cluster] if isinstance(cluster, str) else cluster
    missing_clusters = [c for c in clusters if c not in cluster_to_remote]
    if missing_clusters:
        remotes = await login(missing_clusters)
        assert remotes
        for remote in remotes:
            cluster_to_remote[remote.hostname] = remote

    return {cluster: cluster_to_remote[cluster] for cluster in clusters}
