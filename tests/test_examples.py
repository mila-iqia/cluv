import asyncio
import datetime
import json
import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.submit import ensure_clean_git_state, get_submissions, submit
from cluv.cli.sync import (
    expandvars,
    get_active_remotes,
    sync_common_part,
    sync_per_cluster_part,
)
from cluv.config import get_cluv_config, load_cluv_config
from cluv.remote import Remote
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct
from tests.test_integration import skip_if_cluster_is_not_testable, skip_unless_connected

# TODO: Also run this test on the Mila cluster using the same self-hosted runner setup as in
# mila-docs.


@pytest.mark.slow
@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "cluster",
    [
        "mila",
        pytest.param(
            "rorqual",
            marks=pytest.mark.xfail(
                reason="Rorqual might take a long time for the job to actually run."
            ),
        ),
        "first",
    ],
)
@pytest.mark.parametrize(
    "job_script",
    [
        "scripts/job.sh",
        pytest.param(
            "scripts/safe_job.sh",
            marks=pytest.mark.xfail(reason="TODO: safe_job.sh script needs to be adjusted."),
        ),
    ],
)
async def test_hydra_example(
    cluster: str, monkeypatch: pytest.MonkeyPatch, job_script: str
) -> None:
    """End-to-end: actually run the hydra example.

    Requires an active SSH connection to the cluster and a clean git tree.
    Also actually performs a `cluv sync` to that cluster.
    """
    if cluster != "first":
        # Same gate as the `cluster` fixture the imagenet test below uses: in CI, only the
        # REQUIRED_CLUSTERS are tested, so a stray SSH connection on the runner can't make CI
        # opportunistically submit jobs to some other (possibly very slow) cluster.
        await skip_if_cluster_is_not_testable(cluster)

    if cluster == "first" and not (await get_active_remotes()):
        pytest.fail(
            "Need at least one active connection to a cluster for the `cluster=first` test case to make sense!"
        )

    repo_root = Path(__file__).parent.parent
    monkeypatch.chdir(repo_root / "examples/hydra_example")

    subprocess_result = subprocess.run(
        f"uv run python main.py --multirun launcher=cluv hydra.launcher.cluster={cluster} "
        f"hydra.launcher.job_script={job_script} lr=0.1,0.2",
        shell=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    output = subprocess_result.stdout + "\nSTDERR:\n" + subprocess_result.stderr
    print(f"Output of hydra example:\n{output}")
    # Very simple: Check that this portion of the table, near the end, shows each run as completed.
    assert re.search(r"lr=0\.1\s+│\s+COMPLETED", output)
    assert re.search(r"lr=0\.2\s+│\s+COMPLETED", output)
    assert subprocess_result.returncode == 0


# Clusters whose Slurm partition requires a job to take every GPU of the node(s) it lands on
# (see the comments in their `scripts/job_<cluster>.sh`). Everywhere else, `--gpus-per-node=1`
# `--ntasks-per-node=1` overrides the per-cluster job script's GPU count so this smoke test uses as
# little compute as will actually exercise the distributed code path (rank 0 + rank-0-only logic).
WHOLE_NODE_CLUSTERS = {"tamia"}


@pytest.mark.slow
@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "cluster",
    ["mila", "tamia", "rorqual", "fir", "nibi"],
    indirect=True,
)
async def test_imagenet_example(remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: submit the ImageNet example on a cluster with its own job script.

    Uses `--use_fake_data` so this doesn't need the (~150GB) ImageNet archives to have been synced
    to the cluster, and doesn't pass a job script, so that the per-cluster `job_script_path` from
    the example's `[tool.cluv.clusters.<cluster>]` config is the thing being exercised.

    Set `CLUV_CI_REAL_DATA=1` to exercise the real dataset path instead (staging the archives and
    training on them). That's the only mode that needs `datasets_path` populated on the cluster,
    and the only one that can move ~150GB around, so it's opt-in and manual by design.

    Requires an active SSH connection to the cluster and a clean git tree.
    """
    use_real_data = os.environ.get("CLUV_CI_REAL_DATA", "0") == "1"
    repo_root = Path(__file__).parent.parent
    monkeypatch.chdir(repo_root / "examples/imagenet")

    # Without --job-name, cluv names the job after the job script's filename alone (see
    # `base_name` in `get_sbatch_command`) - no per-project qualifier, so any other project whose
    # job script happens to share that filename (e.g. another `job_tamia.sh`) would get the exact
    # same name on the same cluster. Pinning an explicit, test-specific name avoids that, and
    # doubles as the identity `cancel_stale_jobs` sweeps on below: it's stable across commits (it
    # doesn't depend on GIT_COMMIT), so anything still queued under it can only be a leftover from
    # an earlier run of *this same test* on this cluster - see `cancel_stale_jobs` for why that
    # can't just be left to that earlier run's own cleanup.
    job_name = f"cluv-ci-imagenet-example-{remote.hostname}"
    await cancel_stale_jobs(remote, job_name=job_name)

    # --time: 10min is plenty for this smoke test (the longest verified run in the README is
    # under 9min with the real dataset, and this one uses fake data), and short jobs get
    # backfilled sooner, so the test spends less time waiting in the queue.
    # --no-requeue: the example's config sets `requeue = true` so that real training runs survive
    # preemption. For a CI job that's the wrong tradeoff - a preempted job we've stopped watching
    # would come back and ask for a GPU node all over again.
    sbatch_args = [f"--job-name={job_name}", "--time=0:10:00", "--no-requeue"]
    if remote.hostname not in WHOLE_NODE_CLUSTERS:
        sbatch_args += ["--gpus-per-node=1", "--ntasks-per-node=1"]

    job = await submit(
        remote.hostname,
        job_script=None,
        sbatch_args=sbatch_args,
        program_args=[
            "python",
            "main.py",
            *([] if use_real_data else ["--use_fake_data"]),
            "--epochs=1",
            # Same --batch_size as a real training run (see "The real thing" in the README), and
            # enough --limit_train_samples at that batch size for at least 5 `train/*` logs in
            # wandb (32 batches / --logging_interval=5 = 7, verified locally) - not the full
            # 100_000 a real run uses, since none of that is needed just to check the logging
            # cadence and this should stay fast.
            "--limit_train_samples=8192",
            "--limit_val_samples=2048",
            "--batch_size=256",
            "--logging_interval=5",
            "--model_name=vit_b_32",
            "--no_wandb",
        ],
        # With `--use_fake_data` there are no archives to stage, and the example's `data_source`
        # is on another cluster, so letting the dataset sync run would rsync ~150GB from mila
        # through this machine (the CI runner!) and on to the target cluster, every single run.
        sync_datasets=use_real_data,
    )
    assert job is not None

    should_cancel_job = True
    try:
        # The job script that cluv picked should be the one configured for this cluster.
        assert job.job_script == Path(f"scripts/job_{remote.hostname}.sh")

        state = await wait_for_job_to_finish(remote, job.job_id)
        should_cancel_job = False  # it reached a terminal state, so there is nothing to cancel.
        await record_run(remote, job.job_id, state, real_data=use_real_data)
        assert state.startswith("COMPLETED"), state
    finally:
        if should_cancel_job:
            # The job is still queued or running: it either outlasted the timeout in
            # `wait_for_job_to_finish` (which skips the test) or something above it failed. Either
            # way nothing is watching it any more, so don't leave it on the cluster - these jobs ask
            # for most of a GPU node, and this example's config sets `requeue = true`, so a
            # preempted orphan comes back rather than dying.
            print(f"Cancelling job {job.job_id} on {remote.hostname}.")
            await remote.run(f"scancel {job.job_id}", warn=True, hide=True, display=True)


IMAGENET_EXAMPLE_CLUSTERS = load_cluv_config(
    Path(__file__).resolve().parents[1] / "examples/imagenet/pyproject.toml"
).clusters_names


@pytest.mark.slow
@pytest.mark.parametrize("cluster", IMAGENET_EXAMPLE_CLUSTERS)
async def test_imagenet_job_script_is_accepted_by_slurm(
    cluster: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`sbatch --test-only` the imagenet example's job script on every configured cluster.

    Slurm validates the whole request - partition, account, QoS, and whether the resources asked
    for could ever be satisfied - and reports when the job *would* start, without queueing
    anything or consuming any compute. That makes this cheap enough to run for *all* the clusters
    the example claims to support on every PR, which the end-to-end tests (a real job, run to
    completion, marked `end_to_end`) are far too expensive to do.

    So: this catches "this cluster would reject this job script"; the `end_to_end` tests catch
    "the job actually runs and trains". Skipped for clusters this machine isn't connected to.
    """
    await skip_unless_connected(cluster)
    remote = await get_remote_without_2fa_prompt(cluster)
    assert remote is not None

    repo_root = Path(__file__).parent.parent
    monkeypatch.chdir(repo_root / "examples/imagenet")

    git_commit = ensure_clean_git_state()
    # The job script has to exist *on the cluster* for `sbatch` to read its header, so the project
    # does need to be synced - but the ~150GB of ImageNet archives emphatically do not.
    await sync_common_part([remote], sync_datasets=False)
    await sync_per_cluster_part(remote, sync_datasets=False)

    # No sbatch flags and no job script: exactly what a plain `cluv submit <cluster>` would ask
    # for, i.e. the per-cluster `job_script_path` and `sbatch_args` from the config.
    submissions = get_submissions(
        cluster,
        remote,
        job_script=None,
        sbatch_args=[],
        program_args=["python", "main.py", "--use_fake_data"],
        chunking=None,
        git_commit=git_commit,
        results_path=await expandvars(
            remote, get_cluv_config().get_cluster_config(cluster).results_path
        ),
    )
    assert submissions

    for submission in submissions:
        # `--parsable` prints the job id of a submitted job, which is meaningless here; `sbatch`
        # is otherwise given the exact command line a real submission would use. Issue #193 tracks
        # doing this through a `--test-only` flag on `cluv submit` itself instead.
        assert "sbatch --parsable " in submission.sbatch_command
        test_only_command = submission.sbatch_command.replace(
            "sbatch --parsable ", "sbatch --test-only ", 1
        )
        result = await remote.run(test_only_command, warn=True, hide=True, display=True)
        output = ((result.stdout or "") + (result.stderr or "")).strip()
        assert result.returncode == 0, f"Slurm on {cluster} rejected the job script:\n{output}"
        # A successful dry run says when the job would start; anything else means `--test-only`
        # didn't do what we think it does on this cluster, and this test would be vacuous.
        assert "to start at" in output, f"Unexpected `sbatch --test-only` output:\n{output}"


async def record_run(remote: Remote, job_id: int, state: str, real_data: bool) -> None:
    """Write one line of "this example ran here, and here is the job to prove it" to disk.

    Only does anything when `$CLUV_CI_RESULTS_DIR` is set, which the end-to-end workflow does.
    `.github/scripts/summarize_example_runs.py` turns the files into the per-cluster table that
    gets published, so that the "verified on" list in the example's README stops being something
    a human has to remember to update.
    """
    results_dir = os.environ.get("CLUV_CI_RESULTS_DIR")
    if not results_dir:
        return
    elapsed = str(
        await run_sacct(remote, job_id, format="Elapsed", additional_args="--noconvert")
    ).strip()
    record = {
        "cluster": remote.hostname,
        "job_id": job_id,
        "state": state,
        "elapsed": elapsed.splitlines()[0] if elapsed else "?",
        "real_data": real_data,
        "commit": subprocess.getoutput("git rev-parse --short HEAD").strip(),
        "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    }
    path = Path(results_dir) / f"imagenet-{remote.hostname}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2))


async def cancel_stale_jobs(remote: Remote, job_name: str) -> None:
    """Cancel any jobs already queued under `job_name`, left over from an earlier test run.

    This test's own `finally` block already cancels its job once it's done with it - but on a
    self-hosted CI runner with `concurrency: cancel-in-progress: true`, a *newer* push cancels the
    whole previous workflow run, including this test's process, before that `finally` gets to
    await its `scancel`. On a cluster with a deep enough queue (jobs can sit PENDING for hours),
    that leaves an orphaned job requesting a full node behind for every push, indefinitely - there
    is nothing left running that could ever cancel it. A process that's being killed can't clean
    up after itself, so the cleanup has to happen at the *start* of the next run instead.
    """
    job_ids = (
        await remote.get_output(f"squeue -h -u $USER -n {job_name} -o %i", warn=True)
    ).split()
    if job_ids:
        print(
            f"Cancelling {len(job_ids)} stale job(s) named {job_name} on {remote.hostname}: {job_ids}"
        )
        await remote.run(f"scancel {' '.join(job_ids)}", warn=True, hide=True, display=True)


async def wait_for_job_to_finish(remote: Remote, job_id: int, timeout_minutes: int = 30) -> str:
    """Poll `sacct` until the job reaches a terminal state, and return that state."""
    deadline = time.time() + timeout_minutes * 60
    state = "UNKNOWN"
    while time.time() < deadline:
        await asyncio.sleep(10)
        sacct_output = (await run_sacct(remote, job_id)).strip().splitlines()
        if not sacct_output:
            continue  # the job hasn't shown up in the accounting database yet.
        state = clean_job_state(sacct_output[0])
        print(f"Job {job_id} on {remote.hostname}: {state}")
        if state.startswith(("COMPLETED", "CANCELLED")) or state in FAILED_JOB_STATES:
            return state
    pytest.skip(
        f"Job {job_id} on {remote.hostname} did not finish within {timeout_minutes}min; "
        f"it will be cancelled."
    )
