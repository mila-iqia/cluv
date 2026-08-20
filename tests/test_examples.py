import asyncio
import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from cluv.cli.submit import submit
from cluv.cli.sync import get_active_remotes
from cluv.remote import Remote, control_socket_is_running
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct

# TODO: Also run this test on the Mila cluster using the same self-hosted runner setup as in
# mila-docs.


@pytest.mark.slow
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
    if cluster != "first" and not (await control_socket_is_running(cluster)):
        pytest.xfail(f"Need an active connection to {cluster} for this test to run.")

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

    Requires an active SSH connection to the cluster and a clean git tree.
    """
    repo_root = Path(__file__).parent.parent
    monkeypatch.chdir(repo_root / "examples/imagenet")

    # This job name is stable across commits (it comes from the job script's filename, not
    # GIT_COMMIT), so a job still queued under it can only be a leftover from an earlier run of
    # *this same test* on this cluster - clean it up before adding another one to the queue. See
    # `cancel_stale_jobs` for why this can't just be left to that earlier run's own cleanup.
    await cancel_stale_jobs(remote, job_name=f"cluv-job_{remote.hostname}")

    sbatch_args = ["--time=0:20:00"]
    if remote.hostname not in WHOLE_NODE_CLUSTERS:
        sbatch_args += ["--gpus-per-node=1", "--ntasks-per-node=1"]

    job = await submit(
        remote.hostname,
        job_script=None,
        sbatch_args=sbatch_args,
        program_args=[
            "python",
            "main.py",
            "--use_fake_data",
            "--epochs=1",
            "--limit_train_samples=2048",
            "--limit_val_samples=512",
            "--batch_size=64",
            "--model_name=vit_b_32",
            "--no_wandb",
        ],
    )
    assert job is not None

    should_cancel_job = True
    try:
        # The job script that cluv picked should be the one configured for this cluster.
        assert job.job_script == f"scripts/job_{remote.hostname}.sh"

        state = await wait_for_job_to_finish(remote, job.job_id)
        should_cancel_job = False  # it reached a terminal state, so there is nothing to cancel.
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
