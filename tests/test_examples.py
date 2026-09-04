import asyncio
import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.submit import ensure_clean_git_state, get_submissions, submit
from cluv.cli.sync import (
    get_active_remotes,
    sync_common_part,
    sync_per_cluster_part,
)
from cluv.config import load_cluv_config
from cluv.remote import Remote, control_socket_is_running
from cluv.slurm import FAILED_JOB_STATES, clean_job_state, run_sacct
from tests.test_integration import IN_SELF_HOSTED_GITHUB_CI, REQUIRED_CLUSTERS

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

    sbatch_args = [f"--job-name={job_name}", "--time=0:20:00"]
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
        # This test runs with `--use_fake_data`, so it needs none of the ImageNet archives - and
        # the example's `data_source` is on another cluster, so letting the dataset sync run would
        # rsync ~150GB from mila through this machine (the CI runner!) and on to the target
        # cluster, on every single run.
        sync_datasets=False,
    )
    assert job is not None

    should_cancel_job = True
    try:
        # The job script that cluv picked should be the one configured for this cluster.
        assert job.job_script == Path(f"scripts/job_{remote.hostname}.sh")

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


IMAGENET_EXAMPLE_CLUSTERS = load_cluv_config(
    Path(__file__).resolve().parents[1] / "examples/imagenet/pyproject.toml"
).clusters_names

# Trillium's login nodes wrap `sbatch` in a site submission filter that only accepts a whitelist of
# options, and `--test-only` is not on it: it answers "ERROR:   option --test-only not recognized"
# with a usage message that isn't Slurm's own (the `sbatch` behind it reports slurm 25.11.7, which
# does support the flag). Nothing to work around here - a dry run just isn't available there.
CLUSTERS_WITHOUT_SBATCH_TEST_ONLY = {"trillium-gpu"}


@pytest.mark.slow
@pytest.mark.parametrize("cluster", IMAGENET_EXAMPLE_CLUSTERS)
async def test_imagenet_job_script_is_accepted_by_slurm(
    cluster: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`sbatch --test-only` the example's job script on every cluster we're connected to.

    Slurm validates the whole request - partition, account, QoS, and whether the resources asked
    for could ever be satisfied - and reports when the job *would* start, without queueing anything
    or consuming any compute. Cheap enough to do for every cluster the example claims to support,
    unlike `test_imagenet_example` above, which needs a real GPU job per cluster.

    So: this catches "this cluster would reject this job script", which is the failure mode a
    per-cluster job script actually has. Opportunistic: clusters without an active SSH connection
    are skipped - except the `REQUIRED_CLUSTERS` in CI, where a missing connection is a failure,
    same as for the `cluster` fixture the other integration tests use.
    """
    if cluster in CLUSTERS_WITHOUT_SBATCH_TEST_ONLY:
        pytest.skip(f"`sbatch --test-only` is not available on {cluster}.")
    if not await control_socket_is_running(cluster):
        if IN_SELF_HOSTED_GITHUB_CI and cluster in REQUIRED_CLUSTERS:
            pytest.fail(f"No active SSH connection to {cluster}, which must be tested against!")
        pytest.skip(f"Test requires an active SSH connection to {cluster} to run.")
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
    )
    assert submissions

    for submission in submissions:
        # `--parsable` prints the job id of a submitted job, which is meaningless for a dry run;
        # `sbatch` is otherwise given the exact command line a real submission would use.
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
