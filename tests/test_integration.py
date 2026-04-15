"""Integration tests that require live SSH connections to a real Slurm cluster.

These tests connect to a cluster at the hostname $SLURM_CLUSTER.
They will be skipped if that variable is not set, and fail if they are set and
there is not an active SSH connection to that cluster.
"""

import os

import pytest
import pytest_asyncio

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.status import ClusterStatus, get_real_cluster_status
from cluv.cli.submit import submit
from cluv.remote import Remote, control_socket_is_running

SLURM_CLUSTER = os.environ.get("SLURM_CLUSTER")


@pytest_asyncio.fixture(scope="session")
async def cluster(request: pytest.FixtureRequest) -> str:
    # NOTE: with this `getattr` thing on request, we can also parametrize the cluster fixture to
    # run the same tests on multiple clusters in the same test session, if we want.
    # For example:
    # @pytest.mark.parametrize("cluster", ["mila", "tamia", "rorqual"], indirect=True)
    cluster = getattr(request, "param", SLURM_CLUSTER)
    if cluster is None:
        pytest.skip(
            "No cluster specified. Set the SLURM_CLUSTER environment variable to a "
            "cluster with an active SSH connection to run these tests."
        )
    if not (await control_socket_is_running(cluster)):
        pytest.fail(f"These tests require an active connection to the {cluster} cluster.")
    assert isinstance(cluster, str)
    return cluster


@pytest_asyncio.fixture(scope="session")
async def remote(cluster: str):
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        pytest.xfail(f"Test needs an active SSH connection to the {cluster} cluster.")
    return remote


@pytest_asyncio.fixture(scope="session")
async def cluster_status(remote: Remote):
    return await get_real_cluster_status(remote)


@pytest.mark.asyncio
async def test_status_online(cluster_status: ClusterStatus):
    assert cluster_status.online is True


@pytest.mark.asyncio
async def test_status_has_gpus(cluster_status: ClusterStatus):
    assert cluster_status.gpu_total > 0, "Expected tamia to report GPU nodes"


@pytest.mark.asyncio
async def test_status_gpu_model(cluster_status: ClusterStatus):
    assert cluster_status.gpu_model != "?", f"GPU model not detected: {cluster_status.gpu_model!r}"


@pytest.mark.asyncio
async def test_status_jobs(cluster_status: ClusterStatus):
    # Job counts must be non-negative integers (tamia is a busy cluster)
    assert cluster_status.jobs.running >= 0
    assert cluster_status.jobs.pending >= 0
    assert cluster_status.jobs.my_running >= 0
    assert cluster_status.jobs.my_pending >= 0


@pytest.mark.asyncio
async def test_status_storage(cluster_status: ClusterStatus):
    assert cluster_status.storage.home_quota > 0, "Expected non-zero home quota"
    assert cluster_status.storage.scratch_quota > 0, "Expected non-zero scratch quota"
    assert cluster_status.storage.home_used >= 0
    assert cluster_status.storage.scratch_used >= 0


@pytest.mark.timeout(60)
async def test_submit(remote: Remote):
    """End-to-end: actually submit scripts/job.sh to a slurm cluster via sbatch.

    Requires an active SSH connection to the cluster and a clean git tree.
    Also actually performs a `cluv sync` to that cluster.

    NOTE: This **will** push the current branch to GitHub (since it runs `cluv sync`).
    """
    job_id = await submit(
        cluster=remote.hostname,
        job_script="scripts/job.sh",
        sbatch_args=["--time=00:00:30"],
        program_args=["python", "--version"],
    )
    assert isinstance(job_id, int)
    try:
        job_name = await remote.get_output(
            f"sacct -j {job_id} --format=JobName --noheader --parsable2 | head -1"
        )
        assert job_name.strip().startswith("cluv-")
    finally:
        await remote.run(f"scancel {job_id}")
