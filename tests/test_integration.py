"""Integration tests that require live SSH connections to real clusters.

Run with:
    uv run pytest -m integration -v

Skip with:
    uv run pytest -m "not integration"

These tests connect to real clusters. They will fail if you do not have
active SSH ControlMaster sockets (run `cluv login` first).
"""

import os

import pytest
import pytest_asyncio

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.status import ClusterStatus, get_real_cluster_status
from cluv.cli.submit import submit
from cluv.remote import Remote, control_socket_is_running

pytestmark = pytest.mark.integration
SLURM_CLUSTER = os.environ.get(
    "SLURM_CLUSTER"
)  # for manual test runs, can also be set in the environment


@pytest_asyncio.fixture(
    scope="session",
    params=[SLURM_CLUSTER] if SLURM_CLUSTER else [],
)
async def cluster(request: pytest.FixtureRequest) -> str:
    cluster = getattr(request, "param", None)
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


# ---------------------------------------------------------------------------
# cluv submit — rorqual integration
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
async def test_submit(remote: Remote):
    """End-to-end: actually submit scripts/job.sh to rorqual via sbatch.

    Requires an active SSH connection to rorqual and a clean git tree.
    The project must already be synced on rorqual (sync is mocked out).
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
