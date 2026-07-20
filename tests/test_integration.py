"""Integration tests that require live SSH connections to a real Slurm cluster.

TODO: Do we prefer having tests for only one remote cluster at a time, in different CI steps?
Or have tests for every cluster in the same pytest session?
--> Choosing to have tests for all clusters in the same test session for now. This is more
efficient, since at some point there might be like 10 different clusters, and 10 CI steps to run.
"""

import asyncio
import os
import re
import stat
import subprocess
from pathlib import Path

import milatools.cli.init_command
import pytest
import pytest_asyncio

from cluv.cache import Job
from cluv.cli.init import init
from cluv.cli.login import login
from cluv.cli.status import ClusterStatus, get_cluster_status
from cluv.cli.submit import submit
from cluv.cli.sync import sync
from cluv.config import get_cluv_config, load_cluv_config
from cluv.remote import Remote
from cluv.slurm import run_sacct

REPO_ROOT = Path(__file__).resolve().parents[1]

# Some useful constants used to turn tests on and off depending on where we are.
IN_GITHUB_CI = "GITHUB_ACTIONS" in os.environ
IN_SELF_HOSTED_GITHUB_CI = IN_GITHUB_CI and (
    os.environ.get("RUNNER_ENVIRONMENT", "") == "self-hosted"
)
IN_GITHUB_CLOUD_CI = IN_GITHUB_CI and (os.environ.get("RUNNER_ENVIRONMENT", "") == "github-hosted")
ON_DEV_MACHINE = not IN_GITHUB_CI

# We should either be on a dev machine (not in GitHub CI), on a self-hosted runner, or on a cloud runner.
assert ON_DEV_MACHINE ^ IN_GITHUB_CLOUD_CI ^ IN_SELF_HOSTED_GITHUB_CI


pytestmark = [
    pytest.mark.skipif(
        IN_GITHUB_CLOUD_CI,
        reason="Integration tests are only run on a self-hosted github runner or on a dev machine.",
    ),
    pytest.mark.integration,
    pytest.mark.timeout(20),
]

REQUIRED_CLUSTERS = ("mila", "tamia")
ALL_CLUSTERS = tuple(["mila"] + milatools.cli.init_command.DRAC_CLUSTERS)
STATUS_SUPPORTED_CLUSTERS = {"mila", "tamia", "rorqual"}
SUBMIT_SUPPORTED_CLUSTERS = {"mila", "rorqual"}
# Mark all the tests here as 'slow', so they are only run when the --slow flag is passed to pytest,
# specifically in the integration-tests CI step, which happens on a self-hosted runner that has
# reusable SSH connections to those clusters.


@pytest.fixture(autouse=True)
def mock_home_in_selfhosted_runner(monkeypatch: pytest.MonkeyPatch):
    """Mock the $HOME directory in a self-hosted runner, so that it is able to sync the project
    in its _work folder with the actual project path on the cluster.

    The folder structure goes like this:

    <some_path>/action-runners/some_name/_work/cluv/cluv
    """
    # NOTE: The second part of this condition is used to debug the self-hosted tests by opening
    # the _work folder and running tests there.
    if IN_SELF_HOSTED_GITHUB_CI or "_work" in Path.cwd().parts:
        work_folder = (
            Path.cwd().parent.parent
        )  # This should be the _work folder in the self-hosted runner
        monkeypatch.setattr(Path, "home", lambda: work_folder)


async def test_login(remote: Remote):
    assert (await login([remote.hostname])) == [remote]


@pytest_asyncio.fixture(scope="session")
async def cluster_status(cluster: str) -> ClusterStatus:
    return await get_cluster_status(cluster, {})


@pytest.mark.slow
@pytest.mark.timeout(30)
@pytest.mark.xfail(reason="Status integration tests are flaky and will be reworked soon.")
@pytest.mark.asyncio
async def test_status_online(cluster_status: ClusterStatus, cluster: str):
    if cluster not in STATUS_SUPPORTED_CLUSTERS:
        pytest.xfail(f"Status integration test not supported on cluster {cluster}.")
    assert cluster_status.online is True


@pytest.mark.slow
@pytest.mark.timeout(30)
@pytest.mark.xfail(reason="Status integration tests are flaky and will be reworked soon.")
@pytest.mark.asyncio
async def test_status_has_gpus(cluster_status: ClusterStatus, cluster: str):
    if cluster not in STATUS_SUPPORTED_CLUSTERS:
        pytest.xfail(f"Status integration test not supported on cluster {cluster}.")
    assert cluster_status.gpu_total > 0, "Expected cluster to report GPU nodes"


@pytest.mark.slow
@pytest.mark.timeout(30)
@pytest.mark.xfail(reason="Status integration tests are flaky and will be reworked soon.")
@pytest.mark.asyncio
async def test_status_gpu_model(cluster_status: ClusterStatus, cluster: str):
    if cluster not in STATUS_SUPPORTED_CLUSTERS:
        pytest.xfail(f"Status integration test not supported on cluster {cluster}.")
    assert cluster_status.gpu_model != "?", f"GPU model not detected: {cluster_status.gpu_model!r}"


@pytest.mark.slow
@pytest.mark.timeout(30)
@pytest.mark.xfail(reason="Status integration tests are flaky and will be reworked soon.")
@pytest.mark.asyncio
async def test_status_storage(cluster_status: ClusterStatus):
    assert cluster_status.storage.home_quota > 0, "Expected non-zero home quota"
    assert cluster_status.storage.scratch_quota > 0, "Expected non-zero scratch quota"
    assert cluster_status.storage.home_used >= 0
    assert cluster_status.storage.scratch_used >= 0


TEST_SUBMIT_TIMEOUT_SECONDS = 180


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
    ],
    indirect=True,
)
@pytest.mark.slow
@pytest.mark.timeout(TEST_SUBMIT_TIMEOUT_SECONDS)
async def test_submit(remote: Remote):
    """End-to-end: actually submit scripts/job.sh to a slurm cluster via sbatch.

    Requires an active SSH connection to the cluster and a clean git tree.
    Also actually performs a `cluv sync` to that cluster.

    NOTE: This may push the current branch to GitHub when run locally, but in
    GitHub Actions `cluv sync` skips `git push`.
    """
    if remote.hostname not in SUBMIT_SUPPORTED_CLUSTERS:
        pytest.xfail(f"Submit integration test not supported on cluster {remote.hostname}.")

    should_cancel_job = True
    job = await submit(
        cluster=remote.hostname,
        job_script=Path("scripts/job.sh"),
        sbatch_args=["--time=00:00:30"],
        program_args=["python", "--version"],
    )
    cluster = remote.hostname
    assert isinstance(job, Job)
    job_id = job.job_id

    try:
        job_name = await remote.get_output(
            f"sacct -j {job_id} --format=JobName --noheader --parsable2 | head -1"
        )
        assert job_name.strip().startswith("cluv-")
        wait_time = 5
        TERMINAL_STATES = {
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMEOUT",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
            "BOOT_FAIL",
            "DEADLINE",
        }
        async with asyncio.timeout(TEST_SUBMIT_TIMEOUT_SECONDS):
            while (job_state := await run_sacct(remote, job_id)) not in TERMINAL_STATES:
                print(
                    f"Job {job_id} is in state {job_state}, waiting for it to reach a terminal state..."
                )
                await asyncio.sleep(wait_time)
                wait_time = min(wait_time * 2, 60)  # Don't wait more than 30s between polls

        # Wait until the job exits, then verify output content after syncing logs back locally.
        if job_state == "COMPLETED":
            should_cancel_job = False  # No need to cancel a completed job
        else:
            pytest.fail(f"Submitted job {job_id} ended with unexpected status: {job_state!r}")

        await sync(clusters=[remote.hostname])

        # TODO: get the results dir based on the `job` object somehow, instead of assuming
        # {cluster}_{job_id} which is just the default for a 'regular' job (no chunking or packing).
        output_file = (
            Path(os.path.expandvars(get_cluv_config().results_path))
            / f"{cluster}_{job_id}"
            / f"slurm-{job_id}.out"
        )
        assert output_file.is_file(), (
            f"Expected job output file to be synced locally: {output_file}"
        )
        output_text = output_file.read_text(errors="replace")

        # TODO: Reuse this test for the pytorch example by checking for a different output.
        assert re.search(r"Python \d+\.\d+(\.\d+)?", output_text), (
            f"Expected python version output in {output_file}, got:\n{output_text}"
        )
    except asyncio.TimeoutError:
        pytest.fail(f"Job {job_id} did not reach a terminal state within the timeout period.")
    finally:
        if should_cancel_job:
            await remote.run(f"scancel {job_id}", warn=True, hide=True, display=True)


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)  # Set the home directory to tmp_path
    return tmp_path


@pytest.fixture(params=[True, False], ids=["with_scratch", "without_scratch"])
def scratch(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest, fake_scratch: Path
) -> Path | None:
    """Fixture that sets up a fake SCRATCH directory if requested, or pretends that SCRATCH doesn't exist otherwise."""
    use_scratch = request.param
    if use_scratch:
        return fake_scratch
    if "SCRATCH" in os.environ:
        # Remove the SCRATCH environment variable
        monkeypatch.delenv("SCRATCH")
    return None


@pytest.fixture
def project_name(request: pytest.FixtureRequest) -> str:
    return getattr(request, "param", "my_project")


@pytest.fixture(params=[True, False], ids=["existing_project", "new_project"])
def is_existing_project(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def project_dir(fake_home: Path, project_name: str, is_existing_project: bool) -> Path:
    """Fixture that creates a project directory and changes into it."""
    project_dir = fake_home / project_name
    project_dir.mkdir()
    if is_existing_project:
        subprocess.run(f"uv init {project_dir}", shell=True, check=True)
        job_script = project_dir / "scripts" / "job.sh"
        job_script.parent.mkdir(exist_ok=False, parents=True)
        job_script.touch()  # Touch the job script to simulate an existing project
        # Make the job script executable:
        job_script.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    return project_dir


@pytest.fixture(autouse=True)
def return_to_start_dir():
    start_dir = Path.cwd()
    try:
        yield
    finally:
        os.chdir(start_dir)


@pytest.mark.timeout(5)
def test_init(
    project_dir: Path,
    scratch: Path | None,
    is_existing_project: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(project_dir)

    pyproject_file = project_dir / "pyproject.toml"
    if pyproject_file.exists() and scratch:
        content = pyproject_file.read_text()
        content = content.replace(
            "SCRATCH = $HOME/scratch", f"SCRATCH = {scratch}" if scratch else ""
        )
        pyproject_file.write_text(content)
    monkeypatch.setenv("SCRATCH", str(scratch) if scratch else "")

    init()

    generated_config = load_cluv_config(project_dir / "pyproject.toml")

    assert generated_config.results_path == f"$SCRATCH/logs/{project_dir.name}"
    assert (project_dir / "scripts").is_dir()
    assert (project_dir / "scripts" / "job.sh").is_file()

    job_script = project_dir / "scripts" / "job.sh"
    if is_existing_project:
        assert job_script.read_text() == "", "cluv init overwrote the job script!"
    else:
        # TODO: The created job script should be executable!
        with pytest.raises(AssertionError):
            assert job_script.stat().st_mode & stat.S_IXUSR, "Job script is not executable!"

    if scratch:
        results_symlink = project_dir / generated_config.results_symlink
        results_path = Path(os.path.expandvars(generated_config.results_path))
        assert generated_config.results_path and results_path.exists()
        assert results_symlink.is_symlink()
        assert results_symlink.resolve() == results_path

    expected_config = load_cluv_config(REPO_ROOT / "pyproject.toml")
    assert generated_config.clusters_names == expected_config.clusters_names


@pytest.mark.timeout(5)
def test_init_twice_doesnt_raise(project_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(project_dir)
    init()
    init()


@pytest.mark.timeout(5)
def test_init_at_path(fake_home: Path) -> None:
    """Test that running `cluv init` at a specific path works correctly."""
    project_path = fake_home / "my_new_project"
    init(project_path)

    generated_config = load_cluv_config(project_path / "pyproject.toml")
    assert generated_config.results_path == f"$SCRATCH/logs/{project_path.name}"
    assert (project_path / "scripts").is_dir()
    assert (project_path / "scripts" / "job.sh").is_file()
    assert (project_path / "scripts" / "safe_job.sh").is_file()

    expected_config = load_cluv_config(REPO_ROOT / "pyproject.toml")
    assert generated_config.clusters_names == expected_config.clusters_names
