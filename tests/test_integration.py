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

from cluv.cli.init import DEFAULT_RESULTS_PATH, init
from cluv.cli.login import get_remote_without_2fa_prompt, login
from cluv.cli.status import ClusterStatus, get_cluster_status
from cluv.cli.submit import submit
from cluv.cli.sync import sync
from cluv.config import get_cluv_config, load_cluv_config
from cluv.remote import Remote, control_socket_is_running

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


@pytest_asyncio.fixture(scope="session", params=ALL_CLUSTERS)
async def cluster(request: pytest.FixtureRequest) -> str:
    """Fixture that gives the hostname of the Slurm cluster to run tests with.

    - If the SLURM_CLUSTER environment variable is not set, all tests that depend on this fixture
      will be skipped.
    - If it is set and there is not an active SSH connection to that cluster, this fixture will
      fail, causing all tests that use it to fail, since they require a live connection to a
      cluster.

    NOTE: This fixture can also be (indirectly) parametrized by tests that want to run with a remote
    connected to only some clusters in particular. For example:

    ```python
    @pytest.mark.parametrize("cluster", ["mila", "tamia", "rorqual"], indirect=True)
    def test_something(remote: Remote):
        assert remote.hostname in ["mila", "tamia", "rorqual"]
    ```
    """
    cluster = getattr(request, "param", None)
    if cluster is None:
        pytest.skip(
            "No cluster specified. Set the SLURM_CLUSTER environment variable to a "
            "cluster with an active SSH connection to run these tests."
        )
    existing_ssh_connection = await control_socket_is_running(cluster)
    if existing_ssh_connection:
        assert isinstance(cluster, str)
        return cluster
    if cluster not in REQUIRED_CLUSTERS:
        pytest.skip(
            f"No active SSH connection to {cluster}, but it is not necessary to test against it."
        )
    if IN_SELF_HOSTED_GITHUB_CI:
        pytest.fail(f"No active SSH connection to {cluster}, which must be tested against!")
    # On a dev machine. Just skip and display some instructions.
    pytest.skip(f"Test requires an active SSH connection to {cluster} to run.")


@pytest_asyncio.fixture(scope="session")
async def remote(cluster: str):
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        pytest.xfail(f"Test needs an active SSH connection to the {cluster} cluster.")
    return remote


async def test_login(remote: Remote):
    assert (await login([remote.hostname])) == [remote]


@pytest_asyncio.fixture(scope="session")
async def cluster_status(cluster: str) -> ClusterStatus:
    return await get_cluster_status(cluster)


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
async def test_status_jobs(cluster_status: ClusterStatus, cluster: str):
    if cluster not in STATUS_SUPPORTED_CLUSTERS:
        pytest.xfail(f"Status integration test not supported on cluster {cluster}.")
    # Job counts must be non-negative integers (tamia is a busy cluster)
    assert cluster_status.jobs.running >= 0
    assert cluster_status.jobs.pending >= 0
    assert cluster_status.jobs.my_running >= 0
    assert cluster_status.jobs.my_pending >= 0


@pytest.mark.slow
@pytest.mark.timeout(30)
@pytest.mark.xfail(reason="Status integration tests are flaky and will be reworked soon.")
@pytest.mark.asyncio
async def test_status_storage(cluster_status: ClusterStatus):
    assert cluster_status.storage.home_quota > 0, "Expected non-zero home quota"
    assert cluster_status.storage.scratch_quota > 0, "Expected non-zero scratch quota"
    assert cluster_status.storage.home_used >= 0
    assert cluster_status.storage.scratch_used >= 0


@pytest.mark.slow
@pytest.mark.timeout(180)
async def test_submit(remote: Remote, fake_scratch: Path):
    """End-to-end: actually submit scripts/job.sh to a slurm cluster via sbatch.

    Requires an active SSH connection to the cluster and a clean git tree.
    Also actually performs a `cluv sync` to that cluster.

    NOTE: This may push the current branch to GitHub when run locally, but in
    GitHub Actions `cluv sync` skips `git push`.
    """
    if remote.hostname not in SUBMIT_SUPPORTED_CLUSTERS:
        pytest.xfail(f"Submit integration test not supported on cluster {remote.hostname}.")

    should_cancel_job = True
    job_id = await submit(
        cluster=remote.hostname,
        job_script=Path("scripts/job.sh"),
        sbatch_args=["--time=00:00:30"],
        program_args=["python", "--version"],
    )
    cluster = remote.hostname
    assert isinstance(job_id, int)
    try:
        job_name = await remote.get_output(
            f"sacct -j {job_id} --format=JobName --noheader --parsable2 | head -1"
        )
        assert job_name.strip().startswith("cluv-")
        # Wait until the job exits, then verify output content after syncing logs back locally.
        TERMINAL_STATUSES = {
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
        final_status = "UNKNOWN"
        max_poll_attempts = 5
        poll_interval_seconds = 5
        # Poll for terminal state, leaving a bit of room in the
        # 180s test timeout for sync + output validation.
        for _ in range(max_poll_attempts):
            status_output = await remote.get_output(
                f"sacct -j {job_id} --format=State --noheader --parsable2 --allocations | head -1",
                warn=True,
                hide=True,
                display=False,
            )
            # --parsable2 uses pipe-delimited output (`STATE|...`), so keep only the state field.
            final_status = status_output.strip().partition("|")[0].strip()
            if final_status in TERMINAL_STATUSES:
                break
            await asyncio.sleep(poll_interval_seconds)
            poll_interval_seconds *= 2
        if final_status not in TERMINAL_STATUSES:
            pytest.fail(
                f"Job {job_id} did not reach terminal status within "
                f"{max_poll_attempts * poll_interval_seconds}s "
                f"(last status: {final_status!r})"
            )
        if final_status != "COMPLETED":
            pytest.fail(f"Submitted job {job_id} ended with unexpected status: {final_status!r}")
        should_cancel_job = False

        await sync(clusters=[remote.hostname])

        output_file = (
            Path(os.path.expandvars(get_cluv_config().results_path))
            / f"{cluster}_{job_id}"
            / f"slurm-{job_id}.out"
        )
        assert output_file.is_file(), (
            f"Expected job output file to be synced locally: {output_file}"
        )
        output_text = output_file.read_text(errors="replace")
        assert re.search(r"Python \d+\.\d+(\.\d+)?", output_text), (
            f"Expected python version output in {output_file}, got:\n{output_text}"
        )
    finally:
        if should_cancel_job:
            await remote.run(f"scancel {job_id}", warn=True, hide=True, display=True)


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)  # Set the home directory to tmp_path
    return tmp_path


@pytest.fixture(params=[True, False], ids=["with_scratch", "without_scratch"])
def scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Path | None:
    """Fixture that sets up a fake SCRATCH directory if requested, or pretends that SCRATCH doesn't exist otherwise."""
    use_scratch = request.param
    if use_scratch:
        fake_scratch_dir = tmp_path / "fake_scratch"
        monkeypatch.setenv("SCRATCH", str(fake_scratch_dir))  # Set the SCRATCH env var to tmp_path
        return fake_scratch_dir
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

    init()

    generated_config = load_cluv_config(project_dir / "pyproject.toml")
    assert generated_config.results_path == DEFAULT_RESULTS_PATH
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
    assert generated_config.results_path == DEFAULT_RESULTS_PATH
    assert (project_path / "scripts").is_dir()
    assert (project_path / "scripts" / "job.sh").is_file()
    assert (project_path / "scripts" / "safe_job.sh").is_file()

    expected_config = load_cluv_config(REPO_ROOT / "pyproject.toml")
    assert generated_config.clusters_names == expected_config.clusters_names
