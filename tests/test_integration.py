"""Integration tests that require live SSH connections to a real Slurm cluster.

TODO: Do we prefer having tests for only one remote cluster at a time, in different CI steps?
Or have tests for every cluster in the same pytest session?
--> Choosing to have tests for all clusters in the same test session for now. This is more
efficient, since at some point there might be like 10 different clusters, and 10 CI steps to run.
"""

import os
import stat
import subprocess
from pathlib import Path

import milatools.cli.init_command
import pytest
import pytest_asyncio

from cluv.cli.init import DEFAULT_RESULTS_PATH, DRAC_CLUSTERS, init
from cluv.cli.login import get_remote_without_2fa_prompt, login
from cluv.cli.status import ClusterStatus, get_real_cluster_status
from cluv.cli.submit import submit
from cluv.config import load_cluv_config
from cluv.remote import Remote, control_socket_is_running

# Some useful constants used to turn tests on and off depending on where we are.
IN_GITHUB_CI = "GITHUB_ACTIONS" in os.environ
IN_SELF_HOSTED_GITHUB_CI = IN_GITHUB_CI and ("self-hosted" in os.environ.get("RUNNER_LABELS", ""))
IN_GITHUB_CLOUD_CI = IN_GITHUB_CI and not IN_SELF_HOSTED_GITHUB_CI
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


REQUIRED_CLUSTERS = ("mila", "rorqual", "tamia")
ALL_CLUSTERS = tuple(["mila"] + milatools.cli.init_command.DRAC_CLUSTERS)
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
    if IN_SELF_HOSTED_GITHUB_CI or "_work" in Path.cwd().parts:
        work_folder = (
            Path.cwd().parent.parent
        )  # This should be the _work folder in the self-hosted runner
        assert False, Path.cwd()
        monkeypatch.setattr(Path, "home", work_folder)


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
async def cluster_status(remote: Remote):
    return await get_real_cluster_status(remote)


@pytest.mark.slow
@pytest.mark.timeout(30)
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


@pytest.mark.xfail(
    IN_SELF_HOSTED_GITHUB_CI,
    reason="TODO: Running `cluv sync` does a git push / git pull from the runner's work folder, this causes issues.",
    strict=True,
)
@pytest.mark.slow
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


@pytest.mark.timeout(5)
def test_init(
    project_dir: Path,
    project_name: str,
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
        assert (project_dir / generated_config.results_path).exists()
        assert (project_dir / generated_config.results_path).is_symlink()
        assert (
            project_dir / generated_config.results_path
        ).resolve() == scratch / DEFAULT_RESULTS_PATH / project_name

    assert generated_config.clusters == ["mila"] + DRAC_CLUSTERS


@pytest.mark.timeout(5)
def test_init_twice_doesnt_raise(project_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(project_dir)
    init()
    init()
