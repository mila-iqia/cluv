import os
import stat
import subprocess
from pathlib import Path

import pytest
import pytest_asyncio

import cluv.cli.clean
import cluv.cli.submit
import cluv.config
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import find_pyproject, get_cluv_config, set_local_env_vars
from cluv.remote import control_socket_is_running
from tests.test_integration import ALL_CLUSTERS, IN_SELF_HOSTED_GITHUB_CI, REQUIRED_CLUSTERS


@pytest.fixture(autouse=True)
def reset_cluv_config():
    """Reset the cache of the `get_cluv_config` function before each test to avoid state leakage."""

    get_cluv_config.cache_clear()


@pytest.fixture
def fake_scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Fixture to set a fake SCRATCH environment variable if it's not already set."""
    fake_scratch = tmp_path / "scratch"
    fake_scratch.mkdir()
    monkeypatch.setenv("SCRATCH", str(fake_scratch))

    def _mock_set_local_env_vars(env_vars: dict[str, str]) -> None:
        """Mock function swap our the $SCRATCH value from the pyproject.toml
        for the fake_scratch value during tests.
        """
        new_env_vars = env_vars.copy()
        if "SCRATCH" in env_vars:
            new_env_vars["SCRATCH"] = str(fake_scratch)
        set_local_env_vars(new_env_vars)

    # Patch this, so that the SCRATCH environment variable is always set as we expect it to be.
    monkeypatch.setattr(cluv.config, set_local_env_vars.__name__, _mock_set_local_env_vars)
    return fake_scratch


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    return fake_home


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
def project_dir(
    fake_home: Path, project_name: str, is_existing_project: bool, monkeypatch: pytest.MonkeyPatch
) -> Path:
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
    monkeypatch.chdir(project_dir)  # Set current working dir, as the docstring above promises.
    return project_dir


@pytest.fixture
def cluv_project_dir(project_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(project_dir)  # Set current working dir

    cluv.cli.init()
    return project_dir


@pytest.fixture(autouse=True)
def return_to_start_dir():
    start_dir = Path.cwd()
    try:
        yield
    finally:
        os.chdir(start_dir)


@pytest.fixture(autouse=IN_SELF_HOSTED_GITHUB_CI)
def use_normal_project_dir_on_cluster_instead_of_action_runners_path(
    monkeypatch: pytest.MonkeyPatch, reset_cluv_config: None, request: pytest.FixtureRequest
):
    """The self-hosted runner is running from ~/action-runners/.../_work/cluv/cluv.

    Patch the output of `get_cluv_config` while in the tests, so that it always uses a project_dir that is
    "normal", like ~/repos/cluv and ~/repos/cluv/examples/<example_name> instead of replicating entire
    action-runners/.../_work/cluv on the cluster.

    As a consequence of this, the ~/repos/cluv path on the clusters might be changed by the test runners.
    This is kind-of to be expected though, and is not different than doing a `cluv sync` ourselves.
    """

    # Only do this mocking if the test that is going to be run is marked with @pytest.mark.integration.
    if request.node.get_closest_marker("integration") is None:
        return  # don't patch get_cluv_config to not interfere with unit tests.

    def mock_get_cluv_config() -> cluv.config.CluvConfig:
        config = get_cluv_config()
        if config.project_dir is None:
            project_dir = find_pyproject().parent
            if project_dir.name == "cluv":
                monkeypatch.setattr(config, "project_dir", "$HOME/repos/cluv")
            else:
                assert project_dir.parent.name == "examples"
                monkeypatch.setattr(
                    config, "project_dir", f"$HOME/repos/cluv/examples/{project_dir.name}"
                )
        return config

    monkeypatch.setattr(cluv.config, get_cluv_config.__name__, mock_get_cluv_config)
    monkeypatch.setattr(cluv.cli.submit, get_cluv_config.__name__, mock_get_cluv_config)
    monkeypatch.setattr(cluv.cli.clean, get_cluv_config.__name__, mock_get_cluv_config)


@pytest_asyncio.fixture(scope="session", params=ALL_CLUSTERS)
async def cluster(request: pytest.FixtureRequest) -> str:
    """Fixture that gives the hostname of the Slurm cluster to run tests with.

    - In self-hosted CI, only the `REQUIRED_CLUSTERS` are ever tested, and this fixture fails
      (rather than skips) if one of them isn't connected. Other clusters are skipped outright,
      without even checking connectivity, so that a stray SSH connection to a slow cluster
      (e.g. a DRAC cluster like Narval) can't make CI opportunistically (and slowly) test it.
    - On a dev machine, this fixture opportunistically runs tests against whichever clusters
      have an active SSH connection, and skips the rest.

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
    assert isinstance(cluster, str)

    if IN_SELF_HOSTED_GITHUB_CI:
        # Only ever test the required clusters in CI: don't opportunistically pick up
        # whatever else happens to have a live SSH connection on the runner.
        if cluster not in REQUIRED_CLUSTERS:
            pytest.skip(f"{cluster} is not a required cluster; skipping it in CI.")
        if not await control_socket_is_running(cluster):
            pytest.fail(f"No active SSH connection to {cluster}, which must be tested against!")
        return cluster

    # On a dev machine: opportunistically test against whatever we're connected to.
    if await control_socket_is_running(cluster):
        return cluster
    pytest.skip(f"Test requires an active SSH connection to {cluster} to run.")


@pytest_asyncio.fixture(scope="session")
async def remote(cluster: str):
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        pytest.xfail(f"Test needs an active SSH connection to the {cluster} cluster.")
    return remote
