from pathlib import Path

import pytest
import pytest_asyncio

import cluv.config
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import find_pyproject
from cluv.remote import control_socket_is_running
from tests.test_integration import ALL_CLUSTERS, IN_SELF_HOSTED_GITHUB_CI, REQUIRED_CLUSTERS


@pytest.fixture
def fake_scratch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Fixture to set a fake SCRATCH environment variable if it's not already set."""
    fake_scratch = tmp_path / "scratch"
    fake_scratch.mkdir()
    monkeypatch.setenv("SCRATCH", str(fake_scratch))
    from cluv.config import set_local_env_vars

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


@pytest.fixture(autouse=True)
def reset_cluv_config():
    """Reset the cluv config before each test to avoid state leakage."""
    from cluv.config import get_cluv_config

    get_cluv_config.cache_clear()


@pytest.fixture(autouse=IN_SELF_HOSTED_GITHUB_CI)
def use_normal_project_dir_on_cluster_instead_of_action_runners_path(
    monkeypatch: pytest.MonkeyPatch, reset_cluv_config: None
):
    """The self-hosted runner is running from ~/action-runners/.../_work/cluv/cluv.

    Patch the output of `get_cluv_config` while in the tests, so that it always uses a project_dir that is
    "normal", like ~/repos/cluv and ~/repos/cluv/examples/<example_name> instead of replicating entire
    action-runners/.../_work/cluv on the cluster.

    As a consequence of this, the ~/repos/cluv path on the clusters might be changed by the test runners.
    This is kind-of to be expected though, and is not different than doing a `cluv sync` ourselves.
    """
    from cluv.config import get_cluv_config

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
