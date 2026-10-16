import logging
from pathlib import Path

import pytest
import pytest_asyncio

import cluv.config
import cluv.remote
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import get_cluv_config, set_local_env_vars
from cluv.remote import Remote, control_socket_is_running
from tests.test_integration import (
    ALL_CLUSTERS,
    IN_SELF_HOSTED_GITHUB_CI,
    ON_DEV_MACHINE,
    REQUIRED_CLUSTERS,
)


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


@pytest.fixture(autouse=True)
def reset_cluv_config():
    """Reset the cluv config before each test to avoid state leakage."""

    get_cluv_config.cache_clear()


@pytest.fixture(
    # NOTE: The second part of this condition is used to debug the self-hosted tests by opening
    # the _work folder and running tests there.
    autouse=IN_SELF_HOSTED_GITHUB_CI or (ON_DEV_MACHINE and "_work" in Path.cwd().parts)
)
def use_normal_repo_dir_on_clusters_in_selfhosted_runner(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
):
    """The self-hosted runner is running from ~/action-runners/.../_work/cluv/cluv.

    Set `CLUV_REPO_DIR` so that the projects get a "normal" project_dir on the clusters, like
    ~/repos/cluv and ~/repos/cluv/examples/<example_name>, instead of a path derived from the
    runner's checkout. Being an env var, this also applies to `cluv` running in subprocesses.

    As a consequence of this, the ~/repos/cluv path on the clusters might be changed by the test runners.
    This is kind-of to be expected though, and is not different than doing a `cluv sync` ourselves.
    """
    # Only for integration tests, to not interfere with the config values in unit tests.
    if request.node.get_closest_marker("integration") is not None:
        monkeypatch.setenv("CLUV_REPO_DIR", "$HOME/repos/cluv")


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


logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        # Add the `pytest.mark.xdist_group(cluster)` if the test is marked with pytest.mark.integration or pytest.mark.slow.
        if not hasattr(item, "callspec"):
            continue
        assert isinstance(item, pytest.Function), type(item)
        xdist_group = item.get_closest_marker("xdist_group")
        if xdist_group is not None:
            continue
        # Only add xdist_group for integration or slow tests
        if not (item.get_closest_marker("integration") or item.get_closest_marker("slow")):
            continue
        # If the test uses a `cluster` argument in its signature, group by that value.
        if isinstance(cluster_param := item.callspec.params.get("cluster"), str):
            logger.debug(f"Adding xdist_group({cluster_param}) to {item.nodeid}")
            item.add_marker(pytest.mark.xdist_group(cluster_param))
        # If the test doesn't use `cluster`, but uses `remote`, then group by the remote's hostname.
        elif isinstance(remote_param := item.callspec.params.get("remote"), Remote):
            logger.debug(f"Adding xdist_group({remote_param.hostname}) to {item.nodeid}")
            item.add_marker(pytest.mark.xdist_group(remote_param.hostname))
