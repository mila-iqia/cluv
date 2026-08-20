import os
import re
import subprocess
from pathlib import Path

import pytest

from cluv.cli.sync import get_active_remotes
from cluv.remote import control_socket_is_running

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
