import subprocess
from pathlib import Path

import pytest
from pytest_regressions.file_regression import FileRegressionFixture

from cluv.remote import Remote


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
async def test_hydra_example(
    cluster: str,
    remote: Remote,  # noqa
    monkeypatch: pytest.MonkeyPatch,
    file_regression: FileRegressionFixture,
) -> None:
    """End-to-end: actually run the hydra example.

    Requires an active SSH connection to the cluster and a clean git tree.
    Also actually performs a `cluv sync` to that cluster.
    """
    repo_root = Path(__file__).parent.parent
    monkeypatch.chdir(repo_root / "examples/hydra_example")

    subprocess_result = subprocess.run(
        f"uv run python main.py --multirun launcher=cluv hydra.launcher.cluster={remote.hostname} lr=0.1,0.2",
        shell=True,
        text=True,
        # capture_output=True,
        check=True,
    )
    file_regression.check(
        subprocess_result.stdout or subprocess_result.stderr, extension=".stdout", encoding="utf-8"
    )
