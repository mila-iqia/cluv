import subprocess
from pathlib import Path

import pytest

from cluv.remote import Remote


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
        pytest.param(
            "first",
            marks=pytest.mark.xfail(
                raises=NotImplementedError,
                strict=True,
                reason="hydra launcher doesn't support submit-first yet.",
            ),
        ),
    ],
    indirect=True,
)
async def test_hydra_example(remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
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
        capture_output=True,
        check=True,
    )
    output = subprocess_result.stdout or subprocess_result.stderr
    # Very simple: Check that this portion of the table, near the end, shows each run as completed.
    assert "lr=0.1 │ COMPLETED" in output
    assert "lr=0.2 │ COMPLETED" in output
