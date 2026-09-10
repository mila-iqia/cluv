"""Tests for `cluv.job`, the part of cluv that jobs import at runtime."""

import textwrap
from pathlib import Path

import pytest

import cluv.config
import cluv.job
from cluv.job import current_run_info


@pytest.fixture
def in_a_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    """Make it look like we're a task of a Slurm job on the `mila` cluster."""
    (tmp_path / "pyproject.toml").write_text(
        textwrap.dedent(
            """\
            [tool.cluv]
            results_path = "$SCRATCH/logs/example"
            datasets_path = "$SCRATCH/datasets/example"

            [tool.cluv.clusters.mila]
            """
        )
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.setenv("SLURM_JOB_ID", "1234")
    monkeypatch.setenv("SLURM_PROCID", str(request.param))
    # These are read into module-level constants when `cluv.job` is imported.
    monkeypatch.setattr(cluv.job, "SLURM_JOB_ID", 1234)
    monkeypatch.setattr(cluv.job, "SLURM_PROCID", request.param)
    monkeypatch.setattr(cluv.job, "current_cluster", lambda: "mila")
    monkeypatch.setattr(cluv.config, "current_cluster", lambda: "mila")


@pytest.mark.parametrize("in_a_job", [0, 1, 2], indirect=True)
def test_current_run_info_in_every_task(in_a_job: None, request: pytest.FixtureRequest) -> None:
    """All the tasks of a job need to agree on the run id and results path.

    Regression test: rank 0 used to get `None` here, because `SLURM_PROCID=0` is falsy.
    """
    run_info = current_run_info()
    assert run_info is not None
    assert run_info.cluster == "mila"
    assert run_info.run_id == "mila_1234"
    assert run_info.results_path.name == "mila_1234"


def test_current_run_info_outside_of_a_job(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cluv.job, "SLURM_JOB_ID", None)
    assert current_run_info() is None
