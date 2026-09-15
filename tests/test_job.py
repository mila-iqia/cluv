"""Tests for `cluv.job` — `current_run_info()`/`RunInfo`/`get_run_id()`.

The non-sweep tests below lock in *today's* behavior (plain job / packing / chunking
`run_id` shapes) as regression coverage. The sweep-branch tests describe the behavior
`current_run_info()` has once it grows the sweep branch from `design/cluv_sweep.md` §2.
"""

import sys
import textwrap
from pathlib import Path

import pytest

import cluv.config
import cluv.job
import cluv.sweep
from cluv.config import ClusterConfig
from cluv.job import current_run_info

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _set_in_slurm_job(monkeypatch: pytest.MonkeyPatch, *, job_id: int, procid: int) -> None:
    """Sets both the live env vars (read by `current_run_id()`/`get_run_id()`) and the
    module-level constants `cluv.job` computes once at import time (read by
    `current_run_info()`'s own guard checks), consistently.
    """
    monkeypatch.setenv("SLURM_JOB_ID", str(job_id))
    monkeypatch.setenv("SLURM_PROCID", str(procid))
    monkeypatch.setattr(cluv.job, "SLURM_JOB_ID", job_id)
    monkeypatch.setattr(cluv.job, "SLURM_PROCID", procid)
    monkeypatch.setattr(cluv.job, "in_job_array", False)
    monkeypatch.delenv("SLURM_NTASKS_PER_GPU", raising=False)
    monkeypatch.delenv("SLURM_ARRAY_JOB_ID", raising=False)


@pytest.fixture
def fake_cluster_config(monkeypatch, tmp_path):
    cluster_config = ClusterConfig(
        env={},
        sbatch_args=[{}],
        results_path=tmp_path / "results",
        datasets_path=tmp_path / "datasets",
        job_script_path=None,
        project_dir=None,
    )
    monkeypatch.setattr(cluv.config, "current_cluster_config", lambda: cluster_config)
    monkeypatch.setattr(cluv.job, "current_cluster", lambda: "mila")
    return cluster_config


# ---------------------------------------------------------------------------
# regression coverage for today's (non-sweep) behavior
# ---------------------------------------------------------------------------


def test_current_run_info_returns_none_when_not_in_slurm_job(monkeypatch):
    monkeypatch.setattr(cluv.job, "SLURM_JOB_ID", None)
    assert current_run_info() is None


def test_current_run_info_returns_none_and_warns_when_procid_missing(monkeypatch):
    monkeypatch.setattr(cluv.job, "SLURM_JOB_ID", 123)
    monkeypatch.setattr(cluv.job, "SLURM_PROCID", None)
    with pytest.warns(RuntimeWarning):
        assert current_run_info() is None


def test_current_run_info_plain_job_run_id(fake_cluster_config, monkeypatch):
    _set_in_slurm_job(monkeypatch, job_id=555, procid=0)

    info = current_run_info()

    assert info is not None
    assert info.cluster == "mila"
    assert info.run_id == "mila_555"
    assert info.results_path == fake_cluster_config.results_path / "mila_555"


def test_current_run_info_packing_run_id_includes_task_index(fake_cluster_config, monkeypatch):
    _set_in_slurm_job(monkeypatch, job_id=555, procid=3)
    monkeypatch.setenv("SLURM_NTASKS_PER_GPU", "2")

    info = current_run_info()

    assert info is not None
    assert info.run_id == "mila_555_task3"


def test_current_run_info_chunking_run_id_uses_array_job_id(fake_cluster_config, monkeypatch):
    _set_in_slurm_job(monkeypatch, job_id=555, procid=0)
    monkeypatch.setattr(cluv.job, "in_job_array", True)
    monkeypatch.setattr(cluv.job, "_get_max_active_jobs", lambda: 1)
    monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "999")

    info = current_run_info()

    assert info is not None
    assert info.run_id == "mila_999"


# ---------------------------------------------------------------------------
# sweep branch (design/cluv-sweep.md §2) — not yet implemented
# ---------------------------------------------------------------------------


def test_current_run_info_sweep_branch_uses_sweep_name_and_slug(fake_cluster_config, monkeypatch):
    _set_in_slurm_job(monkeypatch, job_id=42, procid=1)
    monkeypatch.setenv(cluv.sweep.CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.delenv(cluv.sweep.CLUV_SWEEP_TASK_OFFSET_ENV_VAR, raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", "--foo=1,2,3"])
    cluv.sweep.patch_argv()
    _, expected_slug, expected_combo = cluv.sweep._current_sweep_context()

    info = current_run_info()

    assert info is not None
    assert info.run_id == f"mila_sweep-my-sweep_{expected_slug}"
    assert (
        info.results_path
        == fake_cluster_config.results_path / "sweeps" / "my-sweep" / expected_slug
    )
    assert info.command == expected_combo


def test_current_run_info_sweep_same_combo_same_run_id_under_different_offset_procid(
    fake_cluster_config, monkeypatch
):
    # Simulates a resubmission: the same global combo (index 6) lands on a different
    # job id/task index the second time around, but must resolve to the same run_id/
    # results_path — resumability is keyed on the combo's argument values, not on
    # which job/task it happened to land on.
    swept_arg = "--x=" + ",".join(str(i) for i in range(10))
    monkeypatch.setenv(cluv.sweep.CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")

    _set_in_slurm_job(monkeypatch, job_id=1, procid=6)
    monkeypatch.delenv(cluv.sweep.CLUV_SWEEP_TASK_OFFSET_ENV_VAR, raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", swept_arg])
    cluv.sweep.patch_argv()
    info_first = current_run_info()

    _set_in_slurm_job(monkeypatch, job_id=2, procid=2)
    monkeypatch.setenv(cluv.sweep.CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "4")
    monkeypatch.setattr(sys, "argv", ["main.py", swept_arg])
    cluv.sweep.patch_argv()
    info_second = current_run_info()

    assert info_first is not None
    assert info_second is not None
    assert info_first.run_id == info_second.run_id
    assert info_first.results_path == info_second.results_path


# ---------------------------------------------------------------------------
# regression coverage for SLURM_PROCID=0 being falsy (#197)
# ---------------------------------------------------------------------------


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
