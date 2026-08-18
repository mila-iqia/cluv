"""Tests for `cluv.job` — `current_run_info()`/`RunInfo`/`get_run_id()`.

No dedicated test file existed for this module before this design. The non-sweep tests
below lock in *today's* behavior (plain job / packing / chunking `run_id` shapes) as
regression coverage. The sweep-branch tests describe the behavior `current_run_info()`
should have once it grows the new branch from `design/cluv-sweep.md` §2 — they are
written TDD-first and are expected to fail until that branch is added (and, in the
meantime, until `cluv.sweep`'s own stub functions are implemented — see
`tests/test_sweep.py`), since `current_run_info()` doesn't yet call `cluv.sweep` at all.
"""

import sys

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
        sbatch_args={},
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
