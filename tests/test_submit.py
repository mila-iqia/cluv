"""Unit tests for cluv/cli/submit.py — no SSH, no git, all mocked."""

import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cluv.cli import submit as submit_module
from cluv.config import CluvConfig, SubmitConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_COMMIT = "abc1234def5678"


def _make_config(
    clusters=None,
    job_script="scripts/job.sh",
    slurm=None,
    cluster_configs=None,
) -> CluvConfig:
    return CluvConfig(
        clusters=clusters or ["rorqual"],
        results_path=None,
        submit=SubmitConfig(job_script=job_script),
        slurm=slurm or {},
        cluster_configs=cluster_configs or {},
    )


def _git_clean_run(*_args, **_kw):
    """Fake subprocess.run result for a clean git status."""
    result = MagicMock()
    result.stdout = ""
    return result


def _git_dirty_run(*_args, **_kw):
    """Fake subprocess.run result for a dirty git status."""
    result = MagicMock()
    result.stdout = " M some_file.py\n"
    return result


# ---------------------------------------------------------------------------
# Dirty git tree → exit
# ---------------------------------------------------------------------------


async def test_dirty_git_aborts(tmp_path):
    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_dirty_run),
        patch.object(submit_module, "get_config", return_value=_make_config()),
        patch.object(submit_module, "find_pyproject", return_value=tmp_path / "pyproject.toml"),
        pytest.raises(SystemExit) as exc_info,
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# No job script configured → exit
# ---------------------------------------------------------------------------


async def test_no_job_script_aborts(tmp_path):
    fake_remote = AsyncMock()
    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", AsyncMock(return_value=[fake_remote])),
        patch.object(submit_module, "get_config", return_value=_make_config(job_script=None)),
        patch.object(submit_module, "find_pyproject", return_value=tmp_path / "pyproject.toml"),
        pytest.raises(SystemExit) as exc_info,
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )
    assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Happy path — correct remote command is built
# ---------------------------------------------------------------------------


async def test_submit_builds_correct_remote_command(tmp_path):
    fake_remote = AsyncMock()
    project_root = tmp_path / "myproject"
    project_root.mkdir()
    pyproject = project_root / "pyproject.toml"

    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", AsyncMock(return_value=[fake_remote])),
        patch.object(submit_module, "get_config", return_value=_make_config()),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )

    fake_remote.run_async.assert_called_once()
    cmd = fake_remote.run_async.call_args[0][0]
    assert f"GIT_COMMIT={FAKE_COMMIT}" in cmd
    assert "sbatch" in cmd
    assert "scripts/job.sh" in cmd
    assert "python train.py" in cmd


async def test_submit_includes_global_slurm_vars(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

    cfg = _make_config(slurm={"SBATCH_TIME": "3:00:00", "SBATCH_GPUS": "1"})
    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", AsyncMock(return_value=[fake_remote])),
        patch.object(submit_module, "get_config", return_value=cfg),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "SBATCH_TIME=3:00:00" in cmd
    assert "SBATCH_GPUS=1" in cmd


async def test_submit_per_cluster_vars_override_globals(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

    cfg = _make_config(
        slurm={"SBATCH_PARTITION": "default", "SBATCH_TIME": "1:00:00"},
        cluster_configs={"rorqual": {"SBATCH_PARTITION": "main", "SBATCH_ACCOUNT": "def-bengioy"}},
    )
    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", AsyncMock(return_value=[fake_remote])),
        patch.object(submit_module, "get_config", return_value=cfg),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )

    cmd = fake_remote.run_async.call_args[0][0]
    # Per-cluster partition overrides global
    assert "SBATCH_PARTITION=main" in cmd
    assert "SBATCH_PARTITION=default" not in cmd
    # Cluster-specific account is added
    assert "SBATCH_ACCOUNT=def-bengioy" in cmd
    # Global time is still present
    assert "SBATCH_TIME=1:00:00" in cmd


# ---------------------------------------------------------------------------
# --job-script CLI flag overrides config
# ---------------------------------------------------------------------------


async def test_cli_job_script_overrides_config(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", AsyncMock(return_value=[fake_remote])),
        patch.object(submit_module, "get_config", return_value=_make_config(job_script="scripts/job.sh")),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script="scripts/other.sh",
            no_sync=False,
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "scripts/other.sh" in cmd
    assert "scripts/job.sh" not in cmd


# ---------------------------------------------------------------------------
# --no-sync skips sync and calls RemoteV2.connect instead
# ---------------------------------------------------------------------------


async def test_no_sync_skips_sync(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

    mock_connect = AsyncMock(return_value=fake_remote)
    mock_sync = AsyncMock(return_value=[fake_remote])

    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", mock_sync),
        patch.object(submit_module.RemoteV2, "connect", mock_connect),
        patch.object(submit_module, "get_config", return_value=_make_config()),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=True,
        )

    mock_sync.assert_not_called()
    mock_connect.assert_called_once_with("rorqual")


async def test_sync_called_by_default(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

    mock_sync = AsyncMock(return_value=[fake_remote])

    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value=FAKE_COMMIT),
        patch.object(submit_module, "sync", mock_sync),
        patch.object(submit_module, "get_config", return_value=_make_config()),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )

    mock_sync.assert_called_once_with(clusters=["rorqual"])


# ---------------------------------------------------------------------------
# GIT_COMMIT is always injected
# ---------------------------------------------------------------------------


async def test_git_commit_always_injected(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

    with (
        patch.object(submit_module.subprocess, "run", side_effect=_git_clean_run),
        patch.object(submit_module.subprocess, "check_output", return_value="deadbeef"),
        patch.object(submit_module, "sync", AsyncMock(return_value=[fake_remote])),
        patch.object(submit_module, "get_config", return_value=_make_config()),
        patch.object(submit_module, "find_pyproject", return_value=pyproject),
        patch.object(Path, "home", return_value=tmp_path),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["python", "train.py"],
            job_script=None,
            no_sync=False,
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "GIT_COMMIT=deadbeef" in cmd
