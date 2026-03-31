"""Unit tests for cluv/cli/submit.py — no SSH, no git, all mocked."""

import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cluv.cli import submit as submit_module
from cluv.config import CluvConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_COMMIT = "abc1234def5678"


def _make_config(
    clusters=None,
    slurm=None,
    cluster_configs=None,
) -> CluvConfig:
    return CluvConfig(
        clusters=clusters or ["rorqual"],
        results_path=None,
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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=[],
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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=["--", "python", "train.py"],
        )

    fake_remote.run_async.assert_called_once()
    cmd = fake_remote.run_async.call_args[0][0]
    assert f"GIT_COMMIT={FAKE_COMMIT}" in cmd
    assert "sbatch" in cmd
    assert "scripts/job.sh" in cmd
    assert "python train.py" in cmd


# ---------------------------------------------------------------------------
# sbatch flags before '--' are forwarded to sbatch
# ---------------------------------------------------------------------------


async def test_sbatch_flags_forwarded(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=["--partition=gpu", "--mem=40G", "--", "python", "train.py"],
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "--partition=gpu" in cmd
    assert "--mem=40G" in cmd
    assert "python train.py" in cmd
    # sbatch flags must appear before the job script
    assert cmd.index("--partition=gpu") < cmd.index("scripts/job.sh")


# ---------------------------------------------------------------------------
# rest with no '--' → all treated as sbatch flags, no program args
# ---------------------------------------------------------------------------


async def test_rest_without_separator_treated_as_sbatch_flags(tmp_path):
    fake_remote = AsyncMock()
    pyproject = tmp_path / "proj" / "pyproject.toml"
    (tmp_path / "proj").mkdir()

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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=["--gres=gpu:1"],
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "--gres=gpu:1" in cmd
    assert cmd.index("--gres=gpu:1") < cmd.index("scripts/job.sh")


# ---------------------------------------------------------------------------
# Global SBATCH_* env vars from config
# ---------------------------------------------------------------------------


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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=["--", "python", "train.py"],
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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=["--", "python", "train.py"],
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "SBATCH_PARTITION=main" in cmd
    assert "SBATCH_PARTITION=default" not in cmd
    assert "SBATCH_ACCOUNT=def-bengioy" in cmd
    assert "SBATCH_TIME=1:00:00" in cmd


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
            job_script="scripts/job.sh",
            no_sync=True,
            rest=[],
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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=[],
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
            job_script="scripts/job.sh",
            no_sync=False,
            rest=[],
        )

    cmd = fake_remote.run_async.call_args[0][0]
    assert "GIT_COMMIT=deadbeef" in cmd


# ---------------------------------------------------------------------------
# install_scripts
# ---------------------------------------------------------------------------


async def test_install_scripts_runs_on_all_remotes():
    from pathlib import PurePosixPath
    from unittest.mock import AsyncMock, call

    from cluv.cli.sync import install_scripts

    remotes = [AsyncMock(), AsyncMock()]
    project_path = PurePosixPath("repos/myproject")

    await install_scripts(remotes, project_path)

    for remote in remotes:
        remote.run_async.assert_called_once()
        cmd = remote.run_async.call_args[0][0]
        assert "~/.local/bin" in cmd
        assert f"~/{project_path}/scripts" in cmd
        assert "ln -sf" in cmd
        assert 'basename "$f" .sh' in cmd


async def test_install_scripts_command_is_idempotent():
    """ln -sf must be used (force-overwrite) so re-runs don't fail."""
    from pathlib import PurePosixPath
    from unittest.mock import AsyncMock

    from cluv.cli.sync import install_scripts

    remote = AsyncMock()
    await install_scripts([remote], PurePosixPath("repos/proj"))

    cmd = remote.run_async.call_args[0][0]
    assert "ln -sf" in cmd  # -f = force overwrite existing symlink
