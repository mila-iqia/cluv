"""Integration tests that require live SSH connections to real clusters.

Run with:
    uv run pytest -m integration -v

Skip with:
    uv run pytest -m "not integration"

These tests connect to real clusters. They will fail if you do not have
active SSH ControlMaster sockets (run `cluv login` first).
"""

import pytest

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.status import get_all_cluster_statuses, get_real_cluster_status

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _require_remote(cluster: str):
    """Return an active Remote for *cluster*, skip the test if not connected."""
    remote = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        pytest.skip(f"No active SSH connection to {cluster!r}. Run `cluv login {cluster}` first.")
    return remote


# ---------------------------------------------------------------------------
# cluv status – per-cluster
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_tamia_online():
    remote = await _require_remote("tamia")
    status = await get_real_cluster_status(remote)
    assert status.online is True


@pytest.mark.asyncio
async def test_status_tamia_has_gpus():
    remote = await _require_remote("tamia")
    status = await get_real_cluster_status(remote)
    assert status.gpu_total > 0, "Expected tamia to report GPU nodes"


@pytest.mark.asyncio
async def test_status_tamia_gpu_model():
    remote = await _require_remote("tamia")
    status = await get_real_cluster_status(remote)
    assert status.gpu_model != "?", f"GPU model not detected: {status.gpu_model!r}"


@pytest.mark.asyncio
async def test_status_tamia_jobs():
    remote = await _require_remote("tamia")
    status = await get_real_cluster_status(remote)
    # Job counts must be non-negative integers (tamia is a busy cluster)
    assert status.jobs.running >= 0
    assert status.jobs.pending >= 0
    assert status.jobs.my_running >= 0
    assert status.jobs.my_pending >= 0


@pytest.mark.asyncio
async def test_status_tamia_storage():
    remote = await _require_remote("tamia")
    status = await get_real_cluster_status(remote)
    assert status.storage.home_quota > 0, "Expected non-zero home quota"
    assert status.storage.scratch_quota > 0, "Expected non-zero scratch quota"
    assert status.storage.home_used >= 0
    assert status.storage.scratch_used >= 0


# ---------------------------------------------------------------------------
# cluv status – all connected clusters
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_no_args_returns_live_data():
    statuses, is_live = await get_all_cluster_statuses()
    if not is_live:
        pytest.skip("No active SSH connections found. Run `cluv login` first.")
    assert len(statuses) > 0
    assert all(isinstance(s.name, str) for s in statuses)


@pytest.mark.timeout(60)
def test_status_no_args_via_cli():
    """Regression: cluv status with no clusters argument must not raise.

    Previously default=() was validated against choices and raised
    'invalid choice: ()'.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "cluv", "status"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"cluv status exited {result.returncode}:\n{result.stderr}"


@pytest.mark.asyncio
async def test_status_explicit_cluster_list():
    """Passing remotes explicitly should return exactly those clusters."""
    remote = await _require_remote("tamia")
    statuses, is_live = await get_all_cluster_statuses(remotes=[remote])
    assert is_live
    assert len(statuses) == 1
    assert statuses[0].name == "tamia"


# ---------------------------------------------------------------------------
# cluv status – Mila-specific (savail)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_mila_online():
    remote = await _require_remote("mila")
    status = await get_real_cluster_status(remote)
    assert status.online is True


@pytest.mark.asyncio
async def test_status_mila_gpus_from_savail():
    """GPU data on Mila must come from savail, not sinfo."""
    remote = await _require_remote("mila")
    status = await get_real_cluster_status(remote)
    assert status.gpu_total > 0, "Expected savail to report GPU totals on Mila"
    assert status.gpu_model != "?", f"GPU model not detected on Mila: {status.gpu_model!r}"


@pytest.mark.asyncio
async def test_status_mila_storage():
    remote = await _require_remote("mila")
    status = await get_real_cluster_status(remote)
    assert status.storage.home_quota > 0
    assert status.storage.scratch_quota > 0


# ---------------------------------------------------------------------------
# cluv sync
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_tamia_connects():
    """Smoke-test that sync reaches tamia without errors (no actual git push)."""
    from unittest.mock import AsyncMock, patch

    from cluv.cli import sync as sync_module

    remote = await _require_remote("tamia")

    # Patch LocalV2.run so we don't actually git push, and patch
    # login so we get our pre-connected remote back without any 2FA.
    with (
        patch.object(sync_module, "login", AsyncMock(return_value=[remote])),
        patch.object(sync_module, "install_uv", AsyncMock()),
        patch.object(sync_module, "clone_project", AsyncMock()),
    ):
        remotes = await sync_module.sync(clusters=["tamia"])

    assert len(remotes) == 1
    assert remotes[0].hostname == "tamia"


# ---------------------------------------------------------------------------
# cluv submit — rorqual integration
# ---------------------------------------------------------------------------


async def test_submit_rorqual_real():
    """End-to-end: actually submit scripts/job.sh to rorqual via sbatch.

    Requires an active SSH connection to rorqual and a clean git tree.
    The project must already be synced on rorqual (sync is mocked out).
    """
    import subprocess as _subprocess
    from unittest.mock import AsyncMock, patch

    from cluv.cli import submit as submit_module

    remote = await _require_remote("rorqual")

    git_result = _subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if any(not line.startswith("??") for line in git_result.stdout.splitlines()):
        pytest.skip("Working tree is dirty — cluv submit requires a clean git state")

    # Wrap run_async to capture the sbatch response without breaking the call.
    completed: list[_subprocess.CompletedProcess] = []
    _original = remote.run

    async def _capture(*args, **kwargs):
        cp = await _original(*args, **kwargs)
        completed.append(cp)
        return cp

    remote.run = _capture

    with patch.object(submit_module, "sync", AsyncMock(return_value=[remote])):
        await submit_module.submit(
            cluster="rorqual",
            job_script="scripts/job.sh",
            sbatch_args=[],
            program_args=[],
        )

    assert completed, "run_async was never called"
    assert "Submitted batch job" in completed[-1].stdout


async def test_submit_rorqual_builds_correct_command():
    """Integration smoke-test: verify submit builds the right sbatch command for rorqual.

    Connects to rorqual for real (exercises the live SSH path), but replaces
    run_async on the returned remote so sbatch is never actually invoked —
    the project may not be synced on rorqual at test time.
    """
    from unittest.mock import AsyncMock, patch

    from cluv.cli import submit as submit_module
    from cluv.config import CluvConfig

    remote = await _require_remote("rorqual")
    # Direct assignment avoids descriptor/slot issues with patch.object on instances.
    # sync is mocked to return this remote, so submit() uses it instead of calling
    # Remote.connect() (which would return a different object).
    mock_run_async = AsyncMock()
    remote.run = mock_run_async

    cfg = CluvConfig(
        clusters=["rorqual"],
        slurm={"SBATCH_TIME": "0:01:00"},
        cluster_configs={"rorqual": {"SBATCH_PARTITION": "main"}},
    )

    with (
        patch.object(submit_module, "sync", AsyncMock(return_value=[remote])),
        patch.object(submit_module, "get_config", return_value=cfg),
        patch.object(submit_module.subprocess, "run", return_value=_make_clean_run()),
        patch.object(submit_module.subprocess, "check_output", return_value="cafebabe"),
    ):
        await submit_module.submit(
            cluster="rorqual",
            job_script="scripts/job.sh",
            sbatch_args=[],
            program_args=["echo", "hello"],
        )

    mock_run_async.assert_called_once()
    cmd = mock_run_async.call_args[0][0]
    assert "GIT_COMMIT=cafebabe" in cmd
    assert "SBATCH_TIME=0:01:00" in cmd
    assert "SBATCH_PARTITION=main" in cmd
    assert "sbatch" in cmd
    assert "scripts/job.sh" in cmd


def _make_clean_run():
    from unittest.mock import MagicMock

    result = MagicMock()
    result.stdout = ""
    return result
