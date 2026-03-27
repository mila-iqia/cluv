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
    """Return an active RemoteV2 for *cluster*, skip the test if not connected."""
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

    # Patch LocalV2.run_async so we don't actually git push, and patch
    # login so we get our pre-connected remote back without any 2FA.
    with (
        patch.object(sync_module, "login", AsyncMock(return_value=[remote])),
        patch("milatools.utils.local_v2.LocalV2.run_async", AsyncMock()),
        patch.object(sync_module, "install_uv", AsyncMock()),
        patch.object(sync_module, "clone_project", AsyncMock()),
    ):
        remotes = await sync_module.sync(clusters=["tamia"])

    assert len(remotes) == 1
    assert remotes[0].hostname == "tamia"


# ---------------------------------------------------------------------------
# cluv submit — rorqual integration
# ---------------------------------------------------------------------------


async def test_submit_rorqual_dry_run():
    """Smoke-test: submit reaches rorqual and calls sbatch (dry-run via --test).

    Uses `sbatch --test-only` to validate the job script without actually
    queuing a job. Verifies that GIT_COMMIT and SBATCH_* env vars are passed
    through correctly.
    """
    from pathlib import Path
    from unittest.mock import AsyncMock, patch

    from cluv.cli import submit as submit_module
    from cluv.config import CluvConfig, SubmitConfig

    remote = await _require_remote("rorqual")

    captured_commands = []

    async def capture_run_async(cmd, **kwargs):
        captured_commands.append(cmd)
        # Run the real command on the remote so we exercise the SSH path.
        return await original_run_async(cmd, **kwargs)

    original_run_async = remote.run_async
    remote.run_async = capture_run_async

    cfg = CluvConfig(
        clusters=["rorqual"],
        submit=SubmitConfig(job_script="scripts/job.sh"),
        slurm={"SBATCH_TIME": "0:01:00"},
        cluster_configs={"rorqual": {"SBATCH_PARTITION": "main"}},
    )

    with (
        patch.object(submit_module, "sync", AsyncMock(return_value=[remote])),
        patch.object(submit_module, "get_config", return_value=cfg),
        patch.object(submit_module.subprocess, "run", return_value=_make_clean_run()),
        patch.object(
            submit_module.subprocess, "check_output", return_value="cafebabe"
        ),
    ):
        await submit_module.submit(
            cluster="rorqual",
            command=["echo", "hello"],
            job_script=None,
            no_sync=True,
        )

    assert len(captured_commands) == 1
    cmd = captured_commands[0]
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
