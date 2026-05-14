"""Coverage for the OOM-aware resubmit loop in `cluv submit`.

Two seams per the proposal:
- a fake `get_job_status` returning a scripted sacct sequence
- a fake `sbatch` recording the env-var overrides it was called with
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

import pytest

from cluv.cli import submit as submit_module
from cluv.cli.submit import _retry_on_oom
from cluv.config import RetryConfig, get_config
from cluv.remote import Remote


@pytest.fixture
def project_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Mirror the fixture in `tests/test_submit.py` so retry tests pick up a config."""
    get_config.cache_clear()
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    project_dir = tmp_path / "my_project"
    project_dir.mkdir()
    monkeypatch.chdir(project_dir)
    (project_dir / "pyproject.toml").write_text(
        textwrap.dedent(
            """\
            [tool.cluv]
            results_path = "results"
            [tool.cluv.env]
            SBATCH_MEM = "16G"
            """
        )
    )
    return project_dir


class _ScriptedRunner:
    """Bundles fake `get_job_status` / `get_max_rss_mb` / `sbatch` patches.

    `states` is the sequence sacct returns; one entry per `_wait_terminal` call.
    `sbatch_jobs` is the sequence of job ids that successive sbatch calls return.
    `recorded` captures each sbatch call's env_overrides dict (deep-copied so
    later mutations don't bleed back).
    """

    def __init__(self, states: list[str], sbatch_jobs: list[int]) -> None:
        self.states = list(states)
        self.sbatch_jobs = list(sbatch_jobs)
        self.recorded: list[dict[str, str]] = []
        self.sleep_calls = 0

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fake_get_job_status(remote: Remote, job_id: int) -> str:
            return self.states.pop(0)

        async def fake_get_max_rss_mb(remote: Remote, job_id: int) -> int | None:
            return None  # exercise the "MaxRSS unreliable" path

        async def fake_sbatch(
            remote: Remote,
            job_script: Path,
            sbatch_args: list[str],
            program_args: list[str],
            git_commit: str,
            env_overrides: dict[str, str] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            self.recorded.append(dict(env_overrides or {}))
            return subprocess.CompletedProcess(
                args=["sbatch"], returncode=0, stdout=f"{self.sbatch_jobs.pop(0)}\n", stderr=""
            )

        async def fake_sleep(_seconds: float) -> None:
            self.sleep_calls += 1

        monkeypatch.setattr(submit_module, "get_job_status", fake_get_job_status)
        monkeypatch.setattr(submit_module, "get_max_rss_mb", fake_get_max_rss_mb)
        monkeypatch.setattr(submit_module, "sbatch", fake_sbatch)
        monkeypatch.setattr(submit_module.asyncio, "sleep", fake_sleep)


async def test_retry_bumps_mem_then_completes(
    project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _ScriptedRunner(states=["OUT_OF_MEMORY", "COMPLETED"], sbatch_jobs=[1002])
    runner.install(monkeypatch)

    job_id = await _retry_on_oom(
        remote=Remote(hostname="mila"),
        job_id=1001,
        job_script=Path("scripts/job.sh"),
        sbatch_args=[],
        program_args=[],
        git_commit="abcdef",
        retry=RetryConfig(on_oom=["bump_mem(2x, max=128G)", "fail"], max_hops=5),
    )

    assert job_id == 1002, "loop should return the last resubmitted job id"
    assert len(runner.recorded) == 1, "exactly one resubmit before COMPLETED"
    overrides = runner.recorded[0]
    assert overrides["CLUV_HOP"] == "1/5"
    # 16G * 2 = 32G; salvo emits MiB strings.
    assert overrides["SBATCH_MEM"] == f"{32 * 1024}M"


async def test_retry_grows_mem_across_hops(
    project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _ScriptedRunner(
        states=["OUT_OF_MEMORY", "OUT_OF_MEMORY", "COMPLETED"],
        sbatch_jobs=[2002, 2003],
    )
    runner.install(monkeypatch)

    job_id = await _retry_on_oom(
        remote=Remote(hostname="mila"),
        job_id=2001,
        job_script=Path("scripts/job.sh"),
        sbatch_args=[],
        program_args=[],
        git_commit="abcdef",
        retry=RetryConfig(on_oom=["bump_mem(2x, max=128G)", "fail"], max_hops=5),
    )

    assert job_id == 2003
    hops = [r["CLUV_HOP"] for r in runner.recorded]
    mems = [r["SBATCH_MEM"] for r in runner.recorded]
    assert hops == ["1/5", "2/5"], "hop counter increments on each resubmit"
    # 16G -> 32G -> 64G under bump_mem(2x).
    assert mems == [f"{32 * 1024}M", f"{64 * 1024}M"]


async def test_retry_caps_at_max_hops(
    project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _ScriptedRunner(
        states=["OUT_OF_MEMORY", "OUT_OF_MEMORY"],
        sbatch_jobs=[3002, 3003],
    )
    runner.install(monkeypatch)

    job_id = await _retry_on_oom(
        remote=Remote(hostname="mila"),
        job_id=3001,
        job_script=Path("scripts/job.sh"),
        sbatch_args=[],
        program_args=[],
        git_commit="abcdef",
        retry=RetryConfig(on_oom=["bump_mem(1.5x, max=128G)", "fail"], max_hops=2),
    )

    assert job_id == 3003, "loop stops resubmitting once max_hops is reached"
    assert len(runner.recorded) == 2, "exactly max_hops resubmits"
    assert runner.recorded[-1]["CLUV_HOP"] == "2/2"


async def test_retry_terminates_on_fail_step(
    project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _ScriptedRunner(states=["OUT_OF_MEMORY"], sbatch_jobs=[])
    runner.install(monkeypatch)

    # 100G * 5 capped at 128G is reachable; force fall-through to fail by
    # asking for a bump that the policy declines (already at the cap).
    job_id = await _retry_on_oom(
        remote=Remote(hostname="mila"),
        job_id=4001,
        job_script=Path("scripts/job.sh"),
        sbatch_args=[],
        program_args=[],
        git_commit="abcdef",
        retry=RetryConfig(on_oom=["fail"], max_hops=5),
    )

    assert job_id == 4001, "fail step returns the current job id without resubmitting"
    assert runner.recorded == [], "no resubmit when policy returns FailStep"


async def test_retry_returns_immediately_on_non_oom_terminal(
    project_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _ScriptedRunner(states=["COMPLETED"], sbatch_jobs=[])
    runner.install(monkeypatch)

    job_id = await _retry_on_oom(
        remote=Remote(hostname="mila"),
        job_id=5001,
        job_script=Path("scripts/job.sh"),
        sbatch_args=[],
        program_args=[],
        git_commit="abcdef",
        retry=RetryConfig(on_oom=["bump_mem(2x, max=128G)", "fail"], max_hops=5),
    )

    assert job_id == 5001
    assert runner.recorded == []


def test_submit_is_noop_path_when_retry_is_none(project_dir: Path) -> None:
    """When `[tool.cluv.retry]` is absent, `cluv_config.retry` is None.

    This is the cheap structural guarantee: a config without the retry section
    deserializes to `retry=None`, so `submit()` skips `_retry_on_oom` on the
    very first branch and the new code path is dormant.
    """
    config = get_config()
    assert config.retry is None
