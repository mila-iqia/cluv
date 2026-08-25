"""Tests for the local project-state cache."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

import cluv.cache
from cluv.cache import (
    CacheContent,
    Job,
    ProjectStateOnCluster,
    load_jobs,
    read_cache,
    save_job,
    write_cache,
)


@pytest.fixture
def fake_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    monkeypatch.setattr(cluv.cache, cluv.cache._get_cache_dir.__name__, lambda: cache_dir)
    return cache_dir


def test_last_fetch_watermark_roundtrip(fake_cache_dir: Path):
    watermark = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    cache = CacheContent(
        project_states={"mila": ProjectStateOnCluster(last_fetch_watermark=watermark)}
    )
    write_cache(cache)

    reloaded = read_cache()

    assert reloaded.project_states["mila"].last_fetch_watermark == watermark


def test_last_fetch_watermark_defaults_to_none():
    assert ProjectStateOnCluster().last_fetch_watermark is None


def _make_job(job_id: int) -> Job:
    return Job(
        cluster="mila",
        remote=None,
        job_script=Path("scripts/job.sh"),
        sbatch_args={"time": "1:00:00"},
        program_args=["python", "main.py"],
        sbatch_command="bash --login -c '...'",
        n_chunks=None,
        git_commit="deadbeef",
        job_id=job_id,
        submitted_at=datetime(2026, 7, 1, 12, 0, 0),
    )


def test_save_job_and_load_jobs_roundtrip(fake_cache_dir: Path):
    job = _make_job(123)
    save_job(job)

    assert load_jobs() == [job]


def test_load_jobs_skips_and_warns_on_unparseable_records(
    fake_cache_dir: Path, caplog: pytest.LogCaptureFixture
):
    """A record that doesn't match the current Job schema (e.g. written by an older cluv
    version) is skipped rather than raising -- but that's now surfaced via a warning, instead
    of `cluv status jobs` silently showing nothing with no indication why."""
    good_job = _make_job(456)
    cache_path = cluv.cache._get_cached_jobs_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a") as f:
        f.write('{"job_id": 111, "cluster": "mila"}\n')  # old/incompatible schema
        f.write("not even json\n")

    save_job(good_job)

    with caplog.at_level("WARNING"):
        jobs = load_jobs()

    assert jobs == [good_job]
    assert any("Skipped 2 unreadable job record" in message for message in caplog.messages)
