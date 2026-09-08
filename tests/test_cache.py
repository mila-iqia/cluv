"""Tests for the local project-state cache."""

import json
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


def test_load_jobs_drops_and_warns_on_unparseable_records(
    fake_cache_dir: Path, caplog: pytest.LogCaptureFixture
):
    """A record that doesn't match the current Job schema and isn't a recognizable pre-refactor
    record either (e.g. missing fields entirely) is dropped rather than raising -- but that's
    now surfaced via a warning, instead of `cluv status jobs` silently showing nothing with no
    indication why."""
    good_job = _make_job(456)
    cache_path = cluv.cache._get_cached_jobs_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a") as f:
        f.write('{"job_id": 111, "cluster": "mila"}\n')  # missing required fields
        f.write("not even json\n")

    save_job(good_job)

    with caplog.at_level("WARNING"):
        jobs = load_jobs()

    assert jobs == [good_job]
    assert any("Dropped 2 unreadable job record" in message for message in caplog.messages)

    # The two bad lines are gone from disk -- this only needs to happen once.
    assert cache_path.read_text().splitlines() == [
        line for line in cache_path.read_text().splitlines() if "not even json" not in line
    ]
    assert len(cache_path.read_text().splitlines()) == 1


def _write_legacy_record(cache_path: Path, **overrides) -> None:
    record = {
        "job_id": 789,
        "cluster": "mila",
        "job_script": "scripts/job.sh",
        "git_commit": "cafebabe",
        "submitted_at": "2026-07-01T12:00:00",
        "sbatch_args": ["--time=1:00:00", "-N", "2", "--requeue"],
        "program_args": ["python", "main.py"],
        "n_chunks": None,
        **overrides,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def test_load_jobs_migrates_pre_refactor_records(
    fake_cache_dir: Path, caplog: pytest.LogCaptureFixture
):
    """A record from before Job.sbatch_args became a dict (and sbatch_command was added) is
    converted to the current schema instead of being dropped -- cluster/job_id/submitted_at/
    git_commit/program_args are all still perfectly good information."""
    cache_path = cluv.cache._get_cached_jobs_path()
    _write_legacy_record(cache_path)

    with caplog.at_level("INFO"):
        (job,) = load_jobs()

    assert job.job_id == 789
    assert job.cluster == "mila"
    assert job.job_script == Path("scripts/job.sh")
    assert job.git_commit == "cafebabe"
    assert job.submitted_at == datetime(2026, 7, 1, 12, 0, 0)
    assert job.program_args == ["python", "main.py"]
    assert job.sbatch_args == {"time": "1:00:00", "N": "2", "requeue": True}
    assert job.sbatch_command == ""  # no equivalent existed in the old schema
    assert any("Migrated 1 job record" in message for message in caplog.messages)


def test_load_jobs_rewrites_the_cache_file_after_migrating(fake_cache_dir: Path):
    """Migration only needs to happen once: after the first load_jobs(), the file on disk
    should already be in the current schema."""
    cache_path = cluv.cache._get_cached_jobs_path()
    _write_legacy_record(cache_path)

    first = load_jobs()
    on_disk = json.loads(cache_path.read_text().splitlines()[0])
    assert isinstance(on_disk["sbatch_args"], dict)
    assert "sbatch_command" in on_disk

    second = load_jobs()
    assert second == first
