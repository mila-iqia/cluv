"""Coverage for the local history cache + sacct backfill in `cluv.history`."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from salvo.history import JobRecord

from cluv import history


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Redirect the history cache into a per-test tmp dir."""
    monkeypatch.setenv("CLUV_HISTORY_DIR", str(tmp_path / "history"))
    return tmp_path / "history"


def _record(
    *,
    job_id: str = "1",
    key: str = "abc",
    cluster: str = "mila",
    state: str = "COMPLETED",
    mem_mb: int = 2048,
    max_rss_mb: int | None = 1500,
    ts: datetime | None = None,
) -> JobRecord:
    return JobRecord(
        job_id=job_id,
        key=key,
        cluster=cluster,
        state=state,
        mem_mb=mem_mb,
        max_rss_mb=max_rss_mb,
        submitted_at=ts or datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
    )


def test_load_returns_empty_on_cold_cache():
    assert history.load("mila", "missing") == []


def test_save_then_load_roundtrip():
    history.save_record(_record(job_id="42"))
    records = history.load("mila", "abc")
    assert len(records) == 1
    assert records[0].job_id == "42"
    assert records[0].max_rss_mb == 1500


def test_save_dedupes_by_job_id():
    history.save_record(_record(job_id="42", max_rss_mb=1500))
    history.save_record(_record(job_id="42", max_rss_mb=1700))  # same id, new RSS
    records = history.load("mila", "abc")
    assert len(records) == 1
    assert records[0].max_rss_mb == 1700


def test_load_returns_newest_first():
    history.save_record(_record(job_id="1", ts=datetime(2026, 5, 1, tzinfo=UTC)))
    history.save_record(_record(job_id="2", ts=datetime(2026, 5, 3, tzinfo=UTC)))
    history.save_record(_record(job_id="3", ts=datetime(2026, 5, 2, tzinfo=UTC)))
    records = history.load("mila", "abc")
    assert [r.job_id for r in records] == ["2", "3", "1"]


def test_comment_roundtrip():
    assert history.build_comment("xyz") == "cluv:v1:xyz"
    assert history.parse_comment("cluv:v1:xyz") == "xyz"
    assert history.parse_comment("other comment") is None
    assert history.parse_comment("") is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("2G", 2048),
        ("4096M", 4096),
        ("512K", 0),  # 512 KiB rounds to 0 MiB exactly, but max(1,...) clamps.
        ("2Gn", 2048),
        ("2Gc", 2048),
        ("1.5G", 1536),
        ("", None),
        ("0", None),
        ("garbage", None),
    ],
)
def test_parse_mem_to_mb(raw, expected):
    result = history.parse_mem_to_mb(raw)
    if expected == 0:
        assert result == 1  # clamp at min 1 MiB
    else:
        assert result == expected


def test_records_from_sacct_groups_steps_under_allocation():
    output = "\n".join(
        [
            "9615842|OUT_OF_MEMORY|0|2G|62|cluv:v1:abc|2026-05-21T17:51:41",
            "9615842.batch|OUT_OF_MEMORY|904K||62||",
            "9615842.extern|COMPLETED|0||62||",
            "9615845|COMPLETED|0|8G|45|cluv:v1:abc|2026-05-21T17:53:00",
            "9615845.batch|COMPLETED|5235M||45||",
            "9615845.extern|COMPLETED|0||45||",
            "9999999|COMPLETED|0|1G|10|some other job|2026-05-20T10:00:00",
        ]
    )
    records = history._records_from_sacct(output, cluster="mila")
    by_id = {r.job_id: r for r in records}
    assert set(by_id) == {"9615842", "9615845"}
    assert by_id["9615842"].state == "OUT_OF_MEMORY"
    assert by_id["9615842"].mem_mb == 2048
    assert by_id["9615845"].state == "COMPLETED"
    assert by_id["9615845"].max_rss_mb == 5235
    assert by_id["9615845"].mem_mb == 8192


def test_records_from_sacct_drops_rows_without_cluv_comment():
    output = "9999999|COMPLETED|0|1G|10|something else|2026-05-20T10:00:00"
    assert history._records_from_sacct(output, cluster="mila") == []


def test_records_from_sacct_skips_alloc_without_submit():
    # A row missing the Submit timestamp can't be ordered; drop it.
    output = "1|COMPLETED|0|1G|5|cluv:v1:abc|"
    assert history._records_from_sacct(output, cluster="mila") == []


def test_list_keys_counts_records():
    history.save_record(_record(job_id="1", key="abc"))
    history.save_record(_record(job_id="2", key="abc"))
    history.save_record(_record(job_id="3", key="other"))
    keys = history.list_keys("mila")
    assert ("mila", "abc", 2) in keys
    assert ("mila", "other", 1) in keys


def test_clear_specific_key():
    history.save_record(_record(job_id="1", key="abc"))
    history.save_record(_record(job_id="2", key="other"))
    deleted = history.clear("mila", "abc")
    assert deleted == 1
    assert history.load("mila", "abc") == []
    assert len(history.load("mila", "other")) == 1


def test_clear_cluster_wipes_all_keys():
    history.save_record(_record(job_id="1", key="abc"))
    history.save_record(_record(job_id="2", key="other"))
    deleted = history.clear("mila")
    assert deleted == 2


def test_load_skips_corrupt_entries():
    history.save_record(_record(job_id="1", key="abc"))
    path = history.cache_dir() / "mila" / "abc.json"
    path.write_text('[{"junk": true}, ' + path.read_text()[1:])
    # The malformed entry is skipped, the real one survives.
    records = history.load("mila", "abc")
    assert len(records) == 1
