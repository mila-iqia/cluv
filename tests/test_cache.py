"""Tests for the local project-state cache."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

import cluv.cache
from cluv.cache import CacheContent, ProjectStateOnCluster, read_cache, write_cache


@pytest.fixture
def fake_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    monkeypatch.setattr(cluv.cache, "_get_cache_dir", lambda: cache_dir)
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
