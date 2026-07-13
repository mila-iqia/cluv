"""Tests for `cluv disable` / `cluv enable` commands."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import cluv.__main__ as cluv_main
import cluv.cache
from cluv.cache import (
    CacheContent,
    disable_cluster,
    enable_cluster,
    get_disabled_clusters,
    is_cluster_disabled,
    read_cache,
    write_cache,
)
from cluv.cli.disable import parse_duration


# ---------------------------------------------------------------------------
# parse_duration tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "period, expected",
    [
        # integer → days
        ("1", timedelta(days=1)),
        ("3", timedelta(days=3)),
        # Slurm HH:MM:SS
        ("6:00:00", timedelta(hours=6)),
        ("0:30:00", timedelta(minutes=30)),
        ("1:00:00", timedelta(hours=1)),
        # Slurm D-HH:MM:SS
        ("1-06:00:00", timedelta(days=1, hours=6)),
        # suffixed single
        ("2h", timedelta(hours=2)),
        ("30m", timedelta(minutes=30)),
        ("1d", timedelta(days=1)),
        ("45s", timedelta(seconds=45)),
        # suffixed multi-token
        ("1d 6h", timedelta(days=1, hours=6)),
        ("2H 30M", timedelta(hours=2, minutes=30)),
    ],
)
def test_parse_duration(period: str, expected: timedelta):
    assert parse_duration(period) == expected


def test_parse_duration_invalid():
    with pytest.raises(ValueError):
        parse_duration("not-a-duration")


# ---------------------------------------------------------------------------
# cache helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_cache(monkeypatch: pytest.MonkeyPatch):
    """Monkeypatch read_cache/write_cache to use an in-memory CacheContent."""
    cache = CacheContent()

    def _write_cache(content: CacheContent) -> None:
        nonlocal cache
        cache = content

    def _read_cache() -> CacheContent:
        return cache

    monkeypatch.setattr(cluv.cache, read_cache.__name__, _read_cache)
    monkeypatch.setattr(cluv.cache, write_cache.__name__, _write_cache)


def test_disable_cluster_indefinitely(isolated_cache):
    disable_cluster("mila")
    disabled = get_disabled_clusters()
    assert "mila" in disabled
    assert disabled["mila"].disabled_until is None
    assert is_cluster_disabled("mila")


def test_disable_cluster_with_expiry(isolated_cache):
    future = datetime.now(tz=timezone.utc) + timedelta(hours=2)
    disable_cluster("narval", disabled_until=future)
    disabled = get_disabled_clusters()
    assert "narval" in disabled
    assert disabled["narval"].disabled_until is not None
    assert is_cluster_disabled("narval")


def test_disable_cluster_expired(isolated_cache):
    past = datetime.now(tz=timezone.utc) - timedelta(seconds=1)
    disable_cluster("narval", disabled_until=past)
    # Should be auto-removed on check.
    assert not is_cluster_disabled("narval")
    assert "narval" not in get_disabled_clusters()


def test_enable_cluster(isolated_cache):
    disable_cluster("mila")
    assert is_cluster_disabled("mila")
    was_disabled = enable_cluster("mila")
    assert was_disabled
    assert not is_cluster_disabled("mila")


def test_enable_cluster_not_disabled(isolated_cache):
    was_disabled = enable_cluster("mila")
    assert not was_disabled


# ---------------------------------------------------------------------------
# CLI interface via cluv_main.main
# ---------------------------------------------------------------------------


def test_cli_disable_then_enable(isolated_cache, capsys):
    cluv_main.main(["disable", "mila"])
    assert is_cluster_disabled("mila")

    cluv_main.main(["enable", "mila"])
    assert not is_cluster_disabled("mila")


def test_cli_disable_with_period(isolated_cache):
    cluv_main.main(["disable", "narval", "2h"])
    disabled = get_disabled_clusters()
    assert "narval" in disabled
    assert disabled["narval"].disabled_until is not None
    remaining = disabled["narval"].disabled_until - datetime.now(tz=timezone.utc)
    # Should be approximately 2 hours.
    assert timedelta(hours=1, minutes=55) < remaining < timedelta(hours=2, minutes=5)


def test_cli_enable_not_disabled(isolated_cache, capsys):
    # Should not raise; just prints a message.
    cluv_main.main(["enable", "mila"])


def test_cli_disable_unknown_cluster(isolated_cache, capsys):
    """Disabling a cluster that is not in the config should print an error and not disable it."""
    cluv_main.main(["disable", "unknown_cluster_xyz"])
    assert not is_cluster_disabled("unknown_cluster_xyz")
    out = capsys.readouterr().out
    assert "unknown_cluster_xyz" in out
    assert "not defined in the config" in out
    # The error message should list available clusters; mila is always in the project config.
    assert "mila" in out
