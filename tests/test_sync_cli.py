"""Tests for the `cluv sync` command-line interface."""

from unittest import mock

import pytest

import cluv.__main__ as cluv_main


@pytest.mark.parametrize(
    ("extra_args", "fetch_results"),
    [([], True), (["--no-fetch-results"], False)],
)
def test_sync_fetch_results_flag(
    monkeypatch: pytest.MonkeyPatch, extra_args: list[str], fetch_results: bool
):
    """The sync CLI forwards whether results should be fetched."""
    monkeypatch.setattr(cluv_main, "sync", mock_sync := mock.AsyncMock())

    cluv_main.main(["sync", "tamia", *extra_args])

    mock_sync.assert_awaited_once_with(
        clusters=["tamia"], sync_datasets=True, fetch_results=fetch_results
    )
