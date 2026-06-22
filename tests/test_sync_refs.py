"""Unit tests for the git ref resolution used by `cluv sync` in GitHub Actions."""

import pytest

from cluv.cli.sync import _github_pr_ref


@pytest.mark.parametrize(
    "github_ref",
    ["refs/pull/72/merge", "refs/pull/123/head"],
)
def test_pr_ref_is_returned(monkeypatch: pytest.MonkeyPatch, github_ref: str):
    monkeypatch.setenv("GITHUB_REF", github_ref)
    assert _github_pr_ref() == github_ref


@pytest.mark.parametrize(
    "github_ref",
    [
        "refs/heads/master",
        "refs/tags/v0.1.0",
        "refs/pull//merge",
        "refs/pull/72/merge; rm -rf /",
        "",
        "   ",
    ],
)
def test_non_pr_ref_is_ignored(monkeypatch: pytest.MonkeyPatch, github_ref: str):
    monkeypatch.setenv("GITHUB_REF", github_ref)
    assert _github_pr_ref() is None


def test_unset_github_ref(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("GITHUB_REF", raising=False)
    assert _github_pr_ref() is None
