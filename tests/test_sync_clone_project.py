"""Tests for `clone_project`, using local git repos in place of GitHub and a cluster."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from cluv.cache import ProjectStateOnCluster
from cluv.cli.sync import clone_project


class LocalRemote:
    """Stand-in for `Remote` that runs commands on this machine."""

    hostname = "fake-cluster"

    async def run(self, command: str, *, warn: bool = False, env=None, **kwargs):
        result = subprocess.run(
            command, shell=True, text=True, capture_output=True, env={**os.environ, **(env or {})}
        )
        if not warn and result.returncode != 0:
            raise RuntimeError(f"{command!r} failed:\n{result.stderr}")
        return result


def git(*args: str, cwd: Path) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def commit(repo: Path, message: str) -> str:
    git("commit", "--allow-empty", "-m", message, cwd=repo)
    return git("rev-parse", "HEAD", cwd=repo)


@pytest.fixture
def repos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    """An `origin` bare repo, a `local` clone of it on a `feature` branch, and a `cluster` path."""
    # Don't pick up the user's git config (e.g. commit signing).
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for var in ("AUTHOR", "COMMITTER"):
        monkeypatch.setenv(f"GIT_{var}_NAME", "Test")
        monkeypatch.setenv(f"GIT_{var}_EMAIL", "test@example.com")
    monkeypatch.delenv("GITHUB_REF", raising=False)

    origin, local, cluster = tmp_path / "origin", tmp_path / "local", tmp_path / "cluster"
    git("init", "--bare", "-b", "master", str(origin), cwd=tmp_path)
    git("clone", str(origin), str(local), cwd=tmp_path)
    (local / "pyproject.toml").write_text("[project]\nname = 'test'\n")
    git("add", "pyproject.toml", cwd=local)
    commit(local, "initial")
    git("push", "origin", "master", cwd=local)
    git("checkout", "-b", "feature", cwd=local)
    commit(local, "feature work")
    git("push", "-u", "origin", "feature", cwd=local)

    monkeypatch.chdir(local)
    return origin, local, cluster


async def sync_to(cluster: Path) -> ProjectStateOnCluster:
    state = ProjectStateOnCluster()
    await clone_project(LocalRemote(), PurePosixPath(cluster), state)  # type: ignore[arg-type]
    return state


async def test_checks_out_local_commit(repos: tuple[Path, Path, Path]):
    _, local, cluster = repos
    state = await sync_to(cluster)
    head = git("rev-parse", "HEAD", cwd=local)
    assert git("rev-parse", "HEAD", cwd=cluster) == head
    assert state.checked_out_git_commit == head


async def test_branch_without_upstream_on_cluster(repos: tuple[Path, Path, Path]):
    """Regression test: a CI run used to leave a local branch without an upstream in the cluster
    clone, which made a later `git pull` from a normal checkout of that branch fail.
    """
    _, local, cluster = repos
    await sync_to(cluster)
    git("checkout", "-B", "feature", "origin/master", cwd=cluster)
    git("branch", "--unset-upstream", cwd=cluster)

    new_commit = commit(local, "more feature work")
    git("push", cwd=local)
    await sync_to(cluster)
    assert git("rev-parse", "HEAD", cwd=cluster) == new_commit


async def test_github_pr_merge_ref(repos: tuple[Path, Path, Path], monkeypatch):
    """In a PR run, the local HEAD is GitHub's merge commit, which is only reachable from the
    `refs/pull/N/merge` ref on the base repo.
    """
    _, local, cluster = repos
    git("checkout", "--detach", "master", cwd=local)
    git("merge", "--no-ff", "--no-edit", "feature", cwd=local)
    merge_commit = git("rev-parse", "HEAD", cwd=local)
    git("push", "origin", "HEAD:refs/pull/1/merge", cwd=local)
    monkeypatch.setenv("GITHUB_REF", "refs/pull/1/merge")

    await sync_to(cluster)
    assert git("rev-parse", "HEAD", cwd=cluster) == merge_commit
