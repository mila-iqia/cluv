"""`cluv sync` checks out the project's git submodules on the clusters (before `uv sync`)."""

import os
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from cluv.cache import ProjectStateOnCluster
from cluv.cli.sync import clone_project


class LocalRemote:
    """Stands in for a `Remote`: runs the commands on this machine."""

    hostname = "localhost"

    async def run(self, command: str, warn: bool = False, hide=False, env=None, **kwargs):
        return subprocess.run(
            command, shell=True, check=not warn, env={**os.environ, **(env or {})}
        )


def git(*args: str, cwd: Path) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def commit_file(repo: Path, name: str, content: str) -> None:
    (repo / name).write_text(content)
    git("add", name, cwd=repo)
    git("commit", "-q", "-m", f"write {name}", cwd=repo)


@pytest.fixture(autouse=True)
def git_env(monkeypatch: pytest.MonkeyPatch):
    # Ignore the user's git config (commit signing, etc.).
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for var in ("NAME", "EMAIL"):
        monkeypatch.setenv(f"GIT_AUTHOR_{var}", "test@example.com")
        monkeypatch.setenv(f"GIT_COMMITTER_{var}", "test@example.com")
    # Submodules with local paths as URLs use the `file` protocol, which git blocks by default.
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "protocol.file.allow")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "always")


async def test_clone_project_checks_out_submodules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    sub = tmp_path / "sub"
    sub.mkdir()
    git("init", "-q", "-b", "master", cwd=sub)
    commit_file(sub, "data.txt", "v1")

    upstream = tmp_path / "upstream"
    upstream.mkdir()
    git("init", "-q", "-b", "master", cwd=upstream)
    commit_file(upstream, "pyproject.toml", '[project]\nname = "proj"\n')
    git("submodule", "add", "-q", str(sub), "sub", cwd=upstream)
    git("commit", "-q", "-m", "add submodule", cwd=upstream)
    # Pushing to the checked-out branch of a non-bare repo updates its worktree.
    git("config", "receive.denyCurrentBranch", "updateInstead", cwd=upstream)

    local = tmp_path / "local"
    git("clone", "-q", "--recurse-submodules", str(upstream), str(local), cwd=tmp_path)
    monkeypatch.chdir(local)

    cluster_repo_dir = tmp_path / "cluster" / "proj"
    project_state = ProjectStateOnCluster()
    await clone_project(LocalRemote(), PurePosixPath(cluster_repo_dir), project_state)  # type: ignore
    assert (cluster_repo_dir / "sub" / "data.txt").read_text() == "v1"

    # Bumping the submodule locally also updates it on the cluster at the next sync.
    commit_file(sub, "data.txt", "v2")
    git("submodule", "update", "-q", "--remote", "sub", cwd=local)
    git("commit", "-q", "-am", "bump submodule", cwd=local)
    git("push", "-q", cwd=local)
    await clone_project(LocalRemote(), PurePosixPath(cluster_repo_dir), project_state)  # type: ignore
    assert (cluster_repo_dir / "sub" / "data.txt").read_text() == "v2"
