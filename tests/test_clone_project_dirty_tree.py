"""Regression test: `clone_project` must reset a dirty working tree before checkout.

`cluv sync` treats a cluster's cloned repo as a pure mirror of whatever commit was pushed. If
the working tree ever ends up with local modifications there (e.g. from a sync that got
interrupted, or from two clusters that share a filesystem racing each other -- see
`test_sync_shared_filesystem.py`), every subsequent `git checkout` fails with "local changes
would be overwritten by checkout", permanently wedging `cluv sync` for that cluster until
someone manually fixes it on the remote. This is exactly what made
`test_hydra_example[scripts/job.sh-first]` keep failing in CI even after the race itself was
fixed: the shared trillium/trillium-gpu clone was already dirty from an earlier race. Reset the
tree back to a clean state right after fetching, before any checkout, so `clone_project` can
recover on its own.
"""

from __future__ import annotations

import importlib
import subprocess
from pathlib import PurePosixPath

import pytest

from cluv.cache import ProjectStateOnCluster
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


async def test_clone_project_resets_before_checking_out(monkeypatch: pytest.MonkeyPatch):
    commands: list[str] = []

    async def fake_run(self, command: str, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(Remote, "run", fake_run)
    remote = Remote(hostname="foo")

    await sync_module.clone_project(
        remote,
        project_path=PurePosixPath("/home/user/proj"),
        project_state=ProjectStateOnCluster(),
    )

    fetch_index = next(i for i, c in enumerate(commands) if "fetch --all --prune" in c)
    reset_index = next(i for i, c in enumerate(commands) if "reset --hard" in c)
    checkout_index = next(i for i, c in enumerate(commands) if "checkout" in c)

    assert fetch_index < reset_index < checkout_index, commands
