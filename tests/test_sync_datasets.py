"""Tests for the dataset part of `cluv sync`, with a `data_source` on a remote cluster.

Jobs on the source cluster read the data at `data_source` directly, so syncing with only that
cluster must not copy the data to this machine and back.
"""

from __future__ import annotations

import importlib
import os
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

from cluv.cache import ProjectStateOnCluster
from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


@pytest.fixture
def config() -> CluvConfig:
    return CluvConfig(
        results_path="/results",
        data_source="mila:/network/datasets/imagenet",
        datasets_path="/scratch/datasets/imagenet",
    )


@pytest.mark.parametrize(
    ("here", "targets", "should_pull"),
    [
        (None, ["mila"], False),
        (None, ["mila", "narval"], True),
        (None, ["narval"], True),
        # The current cluster is a target, and it needs a copy of the data.
        ("narval", ["narval"], True),
        # The current cluster isn't a target, so it doesn't need a copy of the data.
        ("narval", ["mila"], False),
        # The current cluster is the source: the data is already here.
        ("mila", ["narval"], False),
    ],
)
async def test_pull_datasets_only_when_another_cluster_needs_them(
    here: str | None,
    targets: list[str],
    should_pull: bool,
    config: CluvConfig,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        sync_module,
        sync_module.get_remote_without_2fa_prompt.__name__,
        mock.AsyncMock(return_value=Remote(hostname="mila")),
    )
    monkeypatch.setattr(
        sync_module, sync_module.get_datasets_path.__name__, lambda: Path("/local/datasets")
    )
    monkeypatch.setattr(sync_module, "_pull_datasets", mock_pull := mock.AsyncMock())

    # The current cluster is mapped to None, as in `get_cluster_to_remote`.
    cluster_to_remote = {c: None if c == here else Remote(hostname=c) for c in targets}
    await sync_module.pull_datasets_if_needed(here, config, cluster_to_remote)

    assert mock_pull.await_count == (1 if should_pull else 0)


async def test_push_datasets_to_a_cluster_without_datasets_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Pushing the data to a cluster that has no `datasets_path` raises an error.

    Without this error, rsync would copy the data to a folder named `None` on that cluster.
    """
    # If no error is raised, these mocks keep `mkdir` and `rsync` from reaching narval.
    monkeypatch.setattr(Remote, "run", mock.AsyncMock())
    monkeypatch.setattr(sync_module, "run", mock.AsyncMock())
    config = CluvConfig(
        results_path="/results",
        data_source="mila:/network/datasets/imagenet",
        clusters={"narval": PartialClusterConfig()},
    )
    with pytest.raises(RuntimeError, match="datasets_path"):
        await sync_module._push_datasets_to_remote(
            tmp_path, Remote(hostname="narval"), config, ProjectStateOnCluster()
        )


@pytest.fixture
def pushed_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A local `data_source` with one subdirectory, already pushed once to narval.

    Returns the source path, the cached project state and the rsync mock. `mkdir` and `rsync`
    are mocked, so nothing reaches narval.
    """
    source = tmp_path / "data"
    (source / "train").mkdir(parents=True)
    (source / "train" / "a.txt").write_text("a")
    # Everything in the source was last edited a minute before the first push.
    before_push = time.time() - 60
    for path in (source / "train" / "a.txt", source / "train", source):
        os.utime(path, (before_push, before_push))

    monkeypatch.setattr(Remote, "run", mock.AsyncMock())
    monkeypatch.setattr(sync_module, "run", mock_rsync := mock.AsyncMock())
    config = CluvConfig(
        results_path="/results",
        datasets_path="/scratch/datasets/data",
        clusters={"narval": PartialClusterConfig()},
    )
    state = ProjectStateOnCluster()

    async def push() -> None:
        await sync_module._push_datasets_to_remote(
            source, Remote(hostname="narval"), config, state
        )

    return source, push, mock_rsync


async def test_push_datasets_is_skipped_when_nothing_changed(pushed_source):
    source, push, mock_rsync = pushed_source
    await push()
    await push()
    assert mock_rsync.await_count == 1


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="needs a directory that the current user cannot read",
)
async def test_push_datasets_is_not_skipped_when_a_subdirectory_cannot_be_read(
    pushed_source, caplog: pytest.LogCaptureFixture
):
    """An unreadable subdirectory could hide a change, so the push is not skipped."""
    source, push, mock_rsync = pushed_source
    await push()

    locked = source / "locked"
    locked.mkdir()
    before_push = time.time() - 60
    # Old mtimes everywhere: only the failed scan can explain a second push.
    for path in (locked, source):
        os.utime(path, (before_push, before_push))
    locked.chmod(0o000)
    try:
        with caplog.at_level("WARNING", logger=sync_module.logger.name):
            await push()
    finally:
        locked.chmod(0o700)

    assert mock_rsync.await_count == 2
    assert "Could not read all of" in caplog.text


async def test_push_datasets_scans_the_source_outside_the_event_loop_thread(
    pushed_source, monkeypatch: pytest.MonkeyPatch
):
    """The scan of a large source must not block the pushes to the other clusters."""
    source, push, _ = pushed_source
    scan_threads: list[threading.Thread] = []
    real_scan = sync_module._latest_edit_time

    def recording_scan(path: Path):
        scan_threads.append(threading.current_thread())
        return real_scan(path)

    monkeypatch.setattr(sync_module, "_latest_edit_time", recording_scan)
    await push()

    assert len(scan_threads) == 1
    assert scan_threads[0] is not threading.current_thread()


async def test_push_datasets_again_after_a_file_is_added_in_a_subdirectory(pushed_source):
    """A file added inside an existing subdirectory is pushed, not skipped (#260).

    Adding it changes the mtime of the subdirectory, not of the top directory of the source.
    """
    source, push, mock_rsync = pushed_source
    await push()

    after_push = time.time() + 60
    new_file = source / "train" / "b.txt"
    new_file.write_text("b")
    os.utime(new_file, (after_push, after_push))
    os.utime(source / "train", (after_push, after_push))
    # The top directory is untouched, exactly as on a real filesystem.
    assert source.stat().st_mtime < time.time()

    await push()
    assert mock_rsync.await_count == 2


async def test_push_datasets_again_after_a_symlink_to_an_old_file_is_added(
    pushed_source, tmp_path: Path
):
    """A new symlink is a change even when the file it points to is old (#260)."""
    source, push, mock_rsync = pushed_source
    old_file = tmp_path / "elsewhere.txt"
    old_file.write_text("old")
    long_ago = time.time() - 3600
    os.utime(old_file, (long_ago, long_ago))
    await push()

    after_push = time.time() + 60
    link = source / "train" / "link.txt"
    link.symlink_to(old_file)
    os.utime(link, (after_push, after_push), follow_symlinks=False)

    await push()
    assert mock_rsync.await_count == 2


async def test_push_datasets_ignores_changes_in_folders_that_rsync_excludes(pushed_source):
    """A change inside `.git` is not pushed by rsync, so it must not trigger a push."""
    source, push, mock_rsync = pushed_source
    before_push = time.time() - 60
    (source / ".git").mkdir()
    os.utime(source / ".git", (before_push, before_push))
    os.utime(source, (before_push, before_push))
    await push()

    after_push = time.time() + 60
    git_file = source / ".git" / "index"
    git_file.write_text("x")
    os.utime(git_file, (after_push, after_push))
    os.utime(source / ".git", (after_push, after_push))
    os.utime(source, (before_push, before_push))

    await push()
    assert mock_rsync.await_count == 1


async def test_push_datasets_again_after_a_change_inside_a_linked_directory(
    pushed_source, tmp_path: Path
):
    """rsync copies the contents of a linked directory, so a change there is a change (#260).

    The link also points back at its own parent, to check that the walk does not loop.
    """
    source, push, mock_rsync = pushed_source
    before_push = time.time() - 60
    linked = tmp_path / "elsewhere"
    (linked / "deep").mkdir(parents=True)
    (source / "linked").symlink_to(linked, target_is_directory=True)
    (linked / "back").symlink_to(source, target_is_directory=True)
    for path in (linked / "deep", linked, source):
        os.utime(path, (before_push, before_push))
    for link in (source / "linked", linked / "back"):
        os.utime(link, (before_push, before_push), follow_symlinks=False)
    await push()
    await push()
    assert mock_rsync.await_count == 1

    after_push = time.time() + 60
    new_file = linked / "deep" / "c.txt"
    new_file.write_text("c")
    os.utime(new_file, (after_push, after_push))
    os.utime(linked / "deep", (after_push, after_push))

    await push()
    assert mock_rsync.await_count == 2
