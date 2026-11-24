import asyncio
import importlib
import re
from typing import Literal
from unittest import mock

import pytest

from cluv.cli import status
from cluv.cli.sync import get_active_remotes
from cluv.utils import console

from .test_integration import REQUIRED_CLUSTERS

# `cluv.cli` re-exports the `status` function, so `cluv.cli.status` as an attribute
# would hit the function, not the module. Go through `importlib` instead.
status_module = importlib.import_module("cluv.cli.status")


@pytest.mark.slow
@pytest.mark.timeout(60)
@pytest.mark.parametrize("table", ["clusters", "jobs", "all"])
@pytest.mark.parametrize("all_jobs", [True, False])
async def test_cluv_status(table: Literal["clusters", "jobs", "all"], all_jobs: bool):

    remotes = await get_active_remotes()
    clusters = [remote.hostname for remote in remotes]
    if not all(required_cluster in clusters for required_cluster in REQUIRED_CLUSTERS):
        pytest.fail("Don't have an active connection to all the required clusters!")

    with console.capture() as cap:
        await status(table=table, all_jobs=all_jobs)
    output = cap.get()
    print(output)
    if table in ("all", "clusters"):
        for cluster in clusters:
            assert f"● {cluster}" in output
    if table in ("all", "jobs"):
        last_line = output.strip().splitlines()[-2]  # [-1] is ╰────────────────╯
        if not all_jobs:
            # Showing 10 / 98 cluv jobs.
            assert re.search(r"Showing \d+ / \d+ cluv jobs", last_line)
            assert "Use --all-jobs to show all jobs." in last_line
        if all_jobs:
            # TODO: do we still want to print "Showing 98/98 jobs"? Why? If we don't, then fix this.
            assert re.search(r"Showing \d+ / \d+ cluv jobs", last_line)
            assert "Use --all-jobs to show all jobs." not in last_line

        # TODO: add a better check for the "jobs" portion of the output (varies based on whether the current
        # user ran jobs, and whether the output fits within the console width. It's a bit hard to check.
        # columns = [
        #     "Cluster",
        #     "Job ID",
        #     "Git commit",
        #     "Submitted at",
        #     "Job status",
        #     "Waiting time",
        #     "Elapsed time",
        # ]
        # # Trying to check if there is a single line that has all the column names, but it actually
        # # overflows to two lines sometimes.
        # assert re.search(r"\b" + r"\b.*\b".join(columns) + r"\b", output)

        # There should be "Cluster", some whitespace, and then "Job ID" in one of the lines.
        # This is somewhat reliable, those columns don't seem to wrap around.
        assert re.search(r"Cluster\s+Job ID", output)


async def _never_returns(*args, **kwargs):
    """Stand-in for a command on a cluster that is in maintenance: it never answers."""
    await asyncio.Event().wait()


@pytest.mark.timeout(30)
async def test_get_cluster_status_times_out_on_unresponsive_cluster(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A cluster that never answers is reported offline with a warning, not awaited forever (#297)."""
    remote = mock.Mock()
    remote.get_output = _never_returns
    monkeypatch.setattr(
        status_module, "get_remote_without_2fa_prompt", mock.AsyncMock(return_value=remote)
    )
    monkeypatch.setattr(status_module, "CLUSTER_TIMEOUT_SECONDS", 0.1)

    with caplog.at_level("WARNING"):
        # The outer `wait_for` only keeps this test from hanging when the timeout is missing.
        result = await asyncio.wait_for(
            status_module.get_cluster_status("rorqual", disabled_clusters={}), timeout=5
        )

    assert result == status_module.get_default_cluster_status("rorqual")
    assert any("Connection to rorqual timeout" in message for message in caplog.messages)


@pytest.mark.timeout(30)
async def test_fetch_live_job_info_times_out_on_unresponsive_cluster(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Jobs on a cluster that never answers are skipped with a warning (#297)."""
    monkeypatch.setattr(
        status_module, "get_remote_without_2fa_prompt", mock.AsyncMock(return_value=mock.Mock())
    )
    monkeypatch.setattr(status_module, "run_sacct", _never_returns)
    monkeypatch.setattr(status_module, "CLUSTER_TIMEOUT_SECONDS", 0.1)

    with caplog.at_level("WARNING"):
        result = await asyncio.wait_for(
            status_module.fetch_live_job_info("rorqual", [123], disabled_clusters={}), timeout=5
        )

    assert result == {}
    assert any("Connection to rorqual timeout" in message for message in caplog.messages)


@pytest.mark.timeout(30)
async def test_one_unresponsive_cluster_does_not_hold_back_the_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The clusters that do answer are still reported when another one hangs (#297)."""
    hanging = mock.Mock()
    hanging.get_output = _never_returns
    answering = mock.Mock()
    answering.get_output = mock.AsyncMock(return_value=status_module._SEP.join([""] * 5))
    remotes = {"rorqual": hanging, "narval": answering}

    async def fake_get_remote(cluster: str):
        return remotes[cluster]

    monkeypatch.setattr(status_module, "get_remote_without_2fa_prompt", fake_get_remote)
    monkeypatch.setattr(status_module, "CLUSTER_TIMEOUT_SECONDS", 0.1)

    rorqual, narval = await asyncio.wait_for(
        asyncio.gather(
            status_module.get_cluster_status("rorqual", disabled_clusters={}),
            status_module.get_cluster_status("narval", disabled_clusters={}),
        ),
        timeout=5,
    )

    assert not rorqual.online
    assert narval.online
