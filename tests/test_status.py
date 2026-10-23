import re
from typing import Literal

import pytest

from cluv.cli import status
from cluv.cli.sync import get_active_remotes
from cluv.utils import console

from .test_integration import REQUIRED_CLUSTERS


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
