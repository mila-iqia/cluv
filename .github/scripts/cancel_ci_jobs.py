"""Cancel every job CI submitted, on every cluster it might have submitted to.

The end-to-end tests each cancel their own job in a `finally` block, but a workflow that is
cancelled (a newer push, a timeout, the run being stopped by hand) kills the pytest process
outright, and a process being killed can't clean up after itself. These jobs ask for most of a GPU
node, so an orphan is expensive - hence an `if: always()` step that sweeps them by name.

Cancels jobs whose name starts with `cluv-ci` (see `job_name` in `tests/test_examples.py`), so it
can't touch a researcher's own jobs on a shared account.
"""

import asyncio
from pathlib import Path

from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import load_cluv_config
from cluv.remote import Remote

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_JOB_NAME_PREFIX = "cluv-ci"


async def cancel_on(cluster: str) -> None:
    remote: Remote | None = await get_remote_without_2fa_prompt(cluster)
    if remote is None:
        print(f"{cluster}: no connection, nothing to do.")
        return
    lines = (
        await remote.get_output(
            'squeue -h -u $USER -o "%i %j"', warn=True, hide=True, display=False
        )
    ).splitlines()
    job_ids = [
        line.split()[0]
        for line in lines
        if len(line.split()) == 2 and line.split()[1].startswith(CI_JOB_NAME_PREFIX)
    ]
    if not job_ids:
        print(f"{cluster}: no {CI_JOB_NAME_PREFIX}* jobs queued.")
        return
    print(f"{cluster}: cancelling {len(job_ids)} job(s): {' '.join(job_ids)}")
    await remote.run(f"scancel {' '.join(job_ids)}", warn=True, hide=True, display=True)


async def main() -> None:
    config = load_cluv_config(REPO_ROOT / "examples" / "imagenet" / "pyproject.toml")
    assert config is not None
    await asyncio.gather(*(cancel_on(cluster) for cluster in config.clusters_names))


if __name__ == "__main__":
    asyncio.run(main())
