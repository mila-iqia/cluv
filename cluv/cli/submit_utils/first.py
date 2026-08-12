import logging
import shlex

from cluv.remote import Remote, run
from cluv.utils import console

logger = logging.getLogger(__name__)


async def cancel_job(remote: Remote | None, job_id: int, print: bool = False) -> str:
    """Cancel the job with the given id on the remote cluster."""
    scancel_command = f"scancel {job_id}"
    if remote:
        output = await remote.get_output(scancel_command, hide=True)
        if print:
            console.print(f"Cancelled job {job_id} on cluster {remote.hostname}.")
    else:
        result = await run(tuple(shlex.split(scancel_command)), hide=True)
        if print:
            console.print(f"Cancelled job {job_id} on the current cluster.")
        output = result.stdout
    return output
