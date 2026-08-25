import logging
from pathlib import Path

from cluv.config import SbatchArgs
from cluv.slurm import parse_slurm_time

logger = logging.getLogger(__name__)

CHUNK_SIZE = 3  # In hours


def apply_chunking(
    sbatch_args: SbatchArgs,
    job_script: Path | None,
    chunking: int | None,
    env_vars: dict[str, str] | None = None,
) -> tuple[int | None, SbatchArgs]:
    """Split a job into consecutive chunks of `chunking` hours each, if requested.

    The time limit of the (unchunked) job can come from, in order of precedence: the `time`/`t`
    sbatch arg, the `SBATCH_TIMELIMIT` env var, or a `#SBATCH --time=...` directive in the job
    script header.

    Returns the number of chunks (`None` when `chunking` is `None`, i.e. chunking is disabled)
    and `sbatch_args` updated with the `--time`/`--array` directives needed to run them.
    """
    if chunking is None:
        return None, sbatch_args

    time_limit = (
        sbatch_args.get("time")
        or sbatch_args.get("t")
        or (env_vars or {}).get("SBATCH_TIMELIMIT")
        or (job_script and get_time_from_job_script_header(job_script))
    )
    if not time_limit:
        raise ValueError(
            "Could not find a time value for the job, which is required for chunking."
        )

    total_hours = parse_slurm_time(str(time_limit)).total_seconds() / 3600
    # Split the total time into chunks, and round up. Need at least one chunk, even if the total
    # time is less than `chunking`.
    n_chunks = max(int((total_hours + chunking - 1) // chunking), 1)
    logger.info(f"Chunking job into {n_chunks} smaller jobs of {chunking} hours each.")

    sbatch_args = {k: v for k, v in sbatch_args.items() if k not in ("time", "t")}
    sbatch_args["time"] = f"{chunking}:00:00"
    sbatch_args["array"] = f"0-{n_chunks - 1}%1"
    return n_chunks, sbatch_args


def get_time_from_job_script_header(job_script: Path) -> str | None:
    """Return the SLURM time limit from the job script header if it exists."""
    for line in job_script.read_text().splitlines():
        # For case like "#SBATCH --time=1:00:00" or "#SBATCH -t=1:00:00"
        if line.startswith("#SBATCH"):
            if "--time=" in line:
                return line[line.index("--time=") + len("--time=") :].split()[0]
            elif "-t=" in line:
                return line[line.index("-t=") + len("-t=") :].split()[0]

        if not line.strip().startswith("#"):
            # Stop parsing once we leave the header.
            return
