import argparse
import logging
from pathlib import Path

from cluv.sbatch_args import SbatchArgs
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

    raises:
    - ValueError if `chunking` is true-ish and there is already an --array directive in the
      sbatch args.

    Returns:
    - the number of chunks (`None` when `chunking` is `None`, i.e. chunking is disabled)
    - `sbatch_args_from_config` with all previous '-t' or 'time' keys replaced by a single 'time'
       key with the chunk length (in hours) and 'array' with the right value.
    """
    if not chunking:
        return None, sbatch_args
    if chunking >= 99:
        raise ValueError(
            "Chunking cannot be 99 hours or more! "
            "(also, it makes little sense to use such large chunks! We recommend you try 3/6/12 hours.)"
        )
    if "array" in sbatch_args:
        raise ValueError(
            "Cannot use the `--chunking` option if there is already an 'array' key in the job configuration."
        )
    time_limit = None
    # Use the last value from either --time or -t
    for key, val in sbatch_args.items():
        if key in ("time", "t"):
            time_limit = val
    # If still not found, use the env vars or the job script header.
    if time_limit is None:
        time_limit = (env_vars or {}).get("SBATCH_TIMELIMIT") or (
            job_script and get_time_from_job_script_header(job_script)
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

    sbatch_args_from_config = {k: v for k, v in sbatch_args.items() if k not in ("time", "t")}
    sbatch_args_from_config["time"] = f"{chunking:02d}:00:00"
    sbatch_args_from_config["array"] = f"0-{n_chunks - 1}%1"
    return n_chunks, sbatch_args_from_config


def get_time_from_sbatch_args(sbatch_args: list[str]) -> str | None:
    """Return the SLURM time limit from the sbatch args if it exists."""
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("-t", "--time", dest="time", default=argparse.SUPPRESS)
    args, _ = parser.parse_known_args(sbatch_args)

    return getattr(args, "time", None)


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
