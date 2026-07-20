import logging
from pathlib import Path

from cluv.slurm import parse_slurm_time

logger = logging.getLogger(__name__)

CHUNK_SIZE = 3  # In hours


def chunking_update_sbatch_args(
    sbatch_args: list[str], env_vars: dict[str, str], job_script: Path
) -> list[str]:
    """Add the sbatch args (--array and --time) for chunking the job into multiple smaller jobs."""
    # Add job array
    n_chunks = get_n_chunks(sbatch_args, env_vars, job_script)
    logger.info(f"Chunking job into {n_chunks} smaller jobs of {CHUNK_SIZE} hours each.")
    sbatch_args = sbatch_args.copy()

    # Remove any existing --time or -t args, and add the new one at the end.
    # TODO : --time-min case
    sbatch_args = [arg for arg in sbatch_args if not arg.startswith(("--time", "-t"))]
    sbatch_args.append(f"--time={CHUNK_SIZE}:00:00")
    sbatch_args.append(f"--array=0-{n_chunks - 1}%1")

    return sbatch_args


def get_n_chunks(sbatch_args: list[str], env_vars: dict[str, str], job_script: Path) -> int:
    """Get the number of chunks required from the current time limit."""
    # The time limit of a job can be set multiple way :
    # 1. As an arg to sbatch (with --time or -t)
    # 2. As an env variable in the Cluv or the cluster config (with SBATCH_TIMELIMIT)
    # 3. As a directive in the job script header (#SBATCH --time)
    slurm_time = (
        get_time_from_sbatch_args(sbatch_args)
        or env_vars.get("SBATCH_TIMELIMIT")
        or get_time_from_job_script_header(job_script)
    )

    if not slurm_time:
        raise ValueError(
            "Could not find a time value for the job, which is required for chunking."
        )

    time = parse_slurm_time(slurm_time)
    total_hours = time.total_seconds() / 3600  # Convert to hours

    # Split the total time into chunks, and round up.
    n_chunks = int((total_hours + CHUNK_SIZE - 1) // CHUNK_SIZE)

    return n_chunks


def get_time_from_sbatch_args(sbatch_args: list[str]) -> str | None:
    """Return the SLURM time limit from the sbatch args if it exists."""
    # Last occurrence of --time or -t takes precedence, so we iterate in reverse.
    for arg in reversed(sbatch_args):
        if arg.startswith(("--time", "-t")):
            # Like "--time=00:10:00" or "-t=1-02:00:00"
            return arg.split("=")[1]

    return None


def get_time_from_job_script_header(job_script: Path) -> str | None:
    """Return the SLURM time limit from the job script header if it exists."""
    for line in job_script.read_text().splitlines():
        if line.startswith("#SBATCH") and "--time=" in line:
            # Like "#SBATCH --time=1:00:00"
            return line[line.index("--time=") + len("--time=") :].split()[0]

        if not line.strip().startswith("#"):
            # Stop parsing once we leave the header.
            return
