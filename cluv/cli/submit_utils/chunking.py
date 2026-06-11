import datetime
import re
from pathlib import Path

CHUNK_SIZE = 3  # In hours


def get_n_chunks(sbatch_args_str: list[str], env_vars: dict[str, str], job_script: Path) -> int:
    """TODO"""
    # The time of a job can be set at different places :
    #   - As an env variable in the Cluv or the cluster config (with SBATCH_TIMELIMIT)
    #   - As an arg to sbatch (with --time or -t)
    #   - As a directive in the job script header (#SBATCH --time)
    time = (
        _get_time_from_sbatch_args(sbatch_args_str)
        or env_vars.get("SBATCH_TIMELIMIT")
        or _get_time_from_job_script_header(job_script)
    )

    if not time:
        raise ValueError(
            "Could not find a time value for the job, which is required for chunking."
        )

    total_time = parse_time_arg(time)
    total_hours = total_time.total_seconds() / 3600  # Total hours

    # Split the total time into 3h chunks, and round up.
    n_chunks = int((total_hours + CHUNK_SIZE - 1) // CHUNK_SIZE)

    return n_chunks


def _get_time_from_sbatch_args(sbatch_args_str: list[str]) -> str | None:
    """Return the SLURM time limit from the sbatch args if it exists."""
    for arg in sbatch_args_str:
        if arg.startswith(("--time", "-t")):
            # Like "--time=00:10:00" or "-t=1-02:00:00"
            return arg.split("=")[1]

    return None


def _get_time_from_job_script_header(job_script: Path) -> str | None:
    """Return the SLURM time limit from the job script header if it exists."""
    for line in job_script.read_text().splitlines():
        if line.startswith("#SBATCH") and "--time=" in line:
            # Like "#SBATCH --time=1:00:00"
            return line[line.index("--time=") + len("--time=") :].split()[0]

        if not line.strip().startswith("#"):
            # Stop parsing once we leave the header.
            return


def parse_time_arg(time: str) -> datetime.timedelta:
    """Parse a time value from the sbatch format to a timedelta object."""
    # The SLURM time format is days-hours:minutes:seconds, with the days part optionnal.
    match = re.match(r"(?:(\d+)-)?(\d{1,2}):(\d{2}):(\d{2})", time)
    if not match:
        raise ValueError(f"Could not parse time value: {time}")

    return datetime.timedelta(
        days=int(match.group(1) or 0),
        hours=int(match.group(2)),
        minutes=int(match.group(3)),
        seconds=int(match.group(4)),
    )
