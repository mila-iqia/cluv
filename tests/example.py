"""A script that reads something, and produces some output.

This is a simplified job script, used to test the syncing of the 'dataset' across clusters.
"""

import os
import time
from dataclasses import dataclass
from pathlib import Path

import simple_parsing

SLURM_JOB_ID = int(os.environ["SLURM_JOB_ID"])
SCRATCH = Path(os.environ["SCRATCH"])
SLURM_TMPDIR = Path(os.environ["SLURM_TMPDIR"])

# IDEA: maybe load the cluv config and set the checkpoint_dir
# from cluv.config import load_cluv_config


@dataclass(frozen=True)
class Args:
    # NOTE: This should be the same as the `results_path` in the Cluv config.
    results_path: Path = SCRATCH / "logs" / "cluv" / str(SLURM_JOB_ID)

    # NOTE: This should be the same as the `datasets_path` in the Cluv config.
    datasets_path: Path = Path("tests/data/dataset.csv")

    # Time to wait before producing the result.
    # Can be useful to test and simulate preemption or cancelling jobs.
    wait_duration_seconds: int = 0


def main(args: Args | None = None):
    args = args or simple_parsing.parse(Args, description=__doc__)
    print(f"Job {SLURM_JOB_ID} starts.")

    dataset = args.datasets_path.read_text()
    assert dataset.strip() == 'This is a dummy "dataset".'

    time.sleep(args.wait_duration_seconds)

    print(f"Job {SLURM_JOB_ID} is about to end.")
    results_file = args.results_path / "results.txt"
    with results_file.open("a") as f:
        f.write(f"This is the result of job {SLURM_JOB_ID}\n")


if __name__ == "__main__":
    main()
