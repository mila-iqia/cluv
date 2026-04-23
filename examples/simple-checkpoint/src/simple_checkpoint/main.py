"""
Simple checkpointing example.
This example will create a file counter.txt, and write the counter value to it. If the file already exists, it will read the counter value from it and resume from there.
The counter will be incremented every 5 seconds, and the new value will be written to the file. If the script is interrupted, it will resume from the last checkpoint when restarted.
"""

from pathlib import Path
from time import sleep
import os
import random

# Get JOB_ID from environment variable, or generate a random one if not found (e.g., when running locally)
rand = str(random.randint(1000, 9999))
JOB_ID = os.environ.get("SLURM_JOB_ID", rand)
FILE_PATH = Path("logs") / JOB_ID / "counter.txt"

N_STEPS = 100


def init() -> int:
    if FILE_PATH.exists():
        with open(FILE_PATH, "r") as f:
            counter = int(f.read())
        print(f"Resuming from checkpoint. Counter value: {counter}\n")
    else:
        print("No checkpoint found. Starting from 0.")
        FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        counter = 0
    return counter


def foo(n: int) -> int:
    n += 1
    print(f"Counter value: {n} \n")
    sleep(5)

    return n


def checkpoint(n: int) -> None:
    with open(FILE_PATH, "w") as f:
        f.write(str(n))
    print(f"Counter value written to {FILE_PATH}: {n} \n")


def main() -> None:
    print("Hello from simple-checkpoint!")

    # Initialize counter from checkpoint if it exists, otherwise start from 0
    counter = init()

    # Loop with our task and intermediate checkpointing
    for i in range(N_STEPS):
        print(f"Step {i+1}/{N_STEPS}")
        counter = foo(counter)

        if i % 5 == 0:  # Checkpoint every 5 steps
            checkpoint(counter)

    # Final checkpoint at the end of the loop
    checkpoint(counter)

if __name__ == "__main__":
    main()
