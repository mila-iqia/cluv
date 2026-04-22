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

N_STEPS = 10

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
    with open(FILE_PATH, "w") as f:
        f.write(str(n))
    print(f"Counter value written to {FILE_PATH}: {n} \n")
    sleep(5)

    # Simulate a random interruption to demonstrate checkpointing (10% chance to interrupt)
    if random.random() < 0.1:
        print("Simulating an interruption...\n")
        exit(1)

    return n

def main() -> None:
    print("Hello from simple-checkpoint!")

    counter = init()

    for i in range(N_STEPS):
        print(f"Step {i+1}/{N_STEPS}")
        counter = foo(counter)

if __name__ == "__main__":
    main()
