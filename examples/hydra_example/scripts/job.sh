#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=0:05:00

# Note: --output is set by cluv. No worries there.

# Run the job command passed as an argument when submitting the job ('python main.py' for example)
echo "Running command: $@"
# TODO: Hard-coded thing. Replace with some env var of some sort?
srun --output="$SCRATCH/logs/hydra_example/mila_%j/%j_%t_log.out" uv run "$@"
