#!/bin/bash
#SBATCH --output=logs/%j/slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --mem=8G
#SBATCH --time=0:05:00

# Minimal test job for cluv submit.
echo "hostname: $(hostname)"
echo "Date:     $(date)"

# Sync dependencies.
uv sync

# Run the command passed as an argument.
echo "Running command: $@"
uv run "$@"
