#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=0:15:00

# Minimal job script to validate the PyTorch setup.
echo "hostname: $(hostname)"
echo "Date:     $(date)"

# Run the command passed as an argument.
echo "Running command: $@"
srun uv run "$@"
