#!/bin/bash
#SBATCH --job-name=cluv-test
#SBATCH --output=slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --time=0:05:00

# Minimal test job for cluv submit integration tests.
echo "GIT_COMMIT=${GIT_COMMIT}"
echo "hostname: $(hostname)"
echo "args: $*"
