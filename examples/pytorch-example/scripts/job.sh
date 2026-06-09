#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --time=0:15:00
# NOTE: This will be overwritten by Cluv anyway. Can't be left empty, else the job submission will
# fail on Trillium and Trillium-gpu, since it would be in $HOME which is apparently read-only from
# compute nodes.
#SBATCH --output=/scratch/your_username/slurm-%j.out

# Minimal job script to validate the PyTorch setup.
echo "hostname: $(hostname)"
echo "Date:     $(date)"

# Run the command passed as an argument.
echo "Running command: $@"
srun uv run "$@"
