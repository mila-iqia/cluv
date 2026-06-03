#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=0:05:00
#SBATCH --output=$SCRATCH/logs/cluv/%j/slurm-%j.out

# Run the job command passed as an argument when submitting the job ('python main.py' for example)
echo "Running command: $@"
srun uv run "$@"
