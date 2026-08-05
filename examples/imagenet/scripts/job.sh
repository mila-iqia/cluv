#!/bin/bash
# Generic job script: one node, one GPU. Used on clusters that don't have a `job_script_path` of
# their own in the pyproject.toml, and as the default for `cluv submit first`.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-task=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-gpu=32G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
