#!/bin/bash
# Fir: the 4 H100 GPUs of a node (48 cores / 1.1TB of RAM per node).
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-node=h100:4
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-gpu=64G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
