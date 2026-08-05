#!/bin/bash
# Nibi: 4 of the 8 H100 GPUs of a node (112 cores / 2TB of RAM per node).
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-task=h100:1
#SBATCH --cpus-per-task=14
#SBATCH --mem-per-gpu=64G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
