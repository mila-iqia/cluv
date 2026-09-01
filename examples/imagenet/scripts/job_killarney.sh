#!/bin/bash
# Killarney: the 4 L40S GPUs of a node (64 cores / 515GB of RAM per node).
# Killarney also has 8x H100 nodes; ask for `--gpus-per-node=h100:8` with 8 tasks to use one.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-node=l40s:4
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-gpu=64G

# Killarney's compute nodes have no internet access.
export UV_OFFLINE=1
export WANDB_MODE=offline

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
