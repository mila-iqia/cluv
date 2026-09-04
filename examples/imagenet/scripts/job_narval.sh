#!/bin/bash
# Narval: 1 of the 4 A100-40GB GPUs of a node (48 cores / 510GB of RAM per node), so 1/4 of its
# cores. Note: compute nodes on Narval don't have internet access.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=a100:1
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-gpu=64G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
