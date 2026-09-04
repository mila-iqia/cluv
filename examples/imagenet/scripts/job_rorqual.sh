#!/bin/bash
# Rorqual: 1 of the 4 H100 GPUs of a node (64 cores / 512GB of RAM per node), so 1/4 of its cores.
# Note: compute nodes on Rorqual don't have internet access, so the virtualenv has to have been
# created by `cluv sync` on the login node first.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=h100:1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-gpu=64G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
