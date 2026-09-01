#!/bin/bash
# Mila cluster: 2 of the 4 L40S GPUs of a cn-l node (48 cores / 1TB of RAM per node).
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2
#SBATCH --gpus-per-node=l40s:2
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-gpu=64G
# We need ~200GB of storage on the local disk of each node to extract ImageNet into.
# `--tmp` is honoured on the Mila cluster, but it might not be on other clusters.
#SBATCH --tmp=200G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
