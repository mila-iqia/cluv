#!/bin/bash
# Trillium's GPU nodes: 4 H100 per node (96 cores / 770GB of RAM), allocated whole.
# Note: jobs submitted here report CC_CLUSTER=trillium and Slurm's ClusterName is "grillium", so
# `cluv submit` exports $CLUV_CLUSTER to tell the job it belongs to the `trillium-gpu` config.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-node=h100:4
#SBATCH --cpus-per-task=24
#SBATCH --mem=0

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
