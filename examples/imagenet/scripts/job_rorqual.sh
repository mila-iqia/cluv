#!/bin/bash
# Rorqual: the 4 H100 GPUs of a node (64 cores / 512GB of RAM per node).
# Note: compute nodes on Rorqual don't have internet access, so `[tool.cluv.env] UV_OFFLINE = "1"`
# applies here and the virtualenv has to have been created by `cluv sync` on the login node first.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-task=h100:1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-gpu=64G

exec bash "$SLURM_SUBMIT_DIR/scripts/train.sh" "$@"
