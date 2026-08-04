#!/bin/bash
# Tamia allocates whole nodes: a job must use all 4 GPUs of every node it is allocated.
# `--mem=0` therefore asks for all the memory of the node, which we get anyway.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-task=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=0

exec bash "$SLURM_SUBMIT_DIR/scripts/train.sh" "$@"
