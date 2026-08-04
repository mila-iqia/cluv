#!/bin/bash
# Fir: the 4 H100 GPUs of a node (48 cores / 1.1TB of RAM per node).
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-task=h100:1
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-gpu=64G

exec bash "$SLURM_SUBMIT_DIR/scripts/train.sh" "$@"
