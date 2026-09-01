#!/bin/bash
# Nibi: 4 of the 8 H100 GPUs of a node (112 cores / 2TB of RAM per node).
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-node=h100:4
#SBATCH --cpus-per-task=14
#SBATCH --mem-per-gpu=64G

# Specific to Nibi: Apparently the `MASTER_ADDR` has to be set to this specific pattern.
export MASTER_ADDR="ic-${SLURMD_NODENAME}"

# Nibi's compute nodes have internet access, unlike most other clusters here.
export UV_OFFLINE=0
export WANDB_MODE=online

exec bash scripts/train.sh "$@"
