#!/bin/bash
# Nibi: 1 of the 8 H100 GPUs of a node (112 cores / 2TB of RAM per node), so 1/8 of its cores.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=h100:1
#SBATCH --cpus-per-task=14
#SBATCH --mem-per-gpu=64G

# Specific to Nibi: Apparently the `MASTER_ADDR` has to be set to this specific pattern.
export MASTER_ADDR="ic-${SLURMD_NODENAME}"

exec bash scripts/train.sh "$@"
