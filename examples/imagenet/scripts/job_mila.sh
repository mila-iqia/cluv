#!/bin/bash
# Mila cluster: 2 GPUs on a single node.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2
#SBATCH --gpus-per-task=l40s:1
## Or --gpus-per-task=rtx8000:1    to request a different GPU model
## Or --gpus-per-task=1            for any GPU model
#SBATCH --cpus-per-task=6
#SBATCH --mem-per-gpu=32G
# We need ~200GB of storage on the local disk of each node to extract ImageNet into.
# `--tmp` is honoured on the Mila cluster; the DRAC job scripts don't use it.
#SBATCH --tmp=200G

exec bash "$SLURM_SUBMIT_DIR/scripts/train.sh" "$@"
