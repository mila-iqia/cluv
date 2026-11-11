#!/bin/bash
# Killarney: 1 of the 4 L40S GPUs of a node (64 cores / 515GB of RAM per node), so 1/4 of its cores.
# Killarney also has 8x H100 nodes; ask for `--gpus-per-node=h100:8` with 8 tasks (and more `--mem`)
# to use one.
# NOTE: Killarney's job submit plugin adds a default `--mem` (8G) even when `--mem-per-gpu` is passed,
# which leaves both `SLURM_MEM_PER_NODE` and `SLURM_MEM_PER_GPU` set in the job, and every `srun`
# inside it then fails with "SLURM_MEM_PER_CPU, SLURM_MEM_PER_GPU, and SLURM_MEM_PER_NODE are
# mutually exclusive". Asking for per-node memory with `--mem` instead avoids this.
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=l40s:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
