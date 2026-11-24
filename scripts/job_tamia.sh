#!/bin/bash
# TamIA allocates whole GPU nodes: a job has to use all the GPUs of its nodes (4 H100, 48 cores and
# 500GB of RAM per node). `--mem=0` asks for all the memory of the node.
# https://docs.alliancecan.ca/wiki/TamIA
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=4
#SBATCH --cpus-per-task=48
#SBATCH --mem=0
#SBATCH --time=0:15:00

# Clone the project at the submitted commit and run the command (see scripts/run.sh).
exec bash scripts/run.sh "$@"
