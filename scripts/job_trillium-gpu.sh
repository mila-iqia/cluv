#!/bin/bash
# Trillium's GPU nodes (4 H100 and 96 cores per node) come with a fixed share of memory per GPU:
# `--mem` is rejected, and jobs must last at least 15 minutes.
# https://docs.alliancecan.ca/wiki/Trillium
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=h100:4
#SBATCH --cpus-per-task=96
#SBATCH --time=0:15:00

# $HOME is read-only on the compute nodes, so uv's cache has to be on $SCRATCH. This repeats the
# `UV_CACHE_DIR` of the cluv config, which `cluv sync` uses to fill that cache from the login node.
export UV_CACHE_DIR="$SCRATCH/.cache/uv"

# Clone the project at the submitted commit and run the command (see scripts/run.sh).
exec bash scripts/run.sh "$@"
