#!/bin/bash
# Trillium's GPU nodes: 4 H100 per node (96 cores / 770GB of RAM), allocated whole.
# Note: jobs submitted here report CC_CLUSTER=trillium and Slurm's ClusterName is "grillium", so
# `cluv submit` exports $CLUV_CLUSTER to tell the job it belongs to the `trillium-gpu` config.
# Trillium rejects `--mem` entirely: "there is always 186 GiB of host memory available per gpu,
# while whole-node jobs get the full memory of the node (745 GiB)".
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --gpus-per-node=h100:4
#SBATCH --cpus-per-task=24

# $HOME isn't writable from Trillium's compute nodes, and uv's default cache lives under
# $HOME/.cache/uv - every `uv` invocation below would otherwise fail immediately with
# "Permission denied" trying to create it. $SLURM_TMPDIR is already set by the time this script
# runs and is always node-local and writable, so point the cache there instead. (This has to be a
# plain `export` in a script that already runs on the compute node, not a `[tool.cluv.env]` entry:
# a value containing a variable like `$SCRATCH` there would need the same shell to both set *and*
# expand it, which cluv's submit command doesn't guarantee - see the README's "$SCRATCH expansion"
# note.)
export UV_CACHE_DIR="$SLURM_TMPDIR/uv-cache"

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
