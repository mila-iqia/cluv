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
# $HOME/.cache/uv - every `uv` invocation below would otherwise fail immediately with "Permission
# denied" trying to create it. Compute nodes also have no internet access, so (unlike a node-local
# $SLURM_TMPDIR cache, which would start cold every job) the cache has to be somewhere persistent
# and reachable from the login node too, so `cluv sync`'s own `uv sync` can actually warm it before
# the job runs (see `run_uv_sync` in cluv/cli/sync.py) - $SCRATCH fits.
#
# This mirrors `[tool.cluv.clusters.trillium-gpu].env`'s UV_CACHE_DIR, but has to be repeated here
# as a plain `export`: that config value only reaches the job as a literal, already-expanded-or-not
# string (see the README's "$SCRATCH expansion" note on why cluv can't expand `$SCRATCH` in it for
# the job's own environment), whereas a script that already runs on the compute node can expand it
# itself correctly.
export UV_CACHE_DIR="$SCRATCH/.cache/uv"

# Trillium-gpu's compute nodes have no internet access.
export UV_OFFLINE=1
export WANDB_MODE=offline

# `cluv submit` runs `sbatch --chdir=<project dir>`, so the job starts in this project's
# folder on the cluster, and the rest of the work is shared with the other clusters:
exec bash scripts/train.sh "$@"
