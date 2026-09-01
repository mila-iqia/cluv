#!/usr/bin/env bash
# Uploads the offline W&B runs that `cluv sync` pulled back from the clusters (WANDB_MODE=offline
# there, see the [tool.cluv.env] section of pyproject.toml) to wandb.ai.
#
# Safe to run as often as you like: `wandb sync` writes a `<run>.wandb.synced` marker file next to
# a run once it's uploaded, and this script skips any run that already has one. So even with a
# large backlog of old local runs, only ones that haven't been uploaded yet get synced - this never
# re-uploads (or floods wandb.ai with) the same run twice.
#
# Usage:
#   cluv sync <cluster>          # pull the run(s) back from the cluster first
#   scripts/sync_wandb.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# `results_path` resolved for this (local) machine - see cluv.job.get_results_path(). Every run
# directory that used offline W&B logging has a `wandb/offline-run-<timestamp>-<id>/` subdirectory
# (main.py points wandb's `dir` at the run's results dir); runs started with `--no_wandb`, or with
# WANDB_MODE=online, don't. `wandb sync` needs that specific offline-run directory (the one that
# directly contains the `.wandb` file) - pointing it at the parent `wandb/` dir instead makes it
# silently skip every run ("Skipping directory: ..."), since it only looks one level deep.
results_path=$(uv run python -c "from cluv.job import get_results_path; print(get_results_path())")

shopt -s nullglob
to_sync=()
for run_dir in "$results_path"/*/wandb/offline-run-*; do
    # `wandb sync <specific-run-dir>` (as opposed to `wandb sync --sync-all`) uploads unconditionally
    # - it doesn't check for a prior `.synced` marker itself - so this script has to skip already-
    # synced runs on its own to stay idempotent and avoid re-uploading (or flooding wandb.ai with)
    # the same runs on every call.
    synced_markers=("$run_dir"/*.wandb.synced)
    if [ ${#synced_markers[@]} -eq 0 ]; then
        to_sync+=("$run_dir")
    fi
done

if [ ${#to_sync[@]} -eq 0 ]; then
    echo "No new offline wandb runs to sync under $results_path."
    exit 0
fi
uv run wandb sync --include-offline "${to_sync[@]}"
