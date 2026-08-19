#!/usr/bin/env bash
# Uploads the offline W&B runs that `cluv sync` pulled back from the clusters (WANDB_MODE=offline
# there, see the [tool.cluv.env] section of pyproject.toml) to wandb.ai.
#
# Safe to run as often as you like: `wandb sync` marks each run as synced after a successful
# upload and skips already-synced runs on later calls (unless given --include-synced), so this
# never re-uploads the same run twice and won't flood wandb.ai even with a large backlog of old
# local runs.
#
# Usage:
#   cluv sync <cluster>          # pull the run(s) back from the cluster first
#   scripts/sync_wandb.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# `results_path` resolved for this (local) machine - see cluv.job.get_results_path(). Every run
# directory that used online or offline W&B logging has a `wandb/` subdirectory (main.py points
# wandb's `dir` at the run's results dir); runs started with `--no_wandb` don't.
results_path=$(uv run python -c "from cluv.job import get_results_path; print(get_results_path())")

shopt -s nullglob
wandb_dirs=("$results_path"/*/wandb)
if [ ${#wandb_dirs[@]} -eq 0 ]; then
    echo "No wandb run directories found under $results_path (run 'cluv sync <cluster>' first)."
    exit 0
fi
uv run wandb sync --include-offline "${wandb_dirs[@]}"
