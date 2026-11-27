#!/bin/bash
# Shared body of the job scripts created by `cluv init`. `job.sh` and the `job_<cluster>.sh`
# scripts only hold the `#SBATCH` resources of their clusters, then run this script.
#
# It runs the command passed after the `--` of `cluv submit` with `uv run`, once per Slurm task:
#   cluv submit <cluster> -- python main.py --lr 0.01
#
# Code checkpointing: the project folder on the cluster follows every `cluv sync`, so a job that
# waits in the queue could otherwise start with newer code than it was submitted with. `cluv submit`
# exports $GIT_COMMIT, the commit it synced to the cluster, and this script clones the project at
# that commit on the local disk of each node ($SLURM_TMPDIR) to run the command from there.
set -euo pipefail

if [ $# -eq 0 ]; then
    echo "ERROR: no command to run. Pass it after the '--' of 'cluv submit', for example:" >&2
    echo "  cluv submit <cluster> -- python main.py" >&2
    exit 1
fi

echo "Date:       $(date)"
echo "Cluster:    ${CLUV_CLUSTER:-?}"
echo "Nodes:      $SLURM_JOB_NODELIST ($SLURM_NTASKS tasks)"
echo "Git commit: ${GIT_COMMIT:-<not set>}"
echo "Command:    uv run $*"

# `cluv submit` runs `sbatch --chdir=<project_dir>`, so the job starts in the project folder.
project_dir=$(pwd)
if [ -n "${GIT_COMMIT:-}" ]; then
    # The project can be a subfolder of the git repo (a uv workspace member for example).
    repo_root=$(git rev-parse --show-toplevel)
    project_in_repo=$(realpath --relative-to="$repo_root" "$project_dir")
    # $SLURM_TMPDIR is left unexpanded: it is node-local, so each node expands its own.
    run_dir="\$SLURM_TMPDIR/$(basename "$repo_root")/$project_in_repo"
    echo "Cloning the project at commit $GIT_COMMIT into $run_dir on each node"
    # Once per node, not once per task. The uv cache is usually on another filesystem than
    # $SLURM_TMPDIR, so uv can't hardlink from it and copies the packages instead.
    srun --ntasks-per-node=1 --ntasks="$SLURM_JOB_NUM_NODES" bash -ec "
        git clone --quiet '$repo_root' \$SLURM_TMPDIR/$(basename "$repo_root")
        git -C $run_dir checkout --quiet --detach $GIT_COMMIT
        UV_LINK_MODE=copy uv sync --directory=$run_dir"
else
    echo "WARNING: \$GIT_COMMIT is not set, so the job runs from $project_dir, whose code can" >&2
    echo "change while the job is queued. Submit with 'cluv submit' to set it." >&2
    run_dir="$project_dir"
fi

# One `uv run` per Slurm task. Each task expands $SLURM_TMPDIR in $run_dir itself, and the
# command's arguments are passed as "$@" to keep their quoting.
srun bash -c "uv run --directory=$run_dir \"\$@\"" _ "$@"
