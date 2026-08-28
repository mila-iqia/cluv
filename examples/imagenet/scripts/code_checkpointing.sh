#!/bin/bash
## Code checkpointing utility script ##
# Adapted from the mila-docs ImageNet example this example is based on:
# https://github.com/mila-iqia/mila-docs/tree/master/docs/examples/advanced/imagenet
#
# This stops changes made to the project between job submission and job start from affecting the
# job. The project folder on the cluster is what `cluv sync` writes into, so a later `cluv sync` (or
# another `cluv submit`) can move it to a different commit while this job is still queued. Without
# this, the job would train whatever happens to be checked out by then instead of what it was
# submitted with - and `requeue = true` in this example's config widens that window, since a
# requeued job re-runs the job script.
#
# Usage:
# - Call this from a Slurm job script: `UV_DIR=$(bash scripts/code_checkpointing.sh)`
# - It clones the project onto each node's local storage at $GIT_COMMIT and creates the virtualenv
#   there, then prints the directory to pass to `--directory` of the `uv run` calls that follow.
#   (Progress messages go to stderr so that stdout is only that directory.)
#
# Assumptions:
# - `cluv submit` exports $GIT_COMMIT, and refuses to submit with a dirty tree. That is the role
#   upstream's `safe_sbatch` plays.
# - The project uses Git and uv (https://docs.astral.sh/uv).

set -e # exit on error.

# We need to know where to go after cloning the repo into $SLURM_TMPDIR.
project_root=$(git rev-parse --show-toplevel)
project_dirname=$(basename "$project_root")
# NOTE: upstream uses `${SLURM_SUBMIT_DIR:-$(pwd)}` here. That would be wrong under `cluv submit`,
# which runs `sbatch --chdir=<project_dir>` from the home directory of its SSH session - so
# $SLURM_SUBMIT_DIR is that home directory, not this project. The `--chdir` is what makes $(pwd)
# the project folder.
submit_dir_relative_to_parent=$(realpath --relative-to="$(dirname "$project_root")" "$(pwd)")

# The directory in which `uv` commands should run.
# - Without code checkpointing, that is just the current directory.
# - With it, the path from the parent of the project root down to this project is recreated inside
#   $SLURM_TMPDIR, and uv is pointed there instead.
UV_DIR="."
if [[ -n "$GIT_COMMIT" ]]; then
    echo "Job will run with code from commit $GIT_COMMIT" >&2
    # IMPORTANT: $SLURM_TMPDIR is deliberately left *unexpanded* here. It is node-local and can
    # differ between the nodes of one job, so it has to be expanded inside each task instead of once
    # here on the first node. Every `uv run --directory=$UV_DIR` that follows therefore has to go
    # through a `bash -c "..."`, so that each task expands it itself.
    UV_DIR="\$SLURM_TMPDIR/$submit_dir_relative_to_parent"
    # $SLURM_TMPDIR is empty at job start, so there is nothing to clean up first.
    # `uv sync` needs either internet access on the compute nodes or a warm uv cache / the DRAC
    # wheelhouse. The `cluv sync` that precedes every `cluv submit` already warms that cache.
    # `--ntasks` has to be capped as well, not just `--ntasks-per-node`. Upstream's job script asks
    # for a single task in total, so `--ntasks-per-node=1` alone is enough there. The job scripts in
    # this example ask for one task per GPU, and Slurm then refuses to narrow the step:
    #   srun: warning: can't honor --ntasks-per-node set to 1 which doesn't match the requested
    #   tasks 4 with the maximum number of requested nodes 1. Ignoring --ntasks-per-node.
    # The clone would then run once per GPU, concurrently, into the same directory.
    srun --ntasks-per-node=1 --ntasks=${SLURM_JOB_NUM_NODES:-1} bash -c "\
        git clone --quiet $project_root \$SLURM_TMPDIR/$project_dirname && \
        cd \$SLURM_TMPDIR/$project_dirname && \
        git checkout --quiet --detach $GIT_COMMIT && \
        uv sync --directory=$UV_DIR"
elif [[ -n "$(git -C "$project_root" status --porcelain)" ]]; then
    echo "Warning: GIT_COMMIT is not set and the repo at $project_root has uncommitted changes." >&2
    echo "This may cause future jobs to fail or produce inconsistent results!" >&2
    echo "Submit with 'cluv submit' instead, which sets \$GIT_COMMIT for you." >&2
else
    echo "GIT_COMMIT is not set, but the repo state is clean." >&2
    echo "This job runs straight out of the project folder, so if you modify the files there it" >&2
    echo "might fail or produce inconsistent results." >&2
fi
# Return UV_DIR as this script's output.
echo "$UV_DIR"
