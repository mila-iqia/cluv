#!/bin/bash
# Shared body of every job script in this example.
#
# The per-cluster `scripts/job_<cluster>.sh` wrappers only contain #SBATCH directives, then
# `exec` this script. Which wrapper is used for which cluster is set with `job_script_path` in the
# `[tool.cluv.clusters.<cluster>]` sections of the pyproject.toml.
#
# `cluv submit` runs `sbatch --chdir=<project_dir>`, so the job (and therefore this script) starts in
# this project's folder on the cluster, and `uv run` picks up the right environment without any
# `--directory` flag.
# (Note: $SLURM_SUBMIT_DIR is *not* that folder - it is the directory sbatch itself was run from,
# which for `cluv submit` is the home directory of the SSH session.)
#
# It then switches into a clone of the repo in each node's $SLURM_TMPDIR, checked out at the
# $GIT_COMMIT the job was submitted with, so the code cannot change under a job that is still queued.
# Everything after that point runs from the clone. See the "Run the code from a per-node clone"
# section below.

set -e # exit on error.

# The command to run, as passed after the `--` of `cluv submit`.
job_command=("$@")
if [ ${#job_command[@]} -eq 0 ]; then
    job_command=(python main.py)
fi

echo "Date:       $(date)"
echo "Hostname:   $(hostname)"
echo "Cluster:    ${CC_CLUSTER:-mila}"
echo "Nodes:      ${SLURM_JOB_NODELIST:-?} (${SLURM_NTASKS:-?} tasks)"
echo "Attempt #${SLURM_RESTART_COUNT:-0}"
# `cluv submit` refuses to submit with a dirty tree and exports the commit it synced to the cluster.
echo "Git commit: ${GIT_COMMIT:-<not set - submit this job with 'cluv submit'>}"
echo "Command:    uv run ${job_command[*]}"

# `srun` refuses to launch a step when more than one of these is set:
#   srun: fatal: SLURM_MEM_PER_CPU, SLURM_MEM_PER_GPU, and SLURM_MEM_PER_NODE are mutually exclusive
# That happens on clusters where a site-wide default memory setting ends up in the job environment
# alongside the one this job actually asked for. Keep the most specific one.
if [ -n "${SLURM_MEM_PER_GPU:-}" ]; then
    unset SLURM_MEM_PER_CPU SLURM_MEM_PER_NODE
elif [ -n "${SLURM_MEM_PER_CPU:-}" ]; then
    unset SLURM_MEM_PER_NODE
fi

# Run this once per node. $SLURM_TMPDIR is set by a Slurm plugin at step launch, so it has to be
# read inside an `srun` step rather than here in the batch script.
one_task_per_node="srun --ntasks-per-node=1 --ntasks=${SLURM_JOB_NUM_NODES:-1}"

# The batch script needs the value too (to `cd` into the clone below, so that the steps launched
# afterwards inherit it as their working directory), so ask a step for it when it isn't already set
# here. The path is the same on every node of a job on all the clusters this example targets.
slurm_tmpdir=${SLURM_TMPDIR:-$(srun --ntasks=1 --ntasks-per-node=1 bash -c 'echo "$SLURM_TMPDIR"')}

## Run the code from a per-node clone of the repo, pinned to $GIT_COMMIT ##
# The project folder on the cluster is what `cluv sync` writes into, so a later `cluv sync` (or
# another `cluv submit`) can move it to a different commit while this job is still sitting in the
# queue. The job would then train whatever happens to be checked out at that moment instead of what
# it was submitted with - and with `requeue = true` in the config, a requeued job re-runs this
# script, which widens that window further.
#
# `cluv submit` refuses to submit a dirty tree and exports the commit it synced as $GIT_COMMIT, so
# cloning the repo onto each node and detaching onto that commit makes the code this job runs
# immutable, whatever happens in the project folder afterwards.
#
# The virtualenv is *symlinked*, not copied: it is ~7GB (torch plus the CUDA libraries), it already
# matches this commit's uv.lock because `cluv submit` synced it, and UV_NO_SYNC below stops uv from
# ever modifying it. The trade-off is that third-party packages -- and `cluv` itself, which is
# installed in the venv as an editable workspace member -- are still read from the project folder,
# so only this example's own code is pinned. Replace the symlink with
# `cp -r "$repo_root/.venv" "$clone/.venv"` followed by `uv sync` if you need the dependencies
# pinned too, keeping in mind that this copies ~7GB onto every node.
#
# Note: this does not change where Slurm writes the job's own output file. `--output` is resolved
# relative to the `--chdir` that `cluv submit` passes (the project folder), and Slurm opens that
# file before this script starts running.
if [ -n "${GIT_COMMIT:-}" ]; then
    repo_root=$(git rev-parse --show-toplevel)
    # Where this project sits inside the repo, e.g. `examples/imagenet`.
    project_subdir=$(realpath --relative-to="$repo_root" "$PWD")
    repo_name=$(basename "$repo_root")

    if [ -z "$slurm_tmpdir" ]; then
        echo "ERROR: could not determine \$SLURM_TMPDIR for this job, so there is nowhere" >&2
        echo "node-local to clone the repo into. Request local disk for the job (for example with" >&2
        echo "--tmp), or drop this section to run straight out of the project folder." >&2
        exit 1
    fi

    # $SLURM_TMPDIR is node-local and private to this job, so each node needs its own clone.
    echo "Cloning the repo at $GIT_COMMIT into $slurm_tmpdir on each node..."
    $one_task_per_node bash -c '
        set -e
        repo_root="$1"
        git_commit="$2"
        clone="$3/$(basename "$repo_root")"
        # A requeued job keeps the same $SLURM_JOB_ID, so a clone from a previous attempt can still
        # be there if this attempt landed on the same node.
        rm -rf "$clone"
        git clone --quiet "$repo_root" "$clone"
        if ! git -C "$clone" checkout --quiet --detach "$git_commit"; then
            echo "ERROR: commit $git_commit is not present in the clone of $repo_root." >&2
            echo "The project folder was most likely moved to an unrelated commit while this job" >&2
            echo "was queued. Re-submit the job with cluv submit." >&2
            exit 1
        fi
        ln -s "$repo_root/.venv" "$clone/.venv"
    ' _ "$repo_root" "$GIT_COMMIT" "$slurm_tmpdir"

    cd "$slurm_tmpdir/$repo_name/$project_subdir"
    echo "Running from:  $PWD (pinned to $GIT_COMMIT)"
    # The virtualenv is shared with the project folder, so uv must never modify it.
    export UV_NO_SYNC=1
else
    echo "WARNING: \$GIT_COMMIT is not set, so this job runs straight out of $PWD, and the code" >&2
    echo "can still change under it while it is queued. Submit with \`cluv submit\` instead." >&2
fi

## Warm up the virtualenv on each node ##
# `import torch` pulls in ~2GB of shared libraries. Faulting those in from a networked filesystem
# ($HOME is on Lustre on the DRAC clusters) in all the tasks of a node at once is *very* slow - the
# tasks sit in `cl_sync_io_wait` for minutes. Reading them once per node first puts them in the
# node's page cache, so the tasks below start quickly.
echo "Warming up the virtualenv on each node..."
$one_task_per_node uv run python -c 'import torch, torchvision; print(torch.__version__, flush=True)'

## Stage the dataset into $SLURM_TMPDIR ##
# Extract the ImageNet archives onto each node's local disk, using all the CPUs of each node.
# `prepare_data.py` reads the archives from the `datasets_path` that cluv resolved for this cluster,
# and extracts them where `main.py` looks for them by default ($SLURM_TMPDIR/data).
# With --use_fake_data there is no dataset to stage, which is handy for a quick smoke test on a
# cluster that doesn't have a copy of ImageNet.
if [[ "${job_command[*]}" == *--use_fake_data* ]]; then
    echo "Skipping the dataset staging step, since --use_fake_data was passed."
else
    $one_task_per_node bash -c '
        echo "SLURM_TMPDIR: ${SLURM_TMPDIR:-<unset>}"
        if [ -z "${SLURM_TMPDIR:-}" ]; then
            echo "ERROR: \$SLURM_TMPDIR is not set in this job, so we do not know where this" >&2
            echo "cluster wants ~150GB of node-local scratch to be written. Refusing to guess." >&2
            echo "Request local disk for the job (for example with --tmp), or pass" >&2
            echo "--use_fake_data to run without the real dataset." >&2
            exit 1
        fi
        uv run python prepare_data.py'
fi

# These environment variables are used by torch.distributed and should ideally be set
# before running the python script, or at the very beginning of the python script.
# (Some modules might inadvertently initialize cuda when imported, which is a problem.)
# Master address is the hostname of the first node in the job.
export MASTER_ADDR=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
# Get a unique port for this job based on the job ID
export MASTER_PORT=$(expr 10000 + $(echo -n $SLURM_JOB_ID | tail -c 4))
export WORLD_SIZE=$SLURM_NTASKS
# main.py derives RANK / LOCAL_RANK from $SLURM_PROCID / $SLURM_LOCALID, which differ per task, so
# they must not be set here.

## Pure Slurm version ##
# `srun` runs the command once per task, which is once per GPU in our case.
#
# Note: the job scripts request GPUs with `--gpus-per-node`, not `--gpus-per-task`. With
# `--gpus-per-task=1`, Slurm's cgroups give each task only *its own* GPU, so every task sees a
# single device and `torch.cuda.set_device(LOCAL_RANK)` fails with "invalid device ordinal" in every
# task but the first. (It also breaks NCCL's shared-memory path between local GPUs, giving a
# mysterious `ncclUnhandledCudaError: Call to CUDA function failed`.) `--gres-flags=allow-task-sharing`
# is supposed to lift that isolation, but it is not honoured everywhere, so it's simpler to allocate
# the GPUs per node and let each task pick its own by local rank.
#
# A per-cluster wrapper can `export SRUN_EXTRA_ARGS=...` to add flags here.
srun ${SRUN_EXTRA_ARGS-} uv run "${job_command[@]}"

## srun + torchrun version ##
# srun --ntasks-per-node=1 bash -c "\
#     uv run torchrun --node-rank=\$SLURM_NODEID --nnodes=\$SLURM_STEP_NUM_NODES \
#     --master-addr=$MASTER_ADDR --master-port=$MASTER_PORT --nproc-per-node=gpu \
#     ${job_command[*]}"

## srun + accelerate version ##
## NOTE: This particular example doesn't use accelerate, this is just here to illustrate.
# srun --ntasks-per-node=1 bash -c "\
#     uv run accelerate launch \
#     --machine_rank \$SLURM_NODEID \
#     --main_process_ip $MASTER_ADDR --main_process_port $MASTER_PORT \
#     --num_machines $SLURM_NNODES --num_processes $SLURM_NTASKS \
#     ${job_command[*]}"
