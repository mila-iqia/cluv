#!/bin/bash
# Shared body of every job script in this example.
#
# The per-cluster `scripts/job_<cluster>.sh` wrappers only contain #SBATCH directives, then
# `exec` this script. Which wrapper is used for which cluster is set with `job_script_path` in the
# `[tool.cluv.clusters.<cluster>]` sections of the pyproject.toml.
#
# `cluv submit` runs `sbatch --chdir=<project_dir>`, so the job (and therefore this script) starts in
# this project's folder on the cluster.
# (Note: $SLURM_SUBMIT_DIR is *not* that folder - it is the directory sbatch itself was run from,
# which for `cluv submit` is the home directory of the SSH session.)
#
# `scripts/code_checkpointing.sh` then clones the project onto each node's local storage at the
# commit the job was submitted with, and every `uv run` below is pointed there with `--directory`.

set -e # exit on error.

# The command to run, as passed after the `--` of `cluv submit`.
job_command=("$@")
if [ ${#job_command[@]} -eq 0 ]; then
    echo "ERROR: no command to run was passed to this job script. Pass it after the '--' of" >&2
    echo "'cluv submit', for example:" >&2
    echo "  cluv submit <cluster> -- python main.py --use_fake_data" >&2
    exit 1
fi

echo "Date:       $(date)"
echo "Hostname:   $(hostname)"
echo "Cluster:    ${CC_CLUSTER:-mila}"
echo "Nodes:      ${SLURM_JOB_NODELIST:-?} (${SLURM_NTASKS:-?} tasks)"
echo "Attempt #${SLURM_RESTART_COUNT:-0}"
# `cluv submit` refuses to submit with a dirty tree and exports the commit it synced to the cluster.
echo "Git commit: ${GIT_COMMIT:-<not set - submit this job with 'cluv submit'>}"
echo "Command:    uv run ${job_command[*]}"

## Code checkpointing with git, to avoid unexpected bugs ##
# Clones the project at $GIT_COMMIT onto every node and creates the virtualenv there. What comes back
# is the directory to give to `uv run --directory`, and it still contains a literal, unexpanded
# `$SLURM_TMPDIR`: that path is node-local and can differ between the nodes of one job, so each task
# has to expand it itself. That is why every `uv run` below goes through a `bash -c "..."`.
UV_DIR=$(bash scripts/code_checkpointing.sh)
echo "Running uv commands in directory: $UV_DIR"

## Stage the dataset into $SLURM_TMPDIR ##
# Extract the ImageNet archives onto each node's local disk, using all the CPUs of each node.
# `prepare_data.py` reads the archives from the `datasets_path` that cluv resolved for this cluster,
# and extracts them where `main.py` looks for them by default ($SLURM_TMPDIR/data).
# With --use_fake_data there is no dataset to stage, which is handy for a quick smoke test on a
# cluster that doesn't have a copy of ImageNet.
if [[ "${job_command[*]}" == *--use_fake_data* ]]; then
    echo "Skipping the dataset staging step, since --use_fake_data was passed."
else
    # One task per node, not one per GPU: see the note in scripts/code_checkpointing.sh about why
    # `--ntasks` has to be capped too.
    srun --ntasks-per-node=1 --ntasks=${SLURM_JOB_NUM_NODES:-1} \
        bash -c "uv run --directory=$UV_DIR python prepare_data.py"
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
# `"\$@"` keeps the job command's own quoting intact, while $UV_DIR is expanded by each task.
srun ${SRUN_EXTRA_ARGS-} bash -c "uv run --directory=$UV_DIR \"\$@\"" _ "${job_command[@]}"
