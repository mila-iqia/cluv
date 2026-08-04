#!/bin/bash
# Shared body of every job script in this example.
#
# The per-cluster `scripts/job_<cluster>.sh` wrappers only contain #SBATCH directives, then
# `exec` this script. Which wrapper is used for which cluster is set with `job_script_path` in the
# `[tool.cluv.clusters.<cluster>]` sections of the pyproject.toml.
#
# `cluv submit` runs `sbatch --chdir=<project_dir>`, so $SLURM_SUBMIT_DIR is this project's folder
# on the cluster and `uv run` picks up the right environment without any `--directory` flag.

set -e # exit on error.
cd "$SLURM_SUBMIT_DIR"

echo "Date:       $(date)"
echo "Hostname:   $(hostname)"
echo "Cluster:    ${CC_CLUSTER:-mila}"
echo "Nodes:      ${SLURM_JOB_NODELIST:-?} (${SLURM_NTASKS:-?} tasks)"
echo "Attempt #${SLURM_RESTART_COUNT:-0}"
# `cluv submit` refuses to submit with a dirty tree and exports the commit it synced to the cluster.
echo "Git commit: ${GIT_COMMIT:-<not set - submit this job with 'cluv submit'>}"

## Stage the dataset into $SLURM_TMPDIR ##
# Extract the ImageNet archives onto each node's local disk, using all the CPUs of each node.
# `prepare_data.py` reads the archives from the `datasets_path` that cluv resolved for this cluster.
# With --use_fake_data there is no dataset to stage, which is handy for a quick smoke test on a
# cluster where `cluv sync` hasn't replicated the (~150GB) archives yet.
if [[ "$*" == *--use_fake_data* ]]; then
    echo "Skipping the dataset staging step, since --use_fake_data was passed."
else
    srun --ntasks-per-node=1 --ntasks="${SLURM_JOB_NUM_NODES:-1}" bash -c \
        'uv run python prepare_data.py --dest $SLURM_TMPDIR/data'
fi

# These environment variables are used by torch.distributed and should ideally be set
# before running the python script, or at the very beginning of the python script.
# Master address is the hostname of the first node in the job.
export MASTER_ADDR=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
# Get a unique port for this job based on the job ID
export MASTER_PORT=$(expr 10000 + $(echo -n $SLURM_JOB_ID | tail -c 4))
export WORLD_SIZE=$SLURM_NTASKS

# srun is always used to launch the tasks.
# Whether there is one 'task' per GPU or one task per node can vary based on your setup.
# In the latter case, you would typically use torchrun or accelerate to launch one process
# per GPU within each task.
# See the commented examples below for different ways to launch the training script.

# Important note: In all cases, some variables (for example RANK, LOCAL_RANK, or machine_rank
# in accelerate) vary between tasks, so we need to escape env variables such as $SLURM_PROCID,
# $SLURM_TMPDIR and $SLURM_NODEID so they are evaluated within each task, not just once here
# on the first node.

## Pure Slurm version ##
# Using `srun` executes the command once per task, once per GPU in our case.
# --gres-flags=allow-task-sharing is required to allow tasks on the same node to
# access GPUs allocated to other tasks on that node. Without this flag,
# --gpus-per-task=1 would isolate each task to only see its own GPU, which
# causes a mysterious NCCL error in nn.parallel.DistributedDataParallel:
# ncclUnhandledCudaError: Call to CUDA function failed.
# when NCCL tries to communicate to local GPUs via shared memory but fails due
# to cgroups isolation. See https://slurm.schedmd.com/srun.html#OPT_gres-flags
# and https://support.schedmd.com/show_bug.cgi?id=17875 for details.
# A per-cluster wrapper can set SRUN_EXTRA_ARGS to change or drop these flags.
srun ${SRUN_EXTRA_ARGS---gres-flags=allow-task-sharing} bash -c \
    "RANK=\$SLURM_PROCID LOCAL_RANK=\$SLURM_LOCALID \
    uv run python main.py --dataset_path=\$SLURM_TMPDIR/data $*"

## srun + torchrun version ##
# srun --ntasks-per-node=1 bash -c "\
#     uv run torchrun --node-rank=\$SLURM_NODEID --nnodes=\$SLURM_STEP_NUM_NODES \
#     --master-addr=$MASTER_ADDR --master-port=$MASTER_PORT --nproc-per-node=gpu \
#     main.py $*"

## srun + accelerate version ##
## NOTE: This particular example doesn't use accelerate, this is just here to illustrate.
# srun --ntasks-per-node=1 bash -c "\
#     uv run accelerate launch \
#     --machine_rank \$SLURM_NODEID \
#     --main_process_ip $MASTER_ADDR --main_process_port $MASTER_PORT \
#     --num_machines $SLURM_NNODES --num_processes $SLURM_NTASKS \
#     main.py $*"
