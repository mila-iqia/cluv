#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=4G
#SBATCH --output=logs/%j/slurm-%j.out

project_name="imagenet"  # to be replaced with the user's project name.
project_root="$HOME/repos/cluv/examples/$project_name" # to be replaced with the path to the user's project in their $HOME.
results_dir="logs" # to be replaced with the path to the results dir name. (--output flag above too)


echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"
# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
project_root_in_tmpdir="$SLURM_TMPDIR/$project_name"
echo "Cloning the project and setting up the virtual environment in $project_root_in_tmpdir"

srun --ntasks-per-node=1 --ntasks=$SLURM_JOB_NUM_NODES bash -e <<END
    cd $SLURM_TMPDIR
    echo "Cloning the project from $project_root to $SLURM_TMPDIR"
    set -x  # show commands as they are executed (for debugging).

    git clone $project_root  # clone the project from $HOME to $SLURM_TMPDIR
    cd $SLURM_TMPDIR/$project_name
    git checkout --detach $GIT_COMMIT
    # Copy the virtualenv (seems necessary for some clusters in offline mode).
    cp -r $project_root/.venv $SLURM_TMPDIR/$project_name/.venv
    uv sync

    # Copy any existing results from $SCRATCH to the project root.
    mkdir -p $project_root_in_tmpdir/$results_dir
    if [ -d "$project_root/$results_dir/$SLURM_JOB_ID" ]; then
        rsync --update --recursive $project_root/$results_dir/$SLURM_JOB_ID $project_root_in_tmpdir/$results_dir/
    fi
END


# These environment variables are used by torch.distributed and should ideally be set
# before running the python script, or at the very beginning of the python script.
# Master address is the hostname of the first node in the job.
export MASTER_ADDR=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
# Get a unique port for this job based on the job ID
export MASTER_PORT=$(expr 10000 + $(echo -n $SLURM_JOB_ID | tail -c 4))
export WORLD_SIZE=$SLURM_NTASKS

## Pure Slurm version ##
# They can either be set here or as early as possible in the Python script.
# Use `uv run --offline` on clusters without internet access on compute nodes.
# Using `srun` executes the command once per task, once per GPU in our case.
# --gres-flags=allow-task-sharing is required to allow tasks on the same node to
# access GPUs allocated to other tasks on that node. Without this flag,
# --gpus-per-task=1 would isolate each task to only see its own GPU, which
# causes a a mysterious NCCL error in
# nn.parallel.DistributedDataParallel:
# ncclUnhandledCudaError: Call to CUDA function failed.
# when NCCL tries to communicate to local GPUs via shared memory but fails due
# to cgroups isolation. See https://slurm.schedmd.com/srun.html#OPT_gres-flags
# and https://support.schedmd.com/show_bug.cgi?id=17875 for details.
# Run the actual job command passed as an argument ('python main.py' for example)
echo "Running command: 'uv run $@' in $project_root_in_tmpdir"
srun --gres-flags=allow-task-sharing bash -c \
    "RANK=\$SLURM_PROCID LOCAL_RANK=\$SLURM_LOCALID \
    uv run --directory=$project_root_in_tmpdir $@"


# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
echo "Copying logs from $project_root_in_tmpdir/$results_dir to $project_root/$results_dir"
if [ -d "$project_root_in_tmpdir/$results_dir/$SLURM_JOB_ID" ]; then
    srun --ntasks-per-node=1 --ntasks=$SLURM_JOB_NUM_NODES \
        rsync --update --recursive $project_root_in_tmpdir/$results_dir/$SLURM_JOB_ID $project_root/$results_dir/
fi
