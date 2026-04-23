#!/bin/bash
#SBATCH --output=/home/mila/v/vandenbh/Desktop/cluv/examples/simple-checkpoint/logs/%j/slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --mem=8G
#SBATCH --time=0:05:00

project_name="cluv"
results_path="logs"
git_root="$HOME/Desktop/cluv"
example_path="examples/simple-checkpoint"

# Minimal test job for cluv submit.
echo "hostname: $(hostname)"
echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"

# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
echo "Preparing the repo and virtual environment in $SLURM_TMPDIR"
srun --ntasks-per-node=1 --ntasks=$SLURM_NNODES bash -e <<END
cd $SLURM_TMPDIR
git clone $git_root
cd $SLURM_TMPDIR/$project_name
git checkout --detach $GIT_COMMIT
cd $example_path
exec uv sync
END

# Try to copy the results of a previous run of the job if they exist
if [ -d "$git_root/$example_path/$results_path/$SLURM_JOB_ID/" ]; then
    echo "Copying previous results from $git_root/$example_path/$results_path/$SLURM_JOB_ID/ to $SLURM_TMPDIR/$project_name/$example_path/$results_path"
    srun --ntasks-per-node=1 rsync --update --recursive --mkpath $git_root/$example_path/$results_path/$SLURM_JOB_ID/ $SLURM_TMPDIR/$project_name/$example_path/$results_path
else
    echo "No previous results found at $git_root/$example_path/$results_path/$SLURM_JOB_ID/."
fi


# echo "Copying previous results from $git_root/$example_path/$results_path/$SLURM_JOB_ID/ to $SLURM_TMPDIR/$project_name/$example_path/$results_path"
# srun --ntasks-per-node=1 rsync --update --recursive --mkpath $git_root/$example_path/$results_path/$SLURM_JOB_ID/ $SLURM_TMPDIR/$project_name/$example_path/$results_path

# Run the actual job command passed as an argument ('python main.py' for example)
echo "Running command: $@"
# Note: This `--gres-flags=allow-task-sharing` is required to allow tasks on the same node to access
# GPUs allocated to other tasks on that node. Without this flag, --gpus-per-task=1 would isolate
# each task to only see its own GPU, which can cause some mysterious NCCL errors.
project_example="$SLURM_TMPDIR/$project_name/$example_path"
srun --gres-flags=allow-task-sharing uv --directory=$project_example run "$@"

# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
echo "Copying logs from $SLURM_TMPDIR/$project_name/$example_path/$results_path/ to $git_root/$example_path/$results_path/$SLURM_JOB_ID/"
srun --ntasks-per-node=1 rsync --update --recursive --mkpath $SLURM_TMPDIR/$project_name/$example_path/$results_path/ $git_root/$example_path/$results_path/$SLURM_JOB_ID/
