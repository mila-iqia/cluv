#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=0:05:00
#SBATCH --output=logs/%j/slurm-%j.out

project_name="cluv"  # to be replaced with the user's project name.
project_root="$HOME/repos/$project_name" # to be replaced with the path to the user's project in their $HOME.

echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"
# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
project_root_in_tmpdir="$SLURM_TMPDIR/$project_name"
echo "Cloning the project and setting up the virtual environment in $project_root_in_tmpdir"

srun --ntasks-per-node=1 --ntasks=$SLURM_NNODES bash -e <<END
    cd $SLURM_TMPDIR
    git clone $project_root  # clone the project from $HOME to $SLURM_TMPDIR
    cd $SLURM_TMPDIR/$project_name
    git checkout --detach $GIT_COMMIT
    # Copy the virtualenv (seems necessary for some clusters in offline mode).
    cp -r $project_root/.venv $SLURM_TMPDIR/$project_name/.venv
    exec uv sync
END


# Run the actual job command passed as an argument ('python main.py' for example)
echo "Running command: 'uv run $@' in $project_root_in_tmpdir"
srun uv --directory=$project_root_in_tmpdir run "$@"

# IDEA: Display a warning if there are files in $SLURM_TMPDIR that would be lost.

# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
echo "Copying logs from $project_root_in_tmpdir/logs to $project_root/logs"
srun --ntasks-per-node=1 rsync --update --recursive $project_root_in_tmpdir/logs $project_root/logs
