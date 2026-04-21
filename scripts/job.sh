#!/bin/bash
#SBATCH --job-name=cluv-test
#SBATCH --output=logs/%j/slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --time=0:05:00

project_root="$HOME/repos/cluv"

# Minimal test job for cluv submit integration tests.
echo "hostname: $(hostname)"
echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"
# echo "Job has been restarted $SLURM_RESTART_COUNT times."

# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
echo "Preparing the repo and virtual environment in $SLURM_TMPDIR"
srun --ntasks-per-node=1 --ntasks=$SLURM_NNODES bash -e <<END
cd $SLURM_TMPDIR
git clone $project_root
cd $SLURM_TMPDIR/cluv
git checkout --detach $GIT_COMMIT
exec uv sync
END

# Run the actual job command passed as an argument ('python main.py' for example)
echo "Running command: $@"
srun uv --directory=$SLURM_TMPDIR/cluv run "$@" --output-dir=$SLURM_TMPDIR/logs/$SLURM_JOB_ID

# IDEA: Display a warning if there are files in $SLURM_TMPDIR that would be lost.

# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
echo "Copying logs from $SLURM_TMPDIR/cluv/logs to $project_root/logs"
srun --ntasks-per-node=1 rsync --update --recursive $SLURM_TMPDIR/cluv/logs $project_root/logs
