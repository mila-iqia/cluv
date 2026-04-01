#!/bin/bash
#SBATCH --job-name=cluv-test
#SBATCH --output=logs/slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --time=0:05:00

project_root="$HOME/repos/cluv"

# Minimal test job for cluv submit integration tests.
echo "hostname: $(hostname)"
echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"
# echo "Job has been restarted $SLURM_RESTART_COUNT times."

# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
srun --ntasks-per-node=1 --ntasks=$SLURM_NNODES --input=all bash -e <<END
cd $SLURM_TMPDIR
git clone $project_root
git checkout --detach $GIT_COMMIT
cd $SLURM_TMPDIR/cluv
exec uv sync
END

# Run the actual job command passed as an argument ('python main.py' for example)
srun uv --directory=$SLURM_TMPDIR/cluv run "$@"

# IDEA: Display a warning if there are files in $SLURM_TMPDIR that would be lost.

# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
srun --ntasks-per-node=1 rsync --update --recursive $SLURM_TMPDIR/cluv/logs $project_root/logs
