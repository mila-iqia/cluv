#!/bin/bash
#SBATCH --job-name=cluv-test
#SBATCH --output=slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --time=0:05:00

# Minimal test job for cluv submit integration tests.
echo "hostname: $(hostname)"
echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"
# echo "Job has been restarted $SLURM_RESTART_COUNT times."

project_root="$HOME/repos/cluv"

# Setup the repo in $SLURM_TMPDIR, so the code can change in the project without affecting the job.
srun --ntasks-per-node=1 --ntasks=$SLURM_NNODES --input=all bash -e <<END
git clone $HOME/repos/cluv $SLURM_TMPDIR/cluv
git -c $SLURM_TMPDIR/cluv checkout --detach $GIT_COMMIT
exec uv sync --directory=$SLURM_TMPDIR/cluv
END

# Run the actual job command passed as an argument ('python main.py' for example)
srun uv --dir=$SLURM_TMPDIR/cluv run "$@"

# Copy results (if any) from the local storage back to the results dir (eg in $SCRATCH)
srun --ntasks-per-node=1 rsync --update --recursive $SLURM_TMPDIR/cluv/results $project_root/results