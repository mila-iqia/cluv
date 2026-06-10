#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=0:05:00
#SBATCH --output=logs/%j/slurm-%j.out

project_name="cluv"
project_root="$HOME/repos/$project_name"

echo "GIT_COMMIT=${GIT_COMMIT:?GIT_COMMIT is not set. Use 'cluv submit' to submit this job script.}"
echo "CONTAINER_PATH=${CONTAINER_PATH:?CONTAINER_PATH is not set. Run 'cluv build' first or set it in your cluv config.}"

if [ ! -f "$CONTAINER_PATH" ]; then
    echo "FATAL: container not found at $CONTAINER_PATH" >&2
    echo "Run 'cluv build <cluster>' to build it (the image is tagged by uv.lock hash; rebuild after changing dependencies)." >&2
    exit 1
fi

# Set up the source code at the submitted commit in $SLURM_TMPDIR, so changes
# to the project between submission and job start don't affect the job (same
# approach as safe_job.sh; the container replaces only the venv).
project_root_in_tmpdir="$SLURM_TMPDIR/$project_name"
echo "Cloning the project at $GIT_COMMIT into $project_root_in_tmpdir"
srun --ntasks-per-node=1 --ntasks=$SLURM_JOB_NUM_NODES bash -e <<END
    cd $SLURM_TMPDIR
    git clone $project_root
    cd $project_root_in_tmpdir
    git checkout --detach $GIT_COMMIT
END

module load apptainer 2>/dev/null || true

echo "Running command: apptainer exec $CONTAINER_PATH $@"
srun apptainer exec --nv \
    --env "PYTHONPATH=$project_root_in_tmpdir" \
    --env MPLCONFIGDIR=/tmp/mpl \
    --bind "$SLURM_TMPDIR":"$SLURM_TMPDIR" \
    --bind /dev/shm:/dev/shm \
    ${SCRATCH:+--bind "$SCRATCH":"$SCRATCH"} \
    ${PROJECT:+--bind /project:/project} \
    --pwd "$project_root_in_tmpdir" \
    "$CONTAINER_PATH" \
    "$@"
