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
    echo "Run 'cluv build <cluster>' to build one." >&2
    exit 1
fi

module load apptainer 2>/dev/null || true

echo "Running command: apptainer exec $CONTAINER_PATH $@"
srun apptainer exec --nv \
    --env PYTHONUNBUFFERED=1 \
    --env PYTHONPATH=$project_root \
    --bind $project_root:$project_root \
    ${SLURM_TMPDIR:+--bind $SLURM_TMPDIR:$SLURM_TMPDIR} \
    ${SCRATCH:+--bind $SCRATCH:$SCRATCH} \
    $CONTAINER_PATH \
    "$@"
