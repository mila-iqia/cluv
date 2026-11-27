#!/bin/bash
# Default job script: for the clusters that allocate cores and memory, not whole nodes (Mila,
# Narval, Rorqual, Fir, Nibi, Killarney, Vulcan). The clusters that allocate whole nodes have their
# own `job_<cluster>.sh` script, set by `job_script_path` in the cluv config.
#
# Edit the resources below, or override them when submitting:
#   cluv submit <cluster> --gpus-per-node=1 --time=2:00:00 -- python main.py
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=0:15:00

# Clone the project at the submitted commit and run the command (see scripts/run.sh).
exec bash scripts/run.sh "$@"
