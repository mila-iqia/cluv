#!/bin/bash
#SBATCH --output=logs/%j/slurm-%j.out
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --gpus-per-task=l40s:1
## Or --gpus-per-task=rtx8000:1    to request a different GPU model
## Or --gpus-per-task=1            for any GPU model
#SBATCH --mem-per-gpu=16G
#SBATCH --time=00:5:00
#SBATCH --requeue           # allows the job to be requeued if preempted
#SBATCH --signal=B:TERM@60  # tells the controller to send SIGTERM to the job 1
                            # min before its time ends to give it a chance for
                            # better cleanup. If you cancel the job manually,
                            # make sure that you specify the signal as TERM like
                            # so `scancel --signal=TERM <jobid>`.
                            # https://dhruveshp.com/blog/2021/signal-propagation-on-slurm/

# Exit on error
set -e

# Echo time and hostname into log
echo "Date:     $(date)"
echo "Hostname: $(hostname)"
echo "Job has been preempted $SLURM_RESTART_COUNT times."

# To make your code as much reproducible as possible with
# `torch.use_deterministic_algorithms(True)`, uncomment the following block:
## === Reproducibility ===
## Be warned that this can make your code slower. See
## https://pytorch.org/docs/stable/notes/randomness.html#cublas-and-cudnn-deterministic-operations
## for more details.
# export CUBLAS_WORKSPACE_CONFIG=:4096:8
## === Reproducibility (END) ===

# Stage dataset into $SLURM_TMPDIR
mkdir -p $SLURM_TMPDIR/data
cp /network/datasets/cifar10/cifar-10-python.tar.gz $SLURM_TMPDIR/data/
# General-purpose alternatives combining copy and unpack:
#     unzip   /network/datasets/some/file.zip -d $SLURM_TMPDIR/data/
#     tar -xf /network/datasets/some/file.tar -C $SLURM_TMPDIR/data/

# Execute Python script
# Use the `--offline` option of `uv run` on clusters without internet access on compute nodes.
# Using the `--locked` option can help make your experiments easier to reproduce (it forces
# your uv.lock file to be up to date with the dependencies declared in pyproject.toml).
srun uv run python src/checkpointing/main.py
