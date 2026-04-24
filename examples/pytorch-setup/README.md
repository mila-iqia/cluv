# Pytorch Setup

A simple example to install `torch` and detect the available GPUs.

```bash
# Run the cluv submit command at the root of this example folder.
# Call main.py in the job.sh script on the mila cluster in a SLURM job.
cluv submit mila scripts/job.sh --gres=gpu:1 --cpus-per-task=2 --mem=16G --time=00:15:00 -- python src/pytorch_setup/main.py
```
