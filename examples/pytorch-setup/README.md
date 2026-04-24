# Pytorch Setup

A simple example to install `torch` and detect the available GPUs.

```bash
# Run the cluv submit command at the root of this example folder.

# For the mila cluster
cluv submit mila scripts/job.sh --gres=gpu:1 --cpus-per-task=2 --mem=16G --time=00:15:00 -- python src/pytorch_setup/main.py

# For a DRAC cluster
cluv submit <DRAC_cluster> scripts/job.sh --gres=gpu:1 --cpus-per-task=2 --mem=16G --time=00:15:00 --account=<allocation_account> -- python src/pytorch_setup/main.py
```
