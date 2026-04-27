# Pytorch Setup

A simple example to install `torch` and detect the available GPUs.

```bash
# Run the cluv submit command at the root of this example folder.

# For the mila cluster, with no additional Slurm args
cluv submit mila scripts/job.sh -- python src/pytorch_setup/main.py

# For a DRAC cluster, with an additional Slurm argument to specify the allocation account
cluv submit <DRAC_cluster> scripts/job.sh  --account=<allocation_account> -- python src/pytorch_setup/main.py
```

Note : currently use a Pytorch version inferior to `2.11.0` to avoid compatibility issues with CUDA 12.8.
