# Pytorch Setup

A simple PyTorch example. To run it, first navigate to the root of this example:
```
cd examples/pytorch-example
```


To submit a job on all available clusters and keep the first to run:

```bash
cluv submit first scripts/job.sh -- python main.py
```

To run on a specific remote cluster:

```bash
cluv submit mila scripts/job.sh -- python main.py
cluv submit nibi scripts/job.sh -- python main.py
...
```

Note : currently use a Pytorch version inferior to `2.11.0` to avoid compatibility issues with CUDA 12.8.
