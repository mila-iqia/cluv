# Hydra Example

A [Hydra](https://hydra.cc/)-based example that trains on CIFAR-10 and logs to Weights & Biases,
showing how to use the Cluv Hydra launcher to run `--multirun` sweeps on remote Slurm clusters.

For the full walkthrough (installation, launcher config, the `${cluv:...}` resolver, etc.), see the
[Hydra launcher guide](../../docs/guides/hydra-launcher.md) and the
[dataset syncing guide](../../docs/guides/syncing-datasets.md).

## Setup

```console
cd examples/hydra_example
uv sync
```

This example needs the CIFAR-10 dataset replicated to `datasets_path` (see `pyproject.toml`).
Make sure you're logged in, then sync it to whichever clusters you plan to use:

```console
cluv login
cluv sync
```

## Running locally

```console
uv run python main.py
```

Overrides work the usual Hydra way, e.g. `uv run python main.py lr=0.01 seed=2`.

## Submitting a single job to a cluster

```console
cluv submit mila scripts/job.sh -- python main.py
```

## Running a multirun sweep with the Cluv launcher

Activate the launcher config in `configs/launcher/cluv.yaml` with `launcher=cluv`:

```console
cluv login
uv run python main.py -m launcher=cluv lr=0.01,0.001 seed=1,2,3
```

This syncs the project to the target cluster (set via `cluster:` in
`configs/launcher/cluv.yaml`), submits one job per config combination, waits for them to
complete, and rsyncs the results back to the local `logs/` symlink.
