# cluv

This is a quick overview. For more information, check out the [introduction](guides/introduction.md).


## Installation

Add the package to your project with `uv add` or `pip install`:

```console
uv add cluster-uv
```

Install as a command-line tool in an isolated environment:

```console
uv tool install cluster-uv
```

If you want the bleeding edge version from GitHub, use:

```console
uv add git+https://github.com/mila-iqia/cluv
```

## Usage

To view all available commands, use the `--help` flag:
```console
cluv --help
```

Here are some common workflows:

### Setup cluv in an existing project

```console
cd ~/my-project  # Cluv requires your project to be located somewhere under your home directory (`$HOME`).
cluv init
```

### Establish SSH connections to all clusters

```console
cluv login
```

### Sync your project on all clusters
```console
cluv sync
```

Need to set up dataset replication with `cluv sync`? See the
[dataset sync guide](guides/syncing-datasets.md).


### Launch a Hydra sweep on a remote cluster

```console
python main.py -m launcher=cluv lr=0.01,0.001 +seed=1,2,3
```

See the [Hydra launcher guide](guides/hydra-launcher.md) for setup and usage.


### Sync your project on a specific cluster
```console
cluv sync rorqual
```

### Clean up old run results from the clusters
```console
cluv clean
```

See the [cleaning up runs guide](guides/cleaning-runs.md) for details on how this decides what's safe to delete.

### Submit a job to a specific cluster
```console
cluv submit rorqual scripts/job.sh --time=00:10:00 -- python main.py
```

### Run a command in the synced project a specific cluster
```console
cluv run mila -- ls logs
```

### Check the status of your clusters and jobs
```console
cluv status
```
