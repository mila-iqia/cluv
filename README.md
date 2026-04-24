# cluv

cluv — sync UV-based Python projects across HPC clusters.

## Status

In early development. Commands are functional, but expect bugs or missing features.

## Requirements

- Python >= 3.13
- [UV](https://docs.astral.sh/uv/)
- SSH access configured for each cluster in `~/.ssh/config` (run `cluv login` to open ControlMaster sessions)
- A GitHub repository with your project

## Installation

Install as a UV tool:

```bash
uv tool install git+https://github.com/mila-iqia/cluv
```

Then you can run `cluv` directly as a command:

```bash
cluv init
cluv login mila
cluv sync mila
cluv submit mila job.sh
```

## Quick Start

1. Initialize your project with:
   ```bash
   cluv init
   ```
2. Establish SSH connections to all configured clusters:
   ```bash
   cluv login
   ```
3. Sync your project to all clusters and run `uv sync` on each:
   ```bash
   cluv sync
   ```

## Configuration

Add a `[tool.cluv]` section to the `pyproject.toml` of your project. `cluv init` generates a default config, or you can write it by hand.

### Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `clusters` | list of strings _or_ table | SSH hostnames from `~/.ssh/config`, or a table of per-cluster settings (see below). |
| `results_path` | string (optional) | Path relative to the project root. When set, `cluv sync` rsyncs that directory back from each remote cluster. |

### `[tool.cluv.slurm]`
Environment variables applied when using Slurm commands on all clusters. Use this for global Slurm defaults such as resource limits or for tool configuration like (uv or W&B).

### `[tool.cluv.clusters.<name>]`
Environment variables for a specific cluster. Values here are merged on top of `[tool.cluv.slurm]` when submitting.

### Example

```toml
[tool.cluv]
clusters = ["mila", "narval", "tamia"]
results_path = "logs"

[tool.cluv.slurm]
# Applied to every cluster by default
UV_OFFLINE = "1"
WANDB_MODE = "offline"

[tool.cluv.clusters.mila]
# Overrides for the mila cluster only
UV_OFFLINE = "0"
WANDB_MODE = "online"
SBATCH_PARTITION = "long"
```

## Commands

### `cluv init`

Initialize the current directory as a cluv project. Must be run from inside your `$HOME` directory.

```
cluv init
```

Default project structure after `cluv init`:
```
my_project/
├── README.md
├── logs -> $SCRATCH/logs/my_project   # symlink to $SCRATCH
├── pyproject.toml        # includes [tool.cluv] config
├── scripts/
│   └── job.sh            # Slurm job script template
└── src/
    └── my_project/
        └── __init__.py
```

### `cluv login`

Open SSH ControlMaster connections to all configured clusters. Run this before any command that requires a live connection.

```
cluv login [<cluster> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster> ...` | One or more SSH hostnames. Defaults to all clusters in `[tool.cluv]`. |

### `cluv sync`

Push local git changes, then on each cluster: clone or fetch the repo, check out the current branch, and run `uv sync`. Optionally rsyncs results back if `results_path` is set in the config.

```
cluv sync [<cluster> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster> ...` | Clusters to sync. Defaults to all configured clusters. Pass explicit names to connect to specific clusters. |


### `cluv status`

Display an overview of each cluster: GPU availability, running/queued jobs, estimated queue wait, GPU utilisation, and disk usage. Falls back to mock data if no active connections exist.

```
cluv status [<cluster> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster> ...` | Clusters to query. Defaults to all configured clusters with an active SSH connection. |

### `cluv submit`

Submit a SLURM job on a remote cluster.

```
cluv submit <cluster> <job.sh> [<sbatch-flags> ...] [-- <program-args> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster>` | SSH hostname of the target cluster. |
| `<job.sh>` | Path to the job script. |
| `<sbatch-flags>` | Any extra flags to pass to `sbatch`. |
| `-- <program-args>` | Arguments passed to the job script itself (after `--`). |

### `cluv run`

Sync the project to a cluster, then run a command there with `uv run`.

```
cluv run <cluster> <command> [<args> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster>` | SSH hostname of the target cluster. |
| `<command>` | Command to run (passed to `uv run --directory=<project>`). |

## DRAC Clusters

DRAC clusters are detected automatically via the `$CC_CLUSTER` environment variable.

## Examples

See the [examples](examples) folder for sample projects using cluv. Each example includes a README with instructions specific to that project.
