# cluv

cluv — sync UV-based Python projects across HPC clusters.

## Status

In early development. Commands are functional, but expect bugs or missing features.

## Requirements

- Python >= 3.13
- [UV](https://docs.astral.sh/uv/)
- SSH access configured for each cluster in `~/.ssh/config` (run `cluv login` to open ControlMaster sessions)

## Installation

Install as a UV tool:

```bash
uv tool install git+https://github.com/mila-iqia/cluv
```

Then you can run `cluv` directly as a command like :

```bash
cluv init
cluv login mila
cluv sync mila
cluv submit mila job.sh
```

## Quick Start

1. Initialize your project with :
   ```bash
   cluv init
   ```
   This sets up a UV project and adds a default `[tool.cluv]` section to your `pyproject.toml`. You can customize this config later (see [Configuration](#configuration) below). 
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
Environment variables (key/value pairs) applied when using Slurm commands on all clusters. Use this for global Slurm defaults such as resource limits or for tool configuration like (uv of W&B).

### `[tool.cluv.clusters.<name>]`
Environment variables for a specific cluster. Values here are merged on top of `[tool.cluv.slurm]` when submitting.

### Example

```toml
[tool.cluv]
clusters = ["mila", "narval", "tamia"]   # SSH hostnames from ~/.ssh/config
results_path = "logs"                    # rsynced back by `cluv sync`

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

Steps performed:
1. Runs `uv init --package --build-backend hatch --python 3.13` (skipped if `pyproject.toml` already exists).
2. Warns if no git remote is configured (required for `sync` and `submit`).
3. Appends a `[tool.cluv]` section to `pyproject.toml` if one is not already present, with defaults for Mila and all DRAC clusters.
4. Creates `scripts/job.sh` — a Slurm job script template — if it does not already exist.
5. Creates a symlink `<results_path>/ → $SCRATCH/<results_path>/<project_name>/` so large outputs go to scratch rather than filling `$HOME`.

Default project structure after `cluv init`:
```
my_project/
├── pyproject.toml        # includes [tool.cluv] config
├── scripts/
│   └── job.sh            # Slurm job script template
├── logs -> $SCRATCH/logs/my_project   # symlink to $SCRATCH
└── src/
    └── my_project/
        └── __init__.py
```

### `cluv login`

Open SSH ControlMaster connections to all configured clusters. Run this first, or before any command that requires a live connection.

```
cluv login [<cluster> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster> ...` | One or more SSH hostnames. Defaults to all clusters in `[tool.cluv]`. |

### `cluv sync`

Push local git changes, then on each cluster: clone or fetch the repo, check out the current branch, and run uv sync. Optionally rsyncs results back if `results_path` is set in the config.

```
cluv sync [<cluster> ...]
```

| Argument | Description |
|----------|-------------|
| `<cluster> ...` | Clusters to sync. Defaults to all configured clusters. Pass explicit names to connect to specific clusters. |

Sync steps per cluster:
1. Install or update `uv` to match the local version.
2. Clone the repo from GitHub (if not present).
3. Run `uv sync` on the remote.
4. Rsync `results_path/` back to the local machine (if configured).

### `cluv status`

Display an overview of each cluster: GPU availability, running/queued jobs (yours and cluster-wide), estimated queue wait, GPU utilisation, and disk usage. Falls back to mock data if no active connections exist.

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

Steps performed:
1. Checks that the git working tree is clean.
2. Captures the current commit hash and sets it as `GIT_COMMIT`.
3. Syncs the project to the cluster (equivalent to `cluv sync <cluster>`).
4. Merges `[tool.cluv.slurm]` env vars with per-cluster overrides from `[tool.cluv.clusters.<name>]`.
5. Runs `sbatch` on the remote with all env vars, sbatch flags, and program args.

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
