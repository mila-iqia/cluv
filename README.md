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

## Examples

See the [examples](examples) folder for sample projects using cluv. Each example includes a README with instructions specific to that project.

## Configuration

Add a `[tool.cluv]` section to the `pyproject.toml` of your project. `cluv init` generates a default config, or you can write it by hand.
See the config at the project root for an example, or refer to the schema below.

### Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `clusters` | table | Per-cluster settings, keyed by SSH hostname from `~/.ssh/config`. |
| `env` | table | Global environment variables applied to all clusters. |
| `results_path` | string | Path relative to the project root for storing results. `cluv sync` rsyncs that directory back from each remote cluster. |

### `[tool.cluv.clusters.<name>.env]`
Environment variables for a specific cluster. Values here are merged on top of `[tool.cluv.env]` when submitting.

### Variables priority
Environment variables can be set at multiple levels when submitting jobs, with the following precedence (highest to lowest):
1. Command-line arguments to `cluv submit`.
2. Cluster-specific variables in `[tool.cluv.clusters.<name>.env]`
3. Global variables in `[tool.cluv.env]`
4. SBATCH directives inside the job script (e.g. `#SBATCH --export=VAR=value`)
5. Default values from the cluster (e.g. `SBATCH_PARTITION`)

### Example

Here's an example `pyproject.toml` with cluv configuration for three clusters, and some global and cluster-specific environment variables:

```toml
[tool.cluv]
results_path = "logs"

[tool.cluv.env]
SBATCH_TIMELIMIT = "3:00:00"
WANDB_MODE = "offline"

[tool.cluv.clusters.mila]
env = { WANDB_MODE="online", SBATCH_PARTITION="long" }

[tool.cluv.clusters.narval]

[tool.cluv.clusters.tamia]
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
│   ├── job.sh            # Slurm job script template
│   └── safe_job.sh       # Slurm job script template (copies .venv and prior results)
└── src/
    └── my_project/
        └── __init__.py
```

### `cluv login`

Open SSH ControlMaster connections to all configured clusters. Run this before any command that requires a live connection.

```
cluv login [<cluster> ...]
```

### `cluv sync`

Push local git changes, then on each cluster: clone or fetch the repo, check out the current branch, and run `uv sync`. Optionally rsyncs results back if `results_path` is set in the config.

```
cluv sync [<cluster> ...]
```


### `cluv status`

Display an overview of :
* Cluster: GPU availability, running/queued jobs and disk usage.
* Jobs: cached jobs from `cluv submit` with their status.

```
cluv status [<table>]
```

### `cluv submit`

Submit a SLURM job on a remote cluster.

```
cluv submit <cluster> <job.sh> [<sbatch-flags> ...] [-- <program-args> ...]
```

For example:

```bash
cluv submit rorqual script/job.sh --time=00:10:00 -- python main.py
```

### `cluv run`

Sync the project to a cluster, then run a command there with `uv run`.

```
cluv run <cluster> <command> [<args> ...]
```
