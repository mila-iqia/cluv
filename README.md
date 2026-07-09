# cluv

cluv — sync UV-based Python projects across HPC clusters.

## Status

In early development. Commands are functional, but expect bugs or missing features.

## Requirements

- Python >= 3.13
- [UV](https://docs.astral.sh/uv/)
- SSH access configured for each cluster in `~/.ssh/config`
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

Add a `[tool.cluv]` section to the `pyproject.toml` of your project to manage the behavior of the tool.
The command `cluv init` will add a default config if it doesn't already exists in the `.toml`.

See the config at the project root for an example, or refer to the [docs](https://mila-iqia.github.io/cluv/).

## Examples

See the [examples](examples) folder for sample projects using cluv. Each example includes a README with instructions specific to that project.

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
