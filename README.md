# cluv

cluv — sync UV-based Python projects across HPC clusters.

## Status

Early development. Core commands (`login`, `sync`, `run`) are functional. `status` shows live cluster data. `init` is not yet implemented.

## Requirements

- Python >= 3.13
- [UV](https://docs.astral.sh/uv/)
- SSH access configured for each cluster in `~/.ssh/config` (run `cluv login` to open ControlMaster sessions)

## Installation

Install as a UV tool:

```bash
uv tool install git+https://github.com/mila-iqai/cluv
```

Or run directly from the repo without installing:

```bash
uv run cluv <command>
```

## Quick Start

1. Add a `[tool.cluv]` section to your project's `pyproject.toml` (see [Configuration](#configuration) below).
2. Establish SSH connections to all configured clusters:
   ```bash
   cluv login
   ```
3. Sync your project to all clusters and run `uv sync` on each:
   ```bash
   cluv sync
   ```

## Configuration

Add a `[tool.cluv]` section to the `pyproject.toml` of your project:

```toml
[tool.cluv]
clusters = ["mila", "narval", "tamia"]   # SSH hostnames from ~/.ssh/config
results_path = "results"                  # optional: rsync remote results back here
```

`clusters` must match SSH hostnames in `~/.ssh/config`. `results_path` is relative to the project root; if set, `cluv sync` rsyncs that directory back from each remote cluster.

## Commands

| Command | Description |
|---------|-------------|
| `cluv login` | Open SSH ControlMaster connections to all configured clusters. Run this first, or before any command that requires a live connection. |
| `cluv sync` | Push local git changes, then on each cluster: clone or fetch the repo, check out the current branch, and run `uv sync`. Optionally rsyncs results back. |
| `cluv status` | Display an overview of each cluster: partition availability, running/queued jobs, and storage usage. |
| `cluv run` | Run a command in the synced project on a remote cluster. *(Not yet implemented.)* |
| `cluv init` | Set up the project on all configured clusters for the first time. *(Not yet implemented.)* |

## DRAC Clusters

DRAC clusters are detected automatically via the `$CC_CLUSTER` environment variable.
