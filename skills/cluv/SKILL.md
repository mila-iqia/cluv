---
name: cluv
description: "Assist with cluv multi-cluster HPC projects. Use when working in a repo with a [tool.cluv] section in pyproject.toml."
---

# cluv

cluv is a CLI tool for syncing UV-based Python projects across multiple HPC clusters (Mila, DRAC/Narval, etc.) and managing remote jobs.

## Running cluv

Always run from the project root:

```bash
uv run cluv <command>
```

Add verbosity with `-v`, `-vv`, or `-vvv`:

```bash
uv run cluv -v status
```

## Commands

| Command | Status | Description |
|---------|--------|-------------|
| `cluv login` | implemented | Establish SSH ControlMaster connections to all configured clusters (run this first; prompts for 2FA sequentially) |
| `cluv sync` | implemented | Push local git changes then pull + `uv sync` on each cluster in parallel; optionally rsync results back |
| `cluv status` | partial | Show per-cluster job and storage stats (mock data for now) |
| `cluv init` | stub | Initialize a new cluv-managed project (not yet implemented) |
| `cluv run` | stub | Submit a job to one or more clusters (not yet implemented) |

## Configuration

cluv reads `[tool.cluv]` from the nearest `pyproject.toml`:

```toml
[tool.cluv]
clusters = ["mila", "narval"]   # SSH hostnames from ~/.ssh/config
results_path = "results"        # optional: local path to rsync results into
```

- `clusters`: list of SSH hostnames; must match entries in `~/.ssh/config`
- `results_path`: if set, `cluv sync` rsyncs remote results back here after syncing

## SSH and authentication

All remote operations use SSH ControlMaster sockets (via milatools `RemoteV2`). A socket must already be open before running `sync` or `status` — if it is not, the cluster is silently skipped.

Run `cluv login` once per session to open sockets for all clusters. Login is sequential to avoid concurrent 2FA prompts.

## DRAC clusters

DRAC clusters (narval, tamia, rorqual, fir, etc.) require a `uv.toml` in the project pointing to the DRAC wheelhouse for offline installs, because DRAC compute nodes have no internet access.

DRAC clusters are detected via the `$CC_CLUSTER` environment variable. They expose a `partition-stats` command used by `cluv status` to get queue/node data.

## Architecture notes

- Entry point: `cluv/__main__.py:main()` — argparse/simple_parsing, dispatches to `cluv/cli/<cmd>.py`
- Config: `cluv/config.py` — `CluvConfig` dataclass with `clusters` and `results_path`
- Async: multi-cluster ops use `asyncio.gather`; `sync` uses a progress bar via milatools
- Mila cluster is the "home base", detected by `Path("/home/mila").exists()`
