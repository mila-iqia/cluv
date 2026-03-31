# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies and activate the venv
uv sync

# Run cluv directly (no install needed)
uv run cluv <command>

# Run with verbose logging (-v, -vv, -vvv)
uv run cluv -v status
```

Run tests with `uv run pytest`.

## Architecture

`cluv` is a CLI tool for managing UV-based Python projects across multiple HPC clusters (Mila, DRAC/Narval, etc.).

**Entry point**: `cluv/__main__.py:main()` — builds an `argparse`/`simple_parsing` parser with subcommands, then dispatches to the appropriate async or sync function. Each subcommand registers its args in an `add_<cmd>_args()` function in `__main__.py`, and its implementation lives in `cluv/cli/<cmd>.py`.

**Config**: `cluv/config.py` — reads the `[tool.cluv]` section from the nearest `pyproject.toml`. Key fields: `clusters` (list of hostnames or table with per-cluster settings), `slurm` (global `SBATCH_*` env var defaults), `cluster_configs` (per-cluster `SBATCH_*` overrides). Config is cached with `@functools.cache`.

**SSH connections**: All remote operations go through `milatools.utils.remote_v2.RemoteV2`. Connections reuse existing SSH ControlMaster sockets (checked via `control_socket_is_running_async`) to avoid triggering 2FA prompts. `cluv login` establishes fresh connections sequentially (to avoid concurrent 2FA prompts). The `login.get_remote_without_2fa_prompt()` helper is used by `sync` to only operate on already-connected clusters.

**Async pattern**: Multi-cluster operations use `asyncio.gather` for parallelism. The `sync` command uses `milatools.utils.parallel_progress.run_async_tasks_with_progress_bar` to display per-cluster progress. The top-level `main()` uses `asyncio.run()` when the subcommand function is a coroutine.

**Rich console**: A single `rich.Console` instance is created in `cluv/utils.py` and patched into `milatools` internals (`milatools.cli.console`, etc.) so all output goes through one stream.

**`cluv status`** (`cluv/cli/status.py`): Currently entirely mock data. The `ClusterStatus` / `JobStats` / `StorageStats` dataclasses define the data model. The `get_mock_cluster_status()` function is the stub to replace with real implementations. The display logic (`_build_cluster_table`, `_build_my_jobs_table`) is separate and should not need changes when real data is wired in.

**`cluv sync`** (`cluv/cli/sync.py`): The most complete command. Runs: `git push` locally → `git clone`/`fetch`/`checkout`/`pull` on each remote → `uv sync` on each remote → optional `rsync` of results back. Each cluster's work is encapsulated in `sync_task_function`.

**`cluv submit`** (`cluv/cli/submit.py`): `cluv submit <cluster> <job.sh> [--no-sync] [sbatch-flags...] [-- program-args...]`. Enforces a clean git tree, injects `GIT_COMMIT`, merges global + per-cluster `SBATCH_*` env vars from config, then runs `sbatch` on the remote. Arguments before `--` are forwarded as sbatch flags; arguments after `--` are passed to the job script.

**`cluv init`** and **`cluv run`**: Not yet implemented (stubs/`NotImplementedError`).

## Cluster notes

- **Mila cluster** is typically the "home base". Detected via `Path("/home/mila").exists()` in `utils.current_cluster()`.
- **DRAC clusters** (narval, tamia, rorqual, fir, etc.) are detected via `$CC_CLUSTER` env var. They expose a `partition-stats` command that prints a text table of queued/running/idle node counts by partition type (Regular vs GPU) and walltime bucket. See `partiton-stats_output.txt` for a sample.
- DRAC clusters require a special `uv.toml` pointing to the DRAC wheelhouse for offline installs.
- Cluster hostnames in `[tool.cluv]` must match the SSH hostnames configured in `~/.ssh/config`.

## Work etiquette

- Make small, clean commits regularly. Each commit should only contain changes related to a single "action" or "theme".
- Write minimalist, clean, pythonic code. Avoid being overly general or abstract.
