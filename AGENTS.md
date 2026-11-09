# AGENTS.md

Guidance for AI agents (and humans) working on the `cluv` repository.

## What cluv is

`cluv` ("cluster" + "uv") is a CLI tool that syncs [uv](https://docs.astral.sh/uv/)-based Python
projects across multiple Slurm HPC clusters (Mila, DRAC, PAICE) and dispatches jobs to all of them.
It is a lightweight wrapper around `git`, `uv`, `ssh` and `sbatch`, not a job manager replacement.

Hard assumptions:

- The project is managed by **uv** and has a `pyproject.toml` (with a `[tool.cluv]` section).
- The project is a **git repo hosted on GitHub**; clusters clone it from there.
- SSH access is configured in `~/.ssh/config` with ControlMaster multiplexing; `cluv login`
  creates the reusable connections (so later commands never trigger 2FA prompts).
- **Not supported on Windows** (needs an SSH client with multiplexing support).

Published on PyPI as `cluster-uv`; the module is `cluv`, the CLI is `cluv`.
User-facing docs live in `docs/` (mkdocs-material, published at https://mila-iqia.github.io/cluv/).

## Commands (working on this repo)

```bash
uv sync                        # install dependencies (also installs the examples workspace members)
uv run cluv <command>          # run cluv from the checkout (-v / -vv for more logging, -q quiet)

uv run pytest                  # unit tests (doctests in cluv/ modules run too)
uv run pytest -m "not integration"   # skip tests that need live SSH connections to real clusters
uv run pytest tests/test_submit.py   # a single test file
uv run pre-commit run --all-files    # ruff check --fix + ruff format + misc hooks

uv run mkdocs serve --livereload     # serve the docs from docs/
```

Pytest runs with `--doctest-modules` (docstring examples in `cluv/` count as tests — keep them
runnable) and pytest-xdist in parallel (`--dist=loadgroup`). Tests marked `integration` parametrize
over a `cluster`/`remote` fixture that uses whichever clusters are currently logged in.

## Using cluv

Full flag reference: `cluv <command> --help` and `docs/commands.md`.

- `cluv init [path]` — generate a `[tool.cluv]` config + job-script templates. Safe to re-run.
- `cluv login [clusters...]` — open the reusable SSH connections (sequentially, to avoid
  concurrent 2FA prompts).
- `cluv sync [clusters...]` — clone/checkout + `uv sync` on each cluster,
  fetch results back.
- `cluv submit <cluster> [<job.sh>] [sbatch-args...] [-- program-args...]` — sync, then `sbatch`
  on the cluster (clean git tree required, or `--autocommit`). The racing idea: `cluster=first`,
  multiple configured allocations, and `--vram` all fan the submission out into many jobs and
  keep only the first one to start; `--chunking` splits a long job into checkpointed chunks.
- `cluv run <cluster> <command...>` — sync + `uv run <command>` on one cluster.
- `cluv sh <command...>` — raw shell command on every connected cluster.
- `cluv status [clusters|jobs|all]` — GPU / storage / queue overview, plus a table of submitted jobs.
- `cluv clean [clusters...]` — delete cluster-side results the user already deleted locally.
- `cluv disable <cluster> [period]` / `cluv enable` — temporarily skip a cluster everywhere
  without touching the config.
- Hydra sweeps: `python main.py -m launcher=cluv ...` via the plugin in `hydra_plugins/cluv/`.

## Code structure

### Entry points

- **CLI**: `cluv/__main__.py` builds an argparse parser (one `add_<cmd>_args()` per subcommand)
  and dispatches to `cluv/cli/<cmd>.py` (coroutines run through `asyncio.run`). `submit`'s
  argument parsing (`argparse.REMAINDER`, with cluv's own flags "rescued" back out of it) is
  subtle — read it before changing it.
- **Hydra plugin**: `hydra_plugins/cluv/` — `CluvLauncher` (the `hydra` extra) reuses the same
  sync/submit machinery, plus a `${cluv:...}` config resolver.
- **In-job runtime**: `cluv/job.py` — what *user code inside jobs* imports for unique run ids
  and results/datasets paths, keyed off the `CLUV_CLUSTER` and `GIT_COMMIT` env vars that
  `cluv submit` injects.

### Configuration

Users write `[tool.cluv]` (globals) and `[tool.cluv.clusters.<name>]` (overrides) in their
`pyproject.toml`; `cluv/config.py` validates them into a cached Pydantic model and merges them
per cluster. Paths are `PurePosixPath`s containing `$SCRATCH`-style vars, expanded only on the
machine they belong to.

### Commands (`cluv/cli/`, one file per command)

Commands build on each other: `run` and `submit` call `sync`, and all remote I/O goes through
`cluv/remote.py` — `Remote(hostname)` reuses the SSH ControlMaster sockets opened by `cluv login`.

- `submit.py` (+ `submit_utils/` for `--chunking` and `--vram`) — merges sbatch flags
  (CLI > per-cluster > global), syncs, submits, then races the jobs. `get_sbatch_command()`'s
  nested `bash --login -c '...'` quoting (so `$SCRATCH` expands remotely but program args
  don't) is easy to break — read it first.
- `sync.py` — the push/clone/`uv sync`/fetch-results pipeline; skips disabled clusters, clusters
  sharing a filesystem with one already synced, and work the cache shows is already done.
- `status.py` + `slurm.py` — one SSH round-trip per cluster; keep the Slurm-output parsers in
  `slurm.py` pure (no I/O, unit-tested on fixture text), rendering separate in `status.py`.
- `cache.py` — state that survives between commands under `~/.cache/cluv/<project>/`:
  per-cluster sync state, disabled clusters, cached GPU types, job history, submission logs.

### Docs, tests, examples, scripts

- `docs/` — mkdocs-material site: `commands.md` (per-command reference), `guides/`,
  `hydra-launcher.md`, `troubleshooting.md`.
- `tests/` — one file per module/feature; `conftest.py` has the `cluster`/`remote` fixtures and
  the `$HOME`/`$SCRATCH` mocking.
- `examples/` — sample projects (uv workspace members, tested by `tests/test_examples*`);
  update them when a feature they use changes.
- `scripts/` — `job.sh` / `safe_job.sh` templates that `cluv init` copies into new projects.

## Cross-cutting patterns

- **Async everywhere**: multi-cluster operations use `asyncio.gather` (see `gather_dict`);
  console output from parallel tasks is serialized with `console_lock`.
- **One rich console**: import `console` from `cluv.utils`; never create another `Console`,
  and respect `console.quiet`.
- **Config precedence**: CLI flags > `[tool.cluv.clusters.<name>.*]` > globals; a *list* of
  `sbatch_args` means "one job per allocation, race them".
- **Jobs know their config**: `submit` injects `CLUV_CLUSTER` and `GIT_COMMIT` because clusters
  self-report unreliably (e.g. `trillium-gpu` reports `CC_CLUSTER=trillium`).
- **Remote paths**: `PurePosixPath` with `$HOME`/`$SCRATCH` vars; expand them on the remote
  machine, never locally.
- **Docs are part of the contract**: changing a command's flags or behavior means updating
  `docs/commands.md` (and the relevant guide/example) in the same change.

## Work etiquette

- Make small, clean commits regularly; each commit covers a single action or theme.
- Write minimalist, clean, pythonic code; avoid being overly general or abstract.
- ruff with `line-length = 99` (via pre-commit); docstrings double as `--help` and mkdocs
  content, keep them consistent with the surrounding code.
- **Isolate in a worktree before editing.** Multiple agents and humans share this checkout:
  use your agent's worktree tool (or `git worktree add`) instead of the shared checkout, and
  commit/push from the worktree so the work survives its cleanup.
