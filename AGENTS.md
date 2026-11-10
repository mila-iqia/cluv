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

Published on PyPI as `cluster-uv`; the module and the CLI are `cluv`.
User-facing docs live in `docs/` (mkdocs-material, published at https://mila-iqia.github.io/cluv/).

## Commands (working on this repo)

Full flag reference: `cluv <command> --help` and `docs/commands.md`.

```bash
uv sync                        # install dependencies (also installs the examples workspace members)
uv run cluv <command>          # run cluv from the checkout (-v / -vv for more logging, -q quiet)

uv run pytest                  # everything (integration tests run when logged in to clusters, skip otherwise)
uv run pytest -m "not integration"   # unit tests only (no SSH required)
uv run pytest tests/test_submit.py   # a single test file
uv run pre-commit run --all-files    # ruff check --fix + ruff format + misc hooks

uv run mkdocs serve --livereload     # serve the docs from docs/
```

Pytest runs with `--doctest-modules` (docstring examples in `cluv/` count as tests, keep them
runnable) and pytest-xdist in parallel. The two kinds of tests:

- **Unit tests** never touch the network: pure functions are tested against against fixture data,
  and the commands run against fake or monkeypatched `Remote`s.
- **Integration tests** (`@pytest.mark.integration`) do the real thing: sync, submit and
  query actual clusters over the live SSH connections from `cluv login`. The `cluster`/`remote`
  fixtures in `tests/conftest.py` parametrize them over every currently-connected cluster and
  skip the rest (in CI they run only on a self-hosted runner with real connections).

## Code structure

### Entry points

- **CLI**: `cluv/__main__.py` builds an argparse parser (one `add_<cmd>_args()` per subcommand)
  and dispatches to `cluv/cli/<cmd>.py`. `submit` also parse remaining args (with cluv's own flags
  "rescued" back out of it).
- **Hydra plugin**: `hydra_plugins/cluv/` with `CluvLauncher` (the `hydra` extra) reuses the same
  sync/submit machinery, plus a `${cluv:...}` config resolver.
- **In-job runtime**: `cluv/job.py` with what *user code inside jobs* imports for unique run ids
  and results/datasets paths, and env vars that `cluv submit` injects.

### Configuration

Users write `[tool.cluv]` (globals) and `[tool.cluv.clusters.<name>]` (overrides) in their
`pyproject.toml`. `cluv/config.py` validates them into a cached Pydantic model and merges them
per cluster. Paths are `PurePosixPath`s containing `$SCRATCH`-style vars, expanded only on the
machine they belong to.

### Commands (`cluv/cli/`, one file per command)

Commands build on each other: `run` and `submit` call `sync`, and all remote I/O goes through
`Remote(hostname)` to reuses the SSH ControlMaster sockets opened by `cluv login`.

Use `cache.py` for state that survives between commands under `~/.cache/cluv/<project>/` (per-cluster
sync state, disabled clusters, cached GPU types, job history, submission logs...).

### Docs, tests, examples, scripts

- `docs/`, mkdocs-material site: `commands.md` (per-command reference), `guides/`,
  `hydra-launcher.md`, `troubleshooting.md`.
- `tests/`, one file per module/feature; `conftest.py` has the `cluster`/`remote` fixtures and
  the `$HOME`/`$SCRATCH` mocking.
- `examples/`, sample projects (uv workspace members, tested by `tests/test_examples*`);
  update them when a feature they use changes.
- `scripts/`, `job.sh` / `safe_job.sh` templates that `cluv init` copies into new projects.

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
- **Isolate in a worktree before editing.** Multiple agents and humans share this checkout:
  use your agent's worktree tool (or `git worktree add`) instead of the shared checkout, and
  commit/push from the worktree so the work survives its cleanup.
