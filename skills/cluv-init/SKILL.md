---
name: cluv-init
description: Bootstrap a project to use cluv for managing UV-based Python runs across HPC clusters (Mila, DRAC, etc). Use when the user wants to start using cluv in a new or existing project, or asks what config fields cluv supports.
---

# Setting up a project with cluv

`cluv init` bootstraps a project (or adds cluv config to an existing one) so it can sync and
submit Slurm jobs across multiple clusters.

## Running it

```bash
cluv init [path]
```

- `path` defaults to the current directory.
- If `pyproject.toml` already exists, cluv adds a `[tool.cluv]` section to it.
- If it doesn't exist, cluv runs `uv init` first, then adds the section.
- It also tries to create a `scripts/` directory with two job script templates
  (`job.sh`, `safe_job.sh`) and a `logs/` symlink pointing at
  `$SCRATCH/logs/<project_name>` (the resolved `results_path`).

Resulting layout for a brand-new project:

```
my_project/
├── README.md
├── logs -> $SCRATCH/logs/<project_name>
├── pyproject.toml        # includes [tool.cluv]
├── scripts/
│   ├── job.sh
│   └── safe_job.sh
└── src/
    └── my_project/
        └── __init__.py
```

Re-running `cluv init` on an already-initialized project is safe: it only adds what's missing,
it doesn't overwrite an existing `[tool.cluv]` section or job scripts.

## The `[tool.cluv]` config it creates

All fields below can be overridden per cluster under `[tool.cluv.clusters.<name>]`, except
`data_source`.

```toml
[tool.cluv]
job_script_path = "scripts/job.sh"    # default job script for `cluv submit`
results_path = "$SCRATCH/logs/cluv"   # where job results are stored/fetched
results_symlink = "logs"              # local symlink name pointing at results_path

# project_dir = "$HOME/<project_name>"                       # where the project lives on remotes
# data_source = "<cluster>:<path>"                            # dataset source for `cluv sync --sync-datasets`
# datasets_path = "<path_to_copy_datasets_on_all_clusters>"

[tool.cluv.env]
UV_OFFLINE = "1"       # compute nodes usually have no internet — flip per cluster if they do
WANDB_MODE = "offline"

[tool.cluv.sbatch_args]
time = "3:00:00"
requeue = true

[tool.cluv.clusters.mila]
env = { UV_OFFLINE = "0", WANDB_MODE = "online" }   # Mila login/compute nodes have internet

[tool.cluv.clusters.narval]
sbatch_args = { account = "def-someuser" }
```

`clusters` (a list of hostnames, or the `[tool.cluv.clusters.<name>]` tables) must match hostnames
already configured in `~/.ssh/config` — cluv doesn't manage SSH config itself. If the user doesn't
have one yet, point them at [milatools](https://github.com/mila-iqia/milatools) to generate it.

## After init

The natural next steps are, in order:

1. `cluv login` — see [[cluv-clusters]] for connecting to clusters and handling 2FA.
2. `cluv sync` — see [[cluv-sync]] to push the project and run `uv sync` remotely.
3. `cluv submit <cluster>` — see [[cluv-submit]] to submit a job.

## Troubleshooting

- **"config not found" / no `[tool.cluv]` section**: config is read from the *nearest*
  `pyproject.toml` walking up from the current directory (`cluv/config.py`, cached per process).
  Make sure the command runs from inside the project, and that `cluv init` actually completed.
- **Wrong cluster names**: cluster hostnames in `[tool.cluv]` must exactly match the `Host` entries
  in `~/.ssh/config` — a mismatch here surfaces later as an SSH connection failure, not an init
  error.
