---
name: cluv-init
description: Bootstrap a UV-based Python project to work with cluv across HPC clusters (create pyproject.toml/[tool.cluv] section, job script templates, results symlink). Use when the user wants to start using cluv on a new or existing project, or asks what belongs in the [tool.cluv] config.
---

# Setting up a project with `cluv init`

```bash
cluv init            # initialize the current directory
cluv init my_project # initialize (and create, if needed) a specific path
```

- If `pyproject.toml` already exists, cluv just adds a `[tool.cluv]` section to it.
- If it doesn't exist, cluv runs `uv init` first, then adds the section.
- It also tries to create a `scripts/` directory with two job script templates
  (`job.sh`, `safe_job.sh`) and a `logs/` symlink pointing at the resolved
  `results_path` (typically `$SCRATCH/logs/<project_name>`).

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

## The `[tool.cluv]` config

Everything below except `data_source` can be overridden per cluster under
`[tool.cluv.clusters.<name>]`.

```toml
[tool.cluv]
results_path = "$SCRATCH/logs/cluv"     # required — where job results live/are fetched from
results_symlink = "logs"                # local symlink name, defaults to "logs"
job_script_path = "scripts/job.sh"      # default script for `cluv submit`
# project_dir = "$HOME/my_project"      # where the project is cloned on remotes
# data_source = "mila:/path/to/data"    # source for dataset replication (see cluv-sync)
# datasets_path = "$SCRATCH/datasets"   # per-cluster dataset destination

[tool.cluv.env]
UV_OFFLINE = "1"          # compute nodes usually have no internet

[tool.cluv.sbatch_args]
time = "3:00:00"
requeue = true

[tool.cluv.clusters.mila]
env = { UV_OFFLINE = "0" }             # Mila login/compute nodes do have internet

[tool.cluv.clusters.narval]
sbatch_args = { account = "def-someuser" }
```

Cluster names under `[tool.cluv.clusters.<name>]` must exactly match `Host` entries in
`~/.ssh/config` — cluv never edits your SSH config itself. If the user doesn't have one set up,
point them at [milatools](https://github.com/mila-iqia/milatools) to generate one.

## After init

1. `cluv login` — connect to the configured clusters.
2. `cluv sync` — push the project and run `uv sync` remotely.
3. `cluv submit <cluster>` — submit a job.

## Troubleshooting

- **No `[tool.cluv]` config found**: config is read from the nearest `pyproject.toml` walking up
  from the current directory — make sure the command runs from inside the project.
- **SSH connection errors after init**: usually a mismatch between a cluster name in
  `[tool.cluv.clusters.<name>]` and the `Host` entry in `~/.ssh/config` — they must match exactly.
