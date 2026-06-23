# Configuring job submission with `cluv submit`

`cluv submit` reads your `pyproject.toml` to build the final `sbatch` command. This guide explains
which config fields are used, how global and per-cluster values are merged, and what is injected
automatically.

## Config fields used by `cluv submit`

| Field | Scope | Purpose |
|---|---|---|
| `job_script_path` | global / per-cluster | Default job script when none is passed on the CLI |
| `results_path` | global / per-cluster | Controls `SBATCH_OUTPUT` (where Slurm writes stdout/stderr) |
| `env` | global / per-cluster | Extra environment variables exported before `sbatch` |
| `sbatch_args` | global / per-cluster | Extra `sbatch` flags (e.g. `--time`, `--gpus`) |

Per-cluster values are set under `[tool.cluv.clusters.<name>]`.

## How global and per-cluster settings merge

For both `env` and `sbatch_args`, per-cluster values are merged on top of the global defaults.
A per-cluster key **overrides** the same global key; keys present only in the global config are
kept as-is.

```toml
[tool.cluv]
results_path = "$SCRATCH/results"

[tool.cluv.env]
SBATCH_MEM_PER_NODE = "16G"
SBATCH_CPUS_PER_TASK = "4"

[tool.cluv.sbatch_args]
time = "4:00:00"
gpus = "1"

[tool.cluv.clusters.narval]
results_path = "$SCRATCH/results/narval"

[tool.cluv.clusters.narval.env]
SBATCH_MEM_PER_NODE = "32G"           # overrides the global 16G on narval

[tool.cluv.clusters.narval.sbatch_args]
time = "12:00:00"             # overrides global time on narval
```

When submitting to `narval`, the effective settings are:

- `env`: `SBATCH_MEM_PER_NODE=32G`, `SBATCH_CPUS_PER_TASK=4` (cluster overrides global, rest kept)
- `sbatch_args`: `--time=12:00:00 --gpus=1` (cluster overrides global, rest kept)
- `results_path`: `$SCRATCH/results/narval`

When submitting to any other cluster, the global values apply.

## What cluv injects automatically

Regardless of your config, `cluv submit` always sets these variables before calling `sbatch`:

| Variable | Value |
|---|---|
| `GIT_COMMIT` | SHA of the current local `HEAD` commit |
| `SBATCH_JOB_NAME` | Your configured name (or the job script stem) prefixed with `cluv-` |
| `SBATCH_OUTPUT` | `{results_path}/{cluster}_%j/slurm-%j.out` |

`GIT_COMMIT` is available inside your job script, so you can use it to tag results or check out
the exact commit that was running.

!!! note "`SBATCH_OUTPUT` overrides `#SBATCH --output` in your script"
    If your job script contains an `#SBATCH --output` directive, it will be silently overridden by
    the value cluv computes from `results_path`. This is intentional — it lets `cluv sync` know
    where to fetch results. You will see a warning in the console if this happens.

## Variables priority

Settings can reach `sbatch` through multiple levels. When the same option is set at more than one
level, the following order applies — **higher rows win**:

| Priority | Source | Mechanism |
|---|---|---|
| 1 | `cluv submit` CLI flags (e.g. `--time=1:00:00`) | sbatch CLI flag — appended last |
| 2 | `[tool.cluv.clusters.<name>.sbatch_args]` | sbatch CLI flag — prepended before CLI flags |
| 3 | `[tool.cluv.sbatch_args]` | sbatch CLI flag — base, overridden by cluster |
| 4 | cluv auto-injected (`SBATCH_OUTPUT`, `SBATCH_JOB_NAME`) | `SBATCH_*` env var — set after merging user env |
| 5 | `[tool.cluv.clusters.<name>.env]` | `SBATCH_*` env var — merged over global env |
| 6 | `[tool.cluv.env]` | `SBATCH_*` env var — global baseline |
| 7 | `#SBATCH` directives inside the job script | Slurm script directive |

Two important things to understand about this table:

**`sbatch_args` vs `env` are different mechanisms with different Slurm priorities.** Slurm itself
resolves settings in the order: sbatch CLI flags > `SBATCH_*` env vars > `#SBATCH` script
directives. This means a `time = "4:00:00"` entry in `sbatch_args` (rows 1–3) will always win
over an `SBATCH_TIMELIMIT` entry in `env` (rows 4–6) for the same setting, regardless of where
each was configured.

**Cluv always controls `SBATCH_OUTPUT` and `SBATCH_JOB_NAME`.** These are set unconditionally
after merging your `env` config (row 4), so they cannot be overridden through `[tool.cluv.env]`
or `[tool.cluv.clusters.<name>.env]`. For `SBATCH_JOB_NAME`, cluv uses your configured value as
the *base* and prepends `cluv-` to it — you can influence the name but not remove the prefix.
Any `#SBATCH --output` directive in your job script will also be silently overridden by cluv's
computed `SBATCH_OUTPUT`.

## CLI flags and program args

Extra flags passed on the command line are **appended after** the flags from config. For most
sbatch options the last occurrence wins, so CLI flags effectively override config values for a
single run.

```console
# Config sets --time=4:00:00; this run overrides it to 1:00:00
cluv submit mila job.sh --time=1:00:00

# Arguments after -- are forwarded to the job script, not to sbatch
cluv submit mila job.sh --time=1:00:00 -- python train.py --lr 0.01
```

## Default job script

If no job script is passed on the CLI, cluv uses the `job_script_path` configured for that
cluster, falling back to the global `job_script_path`.

```toml
[tool.cluv]
job_script_path = "scripts/job.sh"    # used by all clusters

[tool.cluv.clusters.narval]
job_script_path = "scripts/job_narval.sh"   # used only on narval
```

A submission without an explicit script then resolves as follows:

```console
cluv submit mila           # uses scripts/job.sh
cluv submit narval         # uses scripts/job_narval.sh
cluv submit narval job.sh  # always uses job.sh, ignoring config
```

If neither a CLI script nor a configured `job_script_path` exists for the target cluster, `cluv
submit` exits with an error.
