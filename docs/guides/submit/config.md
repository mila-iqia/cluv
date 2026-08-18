# Configuring job submission

[`cluv submit`](../../commands.md#cluv-submit) reads your `pyproject.toml` to build the final `sbatch` command.
This guide explains which config fields are used, how global and per-cluster values are merged, and what is injected automatically.

## Config fields used by `cluv submit`

| Field | Scope | Purpose |
|---|---|---|
| `job_script_path` | global / per-cluster | Default job script when none is passed on the CLI |
| `project_dir` | global / per-cluster | Where the project is replicated on clusters. |
| `results_path` | global / per-cluster | Results directory to sync back to the current cluster. |
| `env` | global / per-cluster | Extra environment variables exported before `sbatch` |
| `sbatch_args` | global / per-cluster | Extra `sbatch` flags (e.g. `--time`, `--gpus`). Per-cluster, this can be a list, [one entry per configuration](#multiple-job-configurations-on-the-same-cluster) |

Per-cluster values are set under `[tool.cluv.clusters.<name>]`.

## How global and per-cluster settings merge

For both `env` and `sbatch_args`, per-cluster values are merged on top of the global defaults.
A per-cluster key **overrides** the same global key; keys present only in the global config are
kept as-is.

For example, the following config:

```toml title="pyproject.toml"
[tool.cluv]
results_path = "$SCRATCH/results"

[tool.cluv.sbatch_args]
mem = "16G"
cpus-per-task = 4
time = "4:00:00"
gpus = "1"

[tool.cluv.clusters.narval]
results_path = "$SCRATCH/results/narval"

[tool.cluv.clusters.narval.sbatch_args]
mem = "32G"             # overrides the global 16G on narval
time = "12:00:00"       # overrides global time on narval
```

When submitting to `narval`, the effective settings are:

- `sbatch_args`: `--mem=32G --cpus-per-task=4 --time=12:00:00 --gpus=1` (cluster overrides global, rest kept)
- `results_path`: `$SCRATCH/results/narval`

When submitting to any other cluster, the global values apply.

## Multiple job configurations on the same cluster

The list form of `sbatch_args` isn't limited to switching between `--account` values. Any sbatch
flags can differ between entries, so use it whenever you have several valid configurations for a
cluster and want `cluv` to try them all and keep whichever starts first. Typical cases:

- more than one allocation (through two supervisors, or a `def-` and an `rrg-` account of the same group)
- different GPU types, when one model tends to be less contended than another
- different partitions or walltime limits, when a shorter/smaller request tends to schedule sooner

```toml title="pyproject.toml"
[tool.cluv.clusters.narval]
sbatch_args = [
    { account = "rrg-bengioy-ad" },
    { account = "def-bengioy" },
]
```

The equivalent array-of-tables syntax also works, and is nicer when each entry sets several flags:

```toml title="pyproject.toml"
[[tool.cluv.clusters.narval.sbatch_args]]
account = "rrg-bengioy-ad"

[[tool.cluv.clusters.narval.sbatch_args]]
account = "def-bengioy"
time = "24:00:00"       # this allocation allows longer jobs
```

Or without touching `account` at all - here trying an A100 first, and falling back to whichever
other GPU type frees up first:

```toml title="pyproject.toml"
[tool.cluv.clusters.mila]
sbatch_args = [
    { gpus = "a100:1" },
    { gpus = "rtx8000:1" },
]
```

Each entry is merged on top of the global `[tool.cluv.sbatch_args]` independently, so flags shared
by every entry of a cluster are best kept in the global section (there is no per-cluster "shared"
section: a `sbatch_args` list *replaces* the single-flag-set form).

`cluv submit narval` then submits **one job per entry**, waits until one of them starts, and
cancels the others - exactly what [`cluv submit first`](../../commands.md#cluv-submit) does across
clusters. This is useful whenever you can't predict which configuration will be scheduled first: a
`def-` allocation often starts sooner when the group has been using a lot of compute recently, and
the same reasoning applies to a less-requested GPU type or a shorter walltime bucket.

```console
$ cluv submit narval job.sh
                                 Jobs submitted on the clusters
┏━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Cluster ┃ sbatch arguments                        ┃ Result                                       ┃
┡━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ narval  │ --account=rrg-bengioy-ad --time=3:00:00 │ bash --login -c 'sbatch --parsable           │
│         │                                         │ --chdir=$HOME/my_project                     │
│         │                                         │ --account=rrg-bengioy-ad --time=3:00:00      │
│         │                                         │ $HOME/my_project/job.sh'                     │
│         │                                         │ Job ID: 1234                                 │
├─────────┼─────────────────────────────────────────┼──────────────────────────────────────────────┤
│ narval  │ --account=def-bengioy --time=3:00:00    │ bash --login -c 'sbatch --parsable           │
│         │                                         │ --chdir=$HOME/my_project                     │
│         │                                         │ --account=def-bengioy --time=3:00:00         │
│         │                                         │ $HOME/my_project/job.sh'                     │
│         │                                         │ Job ID: 1235                                 │
└─────────┴─────────────────────────────────────────┴──────────────────────────────────────────────┘
Job 1235 on cluster narval is RUNNING. Cancelling the other jobs...
```

The "sbatch arguments" column only appears when a cluster has more than one configuration - it
shows the full flag set of that entry (config + CLI), so you can tell which one a given job used.

`cluv submit first` also takes every configuration of every cluster into account.

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
    the value cluv computes from `results_path`. This is intentional - it lets `cluv` change the
    output dir based on the cluster the job runs on. The cluster name would otherwise have to
    be hard-coded in the job script file. You will see a warning in the console if this happens.


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

```toml title="pyproject.toml"
[tool.cluv]
job_script_path = "scripts/job.sh"    # used by all clusters

[tool.cluv.clusters.narval]
job_script_path = "scripts/job_narval.sh"   # used only on narval
```

A submission without an explicit script then resolves as follows:

```console
cluv submit mila                # uses scripts/job.sh
cluv submit narval              # uses scripts/job_narval.sh
cluv submit narval new_job.sh   # uses new_job.sh, ignoring config
```

If neither a CLI script nor a configured `job_script_path` exists for the target cluster, [`cluv
submit`](../../commands.md) exits with an error. See the page ["Writing a job script"](job-scripts.md) for what the
script should contain.

Note that a per-cluster job script still has to exist **on your local machine**: [`cluv
submit`](../../commands.md#cluv-submit) reads its header to detect an `#SBATCH --output` directive
before submitting.

!!! tip "Worked example"
    [`examples/imagenet`](https://github.com/mila-iqia/cluv/tree/master/examples/imagenet) uses one
    job script per cluster: each `scripts/job_<cluster>.sh` holds only the `#SBATCH` directives for
    that cluster's node layout, then `exec`s a shared `scripts/train.sh`.
