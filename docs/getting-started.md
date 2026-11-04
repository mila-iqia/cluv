# Getting started

This guide shows how to set up a project with [`cluv init`](commands.md#cluv-init) and how to
adapt the generated config and job scripts to your clusters and your workload.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed on your local machine.
- An account on at least one Slurm cluster (e.g. Mila, or a DRAC cluster like Rorqual or Narval).
- SSH access to each cluster through an entry in your `~/.ssh/config`, with
  [ControlMaster](https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing) enabled. The
  easiest way to set this up is with [`mila init`](https://github.com/mila-iqia/milatools).
- A git repository for your project, hosted on GitHub. If it's private, each cluster needs SSH
  access to GitHub (see the [Introduction](guides/introduction.md#non-goals)).

Then install `cluv`:

```console
uv tool install cluster-uv
```

!!! note
    `cluv` is not supported on Windows. Use WSL2 instead.

## Run `cluv init`

Pass the path of your project, which must be somewhere under your home directory (`$HOME`). The
folder is created if it doesn't exist yet:

```console
cluv init ~/my-project
```

Without a path, `cluv init` sets up the current directory.

This adds a default `[tool.cluv]` section to your `pyproject.toml` and job script templates in
`scripts/`. It only creates what's missing, so it's safe to re-run. **Read the output carefully**:
every warning points to something you'll probably want to fix in the next section.

## Adapt the config to your needs

The generated `[tool.cluv]` section is a starting point that lists many clusters. Open
`pyproject.toml` and go through the steps below. See the [Config reference](reference/config.md)
for every option.

### Keep only your clusters

Each `[tool.cluv.clusters.<name>]` table declares a cluster that cluv can log into, sync to and
submit to. The `<name>` must match a `Host` in your `~/.ssh/config`. An empty table is enough to
enable a cluster with the global defaults:

```toml title="pyproject.toml"
[tool.cluv.clusters.mila]

[tool.cluv.clusters.rorqual]
```

For the clusters you don't want to use (no access, missing SSH entry, ...), you have two options:

- **Remove their table** from the config.
- **Disable them** with [`cluv disable`](commands.md#cluv-disable), without touching the config:

    ```console
    cluv disable narval        # Until `cluv enable narval` is run.
    cluv disable tamia 2d      # For two days.
    ```

  Disabled clusters are skipped by every command unless you name them explicitly.

### Set your allocation accounts

Some clusters require an `--account` for every job. The template contains Mila's accounts:
**replace them with your own** (your PI's `def-*` or `rrg-*` account for DRAC clusters and `aip-*`
for PAICE clusters):

```toml title="pyproject.toml"
[tool.cluv.clusters.rorqual]
sbatch_args = { account = "def-yourpi" }
```

If you have several allocations on the same cluster, list them all: `cluv submit` will submit one
job per allocation and keep the first one that starts.

```toml title="pyproject.toml"
[tool.cluv.clusters.rorqual]
sbatch_args = [{ account = "rrg-yourpi" }, { account = "def-yourpi" }]
```

### Set default job resources

Anything under `[tool.cluv.sbatch_args]` is passed to `sbatch` on every cluster (keys are `sbatch`
long options without the `--`). Put the resources most of your jobs need here, and override them
per cluster when needed:

```toml title="pyproject.toml"
[tool.cluv.sbatch_args]
requeue = true
time = "4:00:00"
mem = "16G"
gpus = "1"

[tool.cluv.clusters.narval]
sbatch_args = { account = "def-yourpi", time = "12:00:00" }
```

Flags given on the command line (`cluv submit mila --time=1:00:00`) always take precedence. See
[Configuring job submission](guides/submit/config.md) for the full merge rules.

### Set environment variables

`[tool.cluv.env]` sets environment variables for the jobs. The defaults assume compute nodes have
no internet access, so `uv` runs offline and Weights & Biases logs offline. Override them on
clusters where compute nodes do have internet:

```toml title="pyproject.toml"
[tool.cluv.env]
UV_OFFLINE = "1"
WANDB_MODE = "offline"

[tool.cluv.clusters.mila]
env = { UV_OFFLINE = "0", WANDB_MODE = "online" }
```

### Choose where results and code go

- `results_path` (default `$SCRATCH/logs/<project-name>`): where job outputs are written on each
  cluster. `cluv sync` copies new results back from there.
- `results_symlink` (default `logs`): the name of the symlink in your project that points to
  `results_path`, so you can browse results from the project folder.
- `project_dir` (default: the same path relative to `$HOME` as on your machine): where the project
  is cloned on each cluster.
  Some clusters don't allow jobs to run from `$HOME` (e.g. Killarney, Trillium), so put the project
  on `$SCRATCH` there:

```toml title="pyproject.toml"
[tool.cluv.clusters.trillium]
project_dir = "$SCRATCH/repos/my-project"
```

All of these can be set globally or per cluster.

### Configure dataset syncing (optional)

If your jobs need a dataset, `data_source` and `datasets_path` let `cluv sync` replicate it from
one cluster to all the others. See [Syncing datasets](guides/syncing-datasets.md).

## Adapt the job scripts

`cluv init` creates two job scripts in `scripts/`. Both are plain `sbatch` scripts that run the
command you pass after `--` with `uv run`:

- **`job.sh`**: the default (set by `job_script_path`). Runs directly from the synced project
  folder. Good for short jobs that start quickly.
- **`safe_job.sh`**: clones the exact commit you submitted into `$SLURM_TMPDIR` before running, so
  a later `cluv sync` can't change the code under a job that is still queued. Prefer this one for
  long-queued or long-running jobs.

Edit the `#SBATCH` lines for resources that are specific to a script, add any setup your program
needs (modules, environment variables, ...), and make sure the arguments are forwarded to your
program with `"$@"`. To make `safe_job.sh` the default, set:

```toml title="pyproject.toml"
[tool.cluv]
job_script_path = "scripts/safe_job.sh"
```

See [Writing a job script](guides/submit/job-scripts.md) for the conventions cluv relies on.

## Commit and try it out

cluv deploys your project through git, so the clusters only see what's been pushed:

```console
git add pyproject.toml scripts/
git commit -m "Add cluv config"
git push
```

Then check that everything works end to end:

```console
cluv login                                        # Connect to every cluster (answer the 2FA prompts).
cluv sync                                         # Clone the project and run `uv sync` on each cluster.
cluv submit mila --time=0:05:00 -- python -c "print('hello')"
cluv status                                       # Follow the job.
cluv sync                                         # Copy the results back into `logs/`.
```

If something goes wrong, see [Troubleshooting](troubleshooting.md).

## Next steps

- [Configuring job submission](guides/submit/config.md): merge rules, multiple job configurations,
  and what cluv injects into every job.
- [Racing clusters with `cluv submit first`](guides/submit/racing-clusters.md).
- [Hydra launcher](hydra-launcher.md): launch Hydra sweeps on remote clusters.
