# Getting started

This guide shows how to set up a project with [`cluv init`](commands.md#cluv-init) and how to
adapt the generated config and job scripts to your clusters and your workload.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed on your local machine.
- An account on at least one Slurm cluster (e.g. Mila, or a DRAC cluster like Rorqual or Narval).
- SSH access to clusters through an entry in your `~/.ssh/config`, with ControlMaster enabled. The
  easiest way to set this up is with [`mila init`](https://github.com/mila-iqia/milatools).
- A git repository for your project, hosted on GitHub. If it's private, each cluster needs SSH
  access to GitHub (see [Troubleshooting](troubleshooting.md)).

Then install `cluv`:

```console
uv tool install cluster-uv
```

!!! note
    `cluv` is not supported on Windows. Use WSL2 instead.

## Run `cluv init`

Generate a default config and job scripts for your project with `cluv init`. Pass the path of your
project, which must be somewhere under your home directory (`$HOME`). The folder is created if it
doesn't exist yet:

```console
cluv init ~/my-project
```

Without a path, `cluv init` sets up the current directory.

This adds a default `[tool.cluv]` section to your `pyproject.toml` and job script templates in
`scripts/`. It only creates what's missing, so it's safe to re-run. **Read the output carefully**:
every warning points to something you'll probably want to fix in the next section.

## Adapt the config to your needs

The generated `[tool.cluv]` section is the config for a default Mila researcher. It's a good
starting point, but some of its defaults are specific to Mila only. Open `pyproject.toml` and go
through the steps below. See the [Config reference](reference/config.md) for every option.

### Keep only your clusters

Each `[tool.cluv.clusters.<name>]` table declares a cluster that cluv can log into, sync to and
submit to. The `<name>` must match a `Host` in your `~/.ssh/config`. The template lists Mila and
the DRAC and PAICE clusters. An empty table is enough to enable a cluster with the global defaults:

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

Some clusters require an `--account` for every job. The template uses Mila's allocations
(`rrg-bengioy-ad`, `def-bengioy`), which are available to core Mila students. Adjust them to the
allocations you actually have access to:

- **Add** your PI's accounts (`def-*` or `rrg-*` for DRAC clusters, `aip-*` for PAICE clusters).
- **Remove** the Mila accounts if you don't have access to them.

When you have access to multiple accounts on a cluster, you can list them all in the config. `cluv submit` will submit one job per allocation and keep the first one that starts.

For example, a core Mila student whose PI also has an allocation on Rorqual:

```toml title="pyproject.toml"
[tool.cluv.clusters.rorqual]
sbatch_args = [{ account = "rrg-bengioy-ad" }, { account = "def-yourpi" }]
```

Setting accounts in the config is optional. You can leave them out and pass one when submitting a
job, like any other `sbatch` flag:

```console
cluv submit rorqual --account=def-yourpi -- python main.py
```

### Set default job resources

Anything under `[tool.cluv.sbatch_args]` is passed to `sbatch` on every cluster (keys are `sbatch`
options without the `--`). The template only sets `requeue = true`, so that preempted jobs go back
in the queue.

Add the resources most of your jobs use, and override them per cluster when needed:

```toml title="pyproject.toml"
[tool.cluv.sbatch_args]
requeue = true
time = "4:00:00"
mem = "16G"
gpus = "1"

[tool.cluv.clusters.narval]
sbatch_args = { account = "def-yourpi", time = "12:00:00" }
```

Flags given on the command line always take precedence.
See [Configuring job submission](guides/submit/config.md) for the full merge rules.

### Set environment variables

`[tool.cluv.env]` sets environment variables for the jobs. The defaults assume compute nodes have
no internet access, so `uv` and Weights & Biases run offline. The template overrides
them on clusters where compute nodes do have internet (Mila, Fir and Nibi):

```toml title="pyproject.toml"
[tool.cluv.env]
UV_OFFLINE = "1"
WANDB_MODE = "offline"

[tool.cluv.clusters.mila]
env = { UV_OFFLINE = "0", WANDB_MODE = "online" }
```

### Choose where results and code go

You can customize where your jobs run and where their outputs are written. The template sets defaults
that work for Mila and the DRAC clusters, but you may need to change them for other clusters.

- `results_path`: where job outputs are written on each cluster. `cluv sync` copies new results back
  from there. When you run cluv on your own machine, `[tool.cluv.local]` sets `$SCRATCH` to
  `$HOME/scratch` so the same path works locally.
- `results_symlink`: the name of the symlink in your project that points to `results_path`, so you
  can browse results from the project folder.
- `project_dir`: where the project is cloned on each cluster. Some clusters don't allow jobs to run
  from `$HOME` (e.g. Killarney, Trillium), so the template puts the project in `$SCRATCH/repos/cluv`
  there.

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

- **`job.sh`**: the default job script, which is used when you don't specify one.Runs directly
  from the synced project folder. Good for short and simple jobs that start quickly.
- **`safe_job.sh`**: clones the exact commit you submitted into `$SLURM_TMPDIR` before running, so
  a later `cluv sync` can't change the code under a job that is still queued. Prefer this one for
  long-queued or long-running jobs.

Keep these two as simple references. When your jobs need something more specific (more resources,
modules, setup steps, ...), add a new job script rather than editing them. Make sure it forwards its 
arguments to your program with `"$@"`, then pass it to `cluv submit` or make it the default:

```toml title="pyproject.toml"
[tool.cluv]
job_script_path = "scripts/train.sh"
```

Clusters often differ (GPU types, number of GPUs per node, available modules, ...), so you can
also set a different job script per cluster:

```toml title="pyproject.toml"
[tool.cluv.clusters.mila]
job_script_path = "scripts/job_mila.sh"

[tool.cluv.clusters.rorqual]
job_script_path = "scripts/job_rorqual.sh"
```

`cluv submit <cluster>` then picks the right script automatically. The
[ImageNet example](https://github.com/mila-iqia/cluv/tree/master/examples/imagenet) uses this to run
a multi-node, multi-GPU training on every cluster: it's a good place to start for your own scripts.

See [Writing a job script](guides/submit/job-scripts.md) for the conventions cluv relies on.

## Commit and try it out

cluv deploys your project through git, so the remote clusters only see what's been pushed:

```console
git add pyproject.toml scripts/
git commit -m "Add cluv config"
git push
```

!!! note
    `cluv submit` refuses to submit a job when your working directory has uncommitted changes
    (untracked files are ignored), so that every job can be traced back to the exact commit it ran
    with. Commit your changes first, or pass `--autocommit` to let cluv commit them for you.

Check that everything works end to end:

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
