# Commands

## cluv

A tool to sync UV-based Python projects across HPC clusters.

**Usage**
```console
cluv <command> [options]
```

**Commands**

[`cluv init`](#cluv-init)

Initialize a project.
{: .indent }

[`cluv login`](#cluv-login)

Establish SSH connections to clusters.
{: .indent }

[`cluv sync`](#cluv-sync)

Sync your project on clusters.
{: .indent }

[`cluv submit`](#cluv-submit)

Submit a job to clusters.
{: .indent }

[`cluv clean`](#cluv-clean)

Remove run results from clusters once they're gone locally.
{: .indent }

[`cluv status`](#cluv-status)

Show the status of clusters and jobs.
{: .indent }

[`cluv disable`](#cluv-disable)

Temporarily skip a cluster in other commands.
{: .indent }

[`cluv enable`](#cluv-enable)

Re-enable a previously disabled cluster.
{: .indent }

[`cluv run`](#cluv-run)

Run a command on a specific cluster.
{: .indent }

**Options**

Available for all commands.

`-h`, `--help`

Show the help message for the command and exit.
{: .indent }

`-v`, `--verbose`

Increase logging verbosity. Can be repeated: `-v` shows info-level logs, `-vv` (or more) shows debug-level logs. Defaults to warning-level logs only.
{: .indent }

`-q`, `--quiet`

Disable command output. Has no effect on `cluv status`.
{: .indent }

---

## [`cluv init`](#cluv-init)

Initialize a cluv project.

If the project already have a `pyproject.toml` file, it will add a `[tool.cluv]` section to the file.

If the project does not have a `pyproject.toml` file, it will create one with a `[tool.cluv]` section.

Default project structure after `cluv init`:
```
my_project/
├── README.md
├── logs -> $SCRATCH/logs/my_project   # symlink to $SCRATCH
├── pyproject.toml        # includes [tool.cluv] config
├── scripts/
│   ├── job.sh            # Slurm job script template
│   └── safe_job.sh       # Slurm job script template (copies .venv and prior results)
└── src/
    └── my_project/
        └── __init__.py
```

**Usage**
```console
cluv init [path]
```

**Arguments**

`path`

The path to use for the project. Defaults to the current working directory.
{: .indent }

---

## [`cluv login`](#cluv-login)

Create an SSH connection with the clusters. Reuse existing connections when possible.

Run this command before any command that requires a live connection (submit, sync, ...).

!!! tip
    If you don't have a SSH config to connect to the clusters, consider using [milatools](https://github.com/mila-iqia/milatools) to generate your config.

**Usage**
```console
cluv login [clusters]
```

**Arguments**

`clusters`

The clusters to connect to. If not specified, will connect to all clusters in the config. Unreachable clusters will be skipped.
{: .indent }

---

## [`cluv sync`](#cluv-sync)

Synchronize the current project across clusters.

This pushes local git commits, then on each remote cluster: clones the project (if needed),
fetches and checks out the current commit, runs `uv sync`, and fetches back any new results
via `rsync`. 

Optionally also pushes/pulls datasets, see the ["Syncing datasets across clusters"](guides/syncing-datasets.md) guide.

**Usage**
```console
cluv sync [clusters] [--sync-datasets | --no-sync-datasets]
```

**Arguments**

`clusters`

One or more cluster hostnames to synchronize with (space-separated). If omitted, synchronizes with every cluster you currently have an active SSH connection to (see [`cluv login`](#cluv-login)).
{: .indent }

**Options**

`--sync-datasets`, `--no-sync-datasets`

Push/pull datasets from `data_source` to each cluster as part of the sync. Requires `data_source` to be set in the config. Enabled by default.
{: .indent }

---

## [`cluv submit`](#cluv-submit)

Submit a Slurm job on a remote cluster.

Enforces a clean git working tree, syncs the project to the target cluster (equivalent to running
[`cluv sync`](#cluv-sync)), then runs `sbatch` on the remote, merging the global and per-cluster arguments from the config.

See the ["Configuring job submission"](guides/submit-config.md) guide for more information.

**Usage**
```console
cluv submit [options] <cluster> [<job.sh>] [sbatch-args...] [-- program-args...]
```

**Arguments**

`cluster`

The cluster to submit the job on. Can be set to `first` to submit the job to every cluster and wait until one of them starts; once one starts, the others are automatically cancelled.
{: .indent }

`job.sh`

Path to the sbatch job script, relative to the project root. Defaults to the job script configured at `job_script_path` for the target cluster.
{: .indent }

`sbatch-args` / `program-args`

Any arguments before `--` are forwarded as flags to `sbatch`. Arguments after `--` are passed to the job script itself.
{: .indent }

**Options**

`--autocommit`

Automatically create a local commit with the tracked changes before submitting, instead of failing when the working tree is dirty.
{: .indent }

---

## [`cluv clean`](#cluv-clean)

Remove run result directories from remote clusters that have been deleted from the local
results dir.

Only considers clusters that have been synced at least once. A remote run
directory is only deleted if it has no local counterpart *and* it already existed at the time of
that last sync; brand-new remote runs that were never fetched locally are left alone. Clusters
that have never been synced are skipped with a warning.

See the ["Cleaning up run results on the clusters"](guides/cleaning-runs.md) guide for more information.

**Usage**
```console
cluv clean [clusters] [-f | --force] [--dry-run]
```

**Arguments**

`clusters`

One or more cluster hostnames to clean. If omitted, cleans every cluster you currently have an active SSH connection to and that has been synced before.
{: .indent }

**Options**

`-f`, `--force`

Skip the confirmation prompt.
{: .indent }

`--dry-run`

Show what would be deleted, without deleting anything.
{: .indent }

---

## [`cluv status`](#cluv-status)

Show the status of clusters and jobs.

The `clusters` table shows each cluster's live GPU availability and storage usage, along with
counts of your running/pending/failed/completed cluv jobs on that cluster.

The `jobs` table shows jobs submitted with `cluv submit` (from the local job cache), enriched with live Slurm
status, wait time, and elapsed time.

Requires an active connection (see [`cluv login`](#cluv-login)) to fetch live data for a cluster; otherwise it is shown as disconnected.

**Usage**
```console
cluv status [table]
```

**Arguments**

`table`

Which table to display in the status output. Can be one of `jobs`, `clusters`, or `all`. Defaults to `all`.
{: .indent }

---

## [`cluv disable`](#cluv-disable)

Temporarily skip a cluster in other commands (`sync`, `submit`, `clean`, ...) without removing it
from the config.

Disabled clusters are recorded locally and are skipped automatically whenever no explicit cluster
list is given to another command. Re-enable with [`cluv enable`](#cluv-enable), or let the period
expire.

**Usage**
```console
cluv disable <cluster> [period]
```

**Arguments**

`cluster`

The cluster hostname to disable.
{: .indent }

`period`

How long to disable the cluster for. Accepts an integer (days), a Slurm-style `HH:MM:SS` / `D-HH:MM:SS` string, or suffixed values like `2h`, `1d 6h`. Omit to disable indefinitely, until [`cluv enable`](#cluv-enable) is run.
{: .indent }

---

## [`cluv enable`](#cluv-enable)

Re-enable a previously disabled cluster with [`cluv disable`](#cluv-disable).

**Usage**
```console
cluv enable <cluster>
```

**Arguments**

`cluster`

The cluster hostname to re-enable.
{: .indent }

---

## [`cluv run`](#cluv-run)

Run a command in the synced project on a cluster.

Similar in spirit to `uv run`, but syncs the project to the target cluster first (equivalent to
[`cluv sync`](#cluv-sync)) and then runs the command there via `uv run`.

**Usage**
```console
cluv run <cluster> <command>
```

**Arguments**

`cluster`

The cluster to run the command on.
{: .indent }

`command`

The command to run, along with any of its arguments.
{: .indent }
