# Configuration

`cluv` is configured through a `[tool.cluv]` section in your project's `pyproject.toml`. The
section is created automatically by [`cluv init`](commands.md#cluv-init), but can also be added or
edited by hand.

!!! warning
    Unknown fields are rejected. `cluv` will raise an error if the `[tool.cluv]` section contains a key
    that isn't listed below.

---
## Project configuration

### [`clusters`](#clusters)
TODO : List of cluster to use with cluv.

Configured under `[tool.cluv.clusters.<name>]`, where `<name>` must match a hostname in your
`~/.ssh/config`. Default to the list of clusters of Mila studant have access, with the credentials
of the Mila global allocation.

See the "Configuring job submission" guide for more information on how the fields of `clusters` are
used when submitting a job.

#### [`datasets_path`](#datasets_path)
TODO : Overrides the global [`datasets_path`](#datasets_path-1) for this cluster.

#### [`env`](#env)
TODO : Merge on top of [`env`](#env-1).

#### [`job_script_path`](#job_script_path)
TODO : Overrides the global [`job_script_path`](#job_script_path-1) for this cluster.

#### [`project_dir`](#project_dir)
TODO : Overrides the global [`project_dir`](#project_dir-1) for this cluster.

#### [`results_path`](#results_path)
TODO : Overrides the global [`results_path`](#results_path-1) for this cluster.

#### [`sbatch_args`](#sbatch_args)
TODO : Merge on top of [`sbatch_args`](#sbatch_args-1).

---

### [`data_source`](#data_source)
TODO. Used together with [`datasets_path`](#datasets_path) when syncing datasets across clusters.
See the ["Syncing datasets across clusters"](guides/syncing-datasets.md) guide.

**Default**: `None`

**Type**: `str`

---

### [`datasets_path`](#datasets_path-1)
TODO

**Default**: `None`

**Type**: `str`

---

### [`env`](#env-1)
Global environment variables set on all clusters when running Slurm commands. 

By default, we consider that the compute nodes of a cluster don't have access to internet, and set
`uv` and `wandb` to offline mode.

**Default**: `{ UV_OFFLINE = "1", WANDB_MODE = "offline" }`

**Type**: `dict`

---

### [`job_script_path`](#job_script_path-1)
Path to the job script submitted by [`cluv submit`](commands.md#cluv-submit) when none
is passed explicitly on the command line.

**Default**: `"scripts/job.sh"`

**Type**: `str`

---

### [`local`](#local)
Settings applied when using cluv on a local machine (not on a Slurm cluster).

#### [`env`](#env-2)
Environnement variables set when running Slurm command.
By default, set a fake `$SCRATCH` directory to run the examples.

**Default**: `{ SCRATCH = "$HOME/scratch" }`

**Type**: `dict`

---

### [`results_path`](#results_path-1)
**Required.** Path to the results directory for all clusters. Use by cluv sync. May contain environment
variables (e.g. `$SCRATCH`), which are expanded on each cluster individually.

**Default**: `TODO`

**Type**: `dict`

---

### [`results_symlink`](#results_symlink)
Name of the symlink created in the project directory pointing to [`results_path`](#results_path-1).

**Default**: `"logs"`

**Type**: `str`

---

### [`sbatch_args`](#sbatch_args-1)
TODO: Global sbatch flags applied on all clusters. These are passed directly to `sbatch` when using 
[`cluv submit`](./commands.md#cluv-submit).

TODO: By default, set the duration of a job to 3h and enable automatic requeue.

**Default**: `{ time = "3:00:00", requeue = true }`

**Type**: `dict`

**Example**:
```toml 
[tool.cluv.sbatch_args]
time = "3:00:00"
requeue = true
```

## Full example
TODO