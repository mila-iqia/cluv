# Writing a job script

[`cluv submit`](../../commands.md#cluv-submit) doesn't have a special format for job scripts:
it's a plain bash script, with `#SBATCH` directives, submitted as-is via
[`sbatch`](https://slurm.schedmd.com/sbatch.html). `cluv` just adds a few conventions on top so it
can fill in cluster-specific details.

## Job script conventions

On the remote cluster, [`cluv submit`](../../commands.md#cluv-submit) invokes the job script roughly
as:

```console
sbatch --chdir=<remote_project_dir> [sbatch-args] <job_script> [program_args...]
```

- The script runs with its working directory set to the project root on the target cluster (`--chdir`),
so relative paths inside the script resolve from there.

- Anything passed after `--` on the `cluv submit` command line is forwarded as positional arguments
(`$@`) to the script, which  just forwards them to `uv run`.

`cluv submit mila scripts/job.sh -- python main.py --lr 0.01` runs `uv run python main.py --lr 0.01`
on the cluster. In case of a custom job script, make sure to forward the arguments to your
program, e.g. `"$@"` in bash.


## Cluv job scripts
The [`cluv init`](../../commands.md/#cluv-init) command adds these job scripts to the `scripts`
folder:

- [`run.sh`](#runsh): the body shared by all the job scripts below. It checks out the commit you
  submitted and runs your command with `uv run`.
- [`job.sh`](#jobsh): the default job script (`job_script_path` in your config, see
  ["Configuring job submission"](config.md#default-job-script)), for the clusters that allocate
  cores and memory: Mila, Narval, Rorqual, Fir, Nibi, Killarney and Vulcan.
- [`job_<cluster>.sh`](#job_clustersh): one script for each cluster that allocates whole nodes, or
  rejects `--mem` (TamIA, Trillium and Trillium GPU). The config that `cluv init` writes sets
  `job_script_path` to these scripts on these clusters.

The `job*.sh` scripts only hold the `#SBATCH` resources of their clusters, then run `run.sh`. To
change the resources, edit them, or override them when submitting:

```console
cluv submit mila --gpus-per-node=1 --time=2:00:00 -- python main.py
```

### `job.sh`

```bash title="scripts/job.sh"
--8<-- "scripts/job.sh"
```

### `job_<cluster>.sh`

```bash title="scripts/job_tamia.sh"
--8<-- "scripts/job_tamia.sh"
```

```bash title="scripts/job_trillium.sh"
--8<-- "scripts/job_trillium.sh"
```

```bash title="scripts/job_trillium-gpu.sh"
--8<-- "scripts/job_trillium-gpu.sh"
```

### `run.sh`
Guarding against the project changing under a queued job.

The project folder on the cluster follows every [`cluv sync`](../../commands.md/#cluv-sync), so if
the job sits in the Slurm queue for a while, a later `cluv sync` (or another
[`cluv submit`](../../commands.md#cluv-submit)) could change the checked-out commit before the job
starts.

`cluv submit` exports `$GIT_COMMIT`, the commit it synced to the cluster. `run.sh` clones the project
at that commit into `$SLURM_TMPDIR` (the local disk of each node), creates the virtual environment
there with `uv sync`, and runs your command from there. The job always runs the exact commit it was
submitted with. Without `$GIT_COMMIT` (a job submitted with `sbatch` directly), it runs from the
project folder and prints a warning.

```bash title="scripts/run.sh"
--8<-- "scripts/run.sh"
```

!!! note
    The `uv sync` in `$SLURM_TMPDIR` needs either internet access on the compute nodes or a warm uv
    cache. The `cluv sync` that runs before every `cluv submit` already warms that cache.
    Results should go to `results_path` (see `cluv.job.get_results_path()`), not inside
    the project, since the clone in `$SLURM_TMPDIR` is deleted at the end of the job.
