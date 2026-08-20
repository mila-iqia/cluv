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
By default, the [`cluv init`](../../commands.md/#cluv-init) command tries to create two job scripts
in the `scripts` folder: [`job.sh`](#jobsh) and [`safe_job.sh`](#safe_jobsh).

### `job.sh`

The simplest possible job script where it just forwards its arguments to `uv run`. It's the default
value for `job_script_path` in your config (see ["Configuring job submission"](config.md#default-job-script)
page), when no script is used in the CLI.

```bash title="scripts/job.sh"
--8<-- "scripts/job.sh"
```

### `safe_job.sh`
Guarding against the project changing under a queued job.

The simple script above runs directly out of the synced project directory. That's fine for
short, immediate jobs, but if the job sits in the Slurm queue for a while, a later [`cluv sync`](../../commands.md/#cluv-sync)
(or another [`cluv submit`](../../commands.md#cluv-submit)) could change the checked-out commit
before the job actually starts running.

The safer pattern is to clone the project into `$SLURM_TMPDIR` and explicitly check out
`$GIT_COMMIT`, so the job always runs the exact commit it was submitted with, independent of
what's currently synced in the project root:

```bash title="scripts/safe_job.sh"
--8<-- "scripts/safe_job.sh"
```

This variant also copies any existing results for `$SLURM_JOB_ID` into `$SLURM_TMPDIR` before
running (in case of requeue) and rsyncs them back to `results_path` afterwards, matching the
`{results_path}/{cluster}_%j/` layout that `SBATCH_OUTPUT` uses.
