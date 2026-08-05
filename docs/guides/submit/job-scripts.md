# Writing a job script

[`cluv submit`](../commands.md#cluv-submit) doesn't have a special format for job scripts: it's a
plain `sbatch` script, with a few conventions that let `cluv` fill in cluster-specific details.

## The basics

A job script is a normal bash script with `#SBATCH` directives, submitted with `sbatch`:

```bash title="scripts/job.sh"
--8<-- "scripts/job.sh"
```

`cluv` invokes it roughly as:

```console
sbatch --chdir=<remote_project_dir> [sbatch-args] <job_script> [program_args...]
```

- The script runs with its working directory set to the project root on the target cluster
  (`--chdir`), so relative paths inside the script resolve from there.
- Anything passed after `--` on the `cluv submit` command line is forwarded as positional
  arguments (`$@`) to the script. The script above just forwards them to `uv run`, so
  `cluv submit mila scripts/job.sh -- python main.py --lr 0.01` runs
  `uv run python main.py --lr 0.01` on the cluster.

## Where the script needs to exist

The path you pass to `cluv submit` (or configure via `job_script_path`) must exist **locally**,
relative to the project root — `cluv` reads it locally to check for a conflicting `--output`
directive (see below) before submitting. The remote `sbatch` call then references the same
relative path under the project's remote directory, so the script must also be present there,
which `cluv sync` takes care of as part of `cluv submit`.

## Guarding against the project changing under a queued job

The simple script above runs directly out of the synced project directory. That's fine for
short, immediate jobs, but if the job sits in the Slurm queue for a while, a later `cluv sync` (or
another `cluv submit`) could change the checked-out commit before the job actually starts
running.

The safer pattern is to clone the project into `$SLURM_TMPDIR` and explicitly check out
`$GIT_COMMIT`, so the job always runs the exact commit it was submitted with, independent of
what's currently synced in the project root:

```bash title="scripts/safe_job.sh"
--8<-- "scripts/safe_job.sh"
```

This variant also copies any existing results for `$SLURM_JOB_ID` into `$SLURM_TMPDIR` before
running (in case of requeue) and rsyncs them back to `results_path` afterwards, matching the
`{results_path}/{cluster}_%j/` layout that `SBATCH_OUTPUT` uses.

## Summary

- It's a regular `sbatch` script — no special cluv syntax.
- Forward `"$@"` to your program so arguments after `--` reach it.
- Don't set `#SBATCH --output`; `cluv` manages it via `results_path`.
- Use `$GIT_COMMIT` if you want the job to check out a specific commit, which matters most for
  jobs that might spend a while queued.
