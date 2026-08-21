---
name: cluv-submit
description: Submit a Slurm job to one or more HPC clusters with cluv submit — sbatch flag merging (global/per-cluster/CLI), racing multiple job configurations or clusters with "first", and time-chunking long jobs into checkpointed arrays. Use when the user wants to launch a training run/job, or is debugging why a submitted job used the wrong flags/output path.
---

# Submitting jobs with `cluv submit`

```bash
cluv submit mila                                       # uses the configured default job script
cluv submit mila job.sh                                 # explicit job script
cluv submit mila job.sh --time=1:00:00                   # extra/overriding sbatch flags
cluv submit mila job.sh -- python train.py --lr 0.01     # program args, forwarded to the script
cluv submit first job.sh                                 # race every cluster, keep whichever starts first
cluv submit mila job.sh --chunking                        # split into ~3h chunks (default size)
cluv submit mila job.sh --chunking=6                      # split into 6h chunks
```

Before actually submitting, `cluv submit`:

1. **Enforces a clean git working tree** — fails rather than submitting stale/uncommitted code.
   `--autocommit` creates a local commit automatically instead of failing.
2. Runs the equivalent of `cluv sync` to the target cluster(s).
3. Runs `sbatch` on the remote with the merged, resolved flags below.

## How sbatch flags are resolved

Precedence, low to high: global `[tool.cluv.sbatch_args]` → per-cluster
`[tool.cluv.clusters.<name>].sbatch_args` → CLI flags (anything before `--`).
A per-cluster key overrides the same global key; keys only set globally are kept as-is.

```toml
[tool.cluv.sbatch_args]
mem = "16G"
cpus-per-task = 4
time = "4:00:00"
gpus = "1"

[tool.cluv.clusters.narval.sbatch_args]
mem = "32G"        # overrides global 16G on narval
time = "12:00:00"  # overrides global time on narval
```

Submitting to `narval` runs with `--mem=32G --cpus-per-task=4 --time=12:00:00 --gpus=1`; any other
cluster gets the global values. CLI flags are appended last and win for options where the last
occurrence takes effect (true for most sbatch flags).

`cluv` also always injects, regardless of config:

| Variable | Value |
|---|---|
| `GIT_COMMIT` | SHA of the local `HEAD` being submitted — usable inside the job script |
| `SBATCH_JOB_NAME` | configured name, or the job script stem, prefixed with `cluv-` |
| `SBATCH_OUTPUT` | `{results_path}/{cluster}_%j/slurm-%j.out` — **overrides** any `#SBATCH --output` in the script (cluv warns when it does) |

## Job script resolution

If no script is given on the CLI, cluv uses the per-cluster `job_script_path` if set, else the
global one; if neither exists for the target cluster, submission errors out.

```toml
[tool.cluv]
job_script_path = "scripts/job.sh"
[tool.cluv.clusters.narval]
job_script_path = "scripts/job_narval.sh"
```

```bash
cluv submit mila                # -> scripts/job.sh
cluv submit narval              # -> scripts/job_narval.sh
cluv submit narval new_job.sh   # -> new_job.sh, config ignored
```

The script is a plain bash script with `#SBATCH` directives, run as
`sbatch --chdir=<remote project dir> [sbatch-args] <script> [program_args...]` — no special cluv
format. Anything after `--` on the `cluv submit` command line is forwarded as `"$@"` to the script.
`cluv init` generates two templates:

- `job.sh` — minimal, runs straight from the synced project dir. Fine for short/immediate jobs.
- `safe_job.sh` — clones into `$SLURM_TMPDIR` and checks out `$GIT_COMMIT` explicitly, so a job
  that sat a while in the queue still runs the exact commit it was submitted with, independent of
  later `cluv sync`/`cluv submit` calls. Also copies/restores results across requeues. Prefer this
  template for anything that might queue for a while.

## Multiple configurations per cluster, and `cluv submit first`

If a cluster's `sbatch_args` is a **list** instead of a single table, `cluv submit <cluster>`
submits one job per entry, waits until one starts, and cancels the rest:

```toml
[tool.cluv.clusters.narval]
sbatch_args = [
    { account = "rrg-bengioy-ad" },
    { account = "def-bengioy" },
]
```

Useful whenever you can't predict which allocation/GPU-type/partition will schedule first —
multiple `--account`s, GPU type fallbacks (`gpus = "a100:1"` vs `"rtx8000:1"`), or a
shorter-walltime fallback. Each entry is merged independently on top of the *global*
`[tool.cluv.sbatch_args]` (there's no separate per-cluster "shared" section once you switch to the
list form).

`cluv submit first job.sh` generalizes this **across clusters**: submits to every cluster (and
every configuration of every cluster), waits for the first job to actually start running, then
cancels all the others.

## Chunking long jobs

```bash
cluv submit mila job.sh --chunking      # default chunk size: 3 hours
cluv submit mila job.sh --chunking=6    # 6-hour chunks
```

Splits the requested walltime into a Slurm job array of consecutive chunks, replacing the plain
`--time`. The time limit is looked up from (in order) the CLI/config `--time`/`-t`, the
`SBATCH_TIMELIMIT` env var (config or cluster), or a `#SBATCH --time` directive in the job script
header — one of these must resolve to a value or chunking errors out. **The job itself must
implement checkpointing/restart** — cluv just
re-launches the script for each array index, it doesn't checkpoint anything for you. Mention this
before recommending `--chunking` if the user's script doesn't already checkpoint.

## Troubleshooting

- **"dirty working tree" error**: commit or stash changes, or pass `--autocommit`.
- **Job used the wrong `--output` path**: expected — `SBATCH_OUTPUT` always overrides a
  `#SBATCH --output` directive in the script; remove the directive instead of fighting it.
- **No job script found for cluster**: no CLI script and no `job_script_path` (global or
  per-cluster) resolves — set one in config or pass the script explicitly.
- **Chunking fails to find a time value**: set `--time` or a `#SBATCH --time` header in the script
  before adding `--chunking`.
