# Packed, multi-job hyperparameter sweeps with `cluv sweep`

`cluv sweep` runs many hyperparameter combinations at once, packing several of them onto each
GPU instead of paying a full job's queueing/startup cost per combination — the way the
[Hydra launcher](hydra-launcher.md) does when it submits one `sbatch` job per combo.

What it adds on top of `cluv submit`:

- **`--flag=v1,v2,...` sweep syntax**: any comma-separated flag after `--` is expanded into a
  Cartesian product of combos. Every other argument is fixed and passed unchanged to every combo.
- **GPU packing**: pass `--ntasks-per-gpu` (and a GPU count via `--gres`/`--gpus`) and cluv works
  out how many combos fit on one GPU.
- **One job per GPU, not one giant job**: cluv submits as many small, identically-shaped jobs as
  needed to cover every combo, rather than one large multi-GPU job. See
  ["Why one job per GPU"](#why-one-job-per-gpu) below.
- **Resumable, per-combo checkpoint dirs**: keyed on the sweep name and the combo's own argument
  values — not on job ID or task index — so re-running the same sweep resumes matching combos
  even if they land on a different job/task this time.

## Add sweep support to your job script's Python entrypoint

Your training script needs exactly one added line: a call to `cluv.sweep.patch_argv()` before
its own argument parsing. This call is a no-op outside a sweep (including when run locally or
under a plain `cluv submit`), so the same script keeps working everywhere else:

```python title="main.py"
import cluv.sweep
from cluv.job import current_run_info  # the same function cluv submit scripts already use

def main():
    cluv.sweep.patch_argv()  # the only line a `cluv submit` script needs to add

    args = parse_args()  # sees this task's resolved combo, patched into sys.argv
    job_info = current_run_info()  # resolves to a sweep-aware run_id/results_path when sweeping
    ...
```

`current_run_info()` is the same function already used by `cluv submit` job scripts (see the
pytorch example) — there's no separate sweep run-info type to learn. When `patch_argv()` has
resolved a combo for this task, `current_run_info()` returns a `RunInfo` whose `run_id` and
`results_path` are built from the sweep name and a slug of the combo's own arguments, and whose
`command` is the resolved combo itself.

## Add a job script

The job script is the same shape as a `cluv submit` script — no changes needed:

```bash title="scripts/job.sh"
--8<-- "examples/hydra_example/scripts/job.sh"
```

## Run a sweep

```console
cluv sweep mila scripts/job.sh --name my-sweep --ntasks-per-gpu=2 --gres=gpu:h100:1 \
    -- python main.py --lr=0.01,0.001,0.0001 --seed=1,2,3
```

This expands to 9 combos (3 learning rates × 3 seeds), packs 2 per GPU (one `h100` per job), and
submits `ceil(9 / 2) = 5` jobs — the last one only needs one of its two task slots, and the other
idle-exits cleanly.

## How combo capacity is derived

One job's task-slot capacity comes from whatever sizing flags you pass:

| Flags | Capacity |
|---|---|
| none | 1 (one job per combo — the simplest default, equivalent to looping `cluv submit`) |
| `--ntasks=N` | `N` |
| `--ntasks-per-gpu=K` | `K` (1 GPU assumed) |
| `--ntasks-per-gpu=K --gres=gpu:h100:G` | `K * G` |

## Why one job per GPU

Packing every combo into one enormous multi-GPU job would be simpler to describe, but it
schedules poorly (a huge job waits far longer in the queue) and is fragile (one bad GPU takes
down the whole sweep). `cluv sweep` instead submits `ceil(n_combos / job_capacity)` small,
identically-shaped jobs — differing only in a `CLUV_SWEEP_TASK_OFFSET` environment variable set
per job — so scheduling is easy and a single failed GPU only costs its own job.

## Options

`--name`
:   Name for this sweep, used to build resumable results paths (`results_path/sweeps/<name>/<slug>`).
    Defaults to the job script's filename stem.

`--autocommit`
:   Same as [`cluv submit`'s `--autocommit`](commands.md#cluv-submit): automatically commit
    tracked changes before submitting, instead of failing on a dirty working tree.

`--max-concurrent-submissions`
:   Maximum number of `sbatch` submissions to run concurrently (default 8).

!!! note "Not yet supported"
    `cluv sweep first` (submitting to every cluster and keeping the first to start) isn't
    supported yet — pick a specific cluster.
