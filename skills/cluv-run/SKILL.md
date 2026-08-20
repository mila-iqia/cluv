---
name: cluv-run
description: Run a one-off command on a cluster with cluv, either through the synced project with uv (cluv run) or as a raw shell command across every connected cluster (cluv sh). Use when the user wants to run something quick on a cluster without submitting a Slurm job, e.g. checking a file, a package version, or disk usage.
---

# Running one-off commands with cluv

Two different commands, for two different needs — don't confuse them.

## `cluv run` — sync, then `uv run` on one cluster

```bash
cluv run mila python -c "import torch; print(torch.__version__)"
cluv run narval pytest tests/
```

- Syncs the project to the target cluster first (equivalent to `cluv sync`, see [[cluv-sync]]).
- Then runs the given command via `uv run` in the synced project directory on that cluster.
- Similar in spirit to running `uv run <command>` locally, but against a specific remote cluster's
  synced environment. Good for quick sanity checks against a cluster's actual dependencies/env
  without writing a Slurm job script.
- Requires an active connection to that cluster (see [[cluv-clusters]]).

## `cluv sh` — raw shell command on every connected cluster

```bash
cluv sh nvidia-smi
cluv sh du -sh $SCRATCH
cluv sh "ls -la ~/my_project"
```

- Runs the given command **as-is**, via [`clush`](https://clustershell.readthedocs.io/), on every
  cluster you currently have an active SSH connection to. If invoked from inside a cluster (a
  login node or a running job), it also runs the command locally first.
- Does **not** sync the project and does **not** wrap the command in `uv run` — unlike `cluv run`,
  it's a thin fan-out over raw shell, useful for quick multi-cluster checks (disk usage, GPU
  availability, whether a file exists) rather than anything project-specific.
- Never tries to connect to a cluster it isn't already connected to — it only targets clusters
  with a live connection, so it can never trigger a 2FA prompt. Run `cluv login` first (see
  [[cluv-clusters]]) if a cluster you want isn't included.

## Choosing between them

| Need | Use |
|---|---|
| Run project code / tests with the synced env, on one specific cluster | `cluv run <cluster> <cmd>` |
| Quick raw shell check (disk space, GPU status, file existence) across all connected clusters | `cluv sh <cmd>` |
| Actually submit a long-running/GPU job under Slurm | `cluv submit` — see [[cluv-submit]] |

Neither `cluv run` nor `cluv sh` goes through Slurm — they run directly on whatever node you're
SSH'd into (typically a login node). Don't use them for anything resource-heavy or long-running;
use [[cluv-submit]] for that.
