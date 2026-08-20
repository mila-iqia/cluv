---
name: cluv-clean
description: Remove stale run-result folders (logs, checkpoints) from HPC clusters after they've been deleted locally, using cluv clean. Use when the user wants to free up cluster storage from old runs, or asks why deleted local run folders keep reappearing after cluv sync.
---

# Cleaning up run results with cluv

`cluv sync` (see [[cluv-sync]]) only ever copies results **from** clusters **to** the local
machine — it never deletes anything on either side. That means deleting a run folder locally and
re-syncing just re-downloads it, and every job run adds another folder under `results_path`
(usually under `$SCRATCH`) on every cluster, forever, until something removes it. `cluv clean` is
that removal step.

## Usage

```bash
cluv clean --dry-run       # preview what would be deleted, nothing actually removed
cluv clean                 # clean every connected, previously-synced cluster (asks to confirm)
cluv clean rorqual narval  # only these clusters
cluv clean --force         # skip the confirmation prompt (e.g. for scripting)
```

Typical workflow:

```bash
cluv sync            # fetch results from all clusters
rm -rf logs/12345     # done with this run, delete it locally
cluv clean            # remove the matching folder wherever it ran
```

## How it decides what's safe to delete

The local results folder (the `logs` symlink from `cluv init`) is the source of truth for what to
keep. A remote folder under `results_path` is only deleted if **both**:

1. There's no folder with that name locally.
2. It's older than the last time `cluv sync` successfully pulled results from that cluster.

Condition 2 exists so a run that finished on the cluster five minutes ago (and hasn't been fetched
yet) is never mistaken for one you deliberately deleted — `clean` never removes something you
haven't seen yet locally. This is also why **a cluster needs at least one prior successful `cluv
sync` before `clean` will touch it** — clusters never synced are skipped with a note to sync
first.

## Things to know / warn the user about

- **Not job-state-aware**: `clean` doesn't check Slurm queue state before deleting. An actively
  running job usually keeps its folder looking "new" (still being written to), but this isn't
  guarded against explicitly — don't rely on it as protection for a long-idle-but-still-running job.
- **Per-cluster, independently**: the same job-id folder existing on two clusters is two separate
  decisions. Keeping the local copy keeps it on *every* cluster with a matching folder name, even
  if only one of them is "the" run you actually meant to keep.
- **Not recoverable**: deletion is a remote `rm -rf` over SSH — no trash, no undo. Always suggest
  `--dry-run` first if the user is unsure what will be removed.
- Editing files inside a local run folder (adding notes, analysis outputs) doesn't protect it —
  `clean` only checks whether the folder *exists* locally, not its modification time.

## Troubleshooting

- **"Cluster has never been synced" warning**: run `cluv sync` (or `cluv sync <cluster>`) at least
  once for that cluster before `clean` can act on it.
- **A folder I deleted is still there after `clean`**: check whether it's newer than the cluster's
  last successful sync (condition 2 above) — if so, `clean` is deliberately leaving it alone until
  it's been fetched via `cluv sync` at least once.
