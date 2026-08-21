---
name: cluv-status
description: Check live cluster GPU/storage availability and cluv-submitted job status across HPC clusters with cluv status. Use when the user wants an overview of their clusters or jobs, or asks why a cluster shows as disconnected.
---

# Checking status with `cluv status`

```bash
cluv status              # both tables
cluv status clusters     # just the clusters table
cluv status jobs         # just the jobs table
cluv status --all-jobs   # jobs table shows every job, not just the 10 most recent
```

## The two tables

- **`clusters`**: live GPU availability (idle vs total, per GPU model including MIGs) and storage usage
  (home/scratch quota), plus counts of your running/pending/failed/completed cluv jobs there.
  GPU info comes from `savail` on Mila and `sinfo` on DRAC clusters; storage from the cluster's
  disk-quota reporting. Requires an active connection (`cluv login`) to fetch live data — a
  cluster with no connection shows as **disconnected** rather than erroring.
- **`jobs`**: jobs previously submitted with `cluv submit` (read from the local job cache),
  enriched with live Slurm status (via `sacct`), wait time, and elapsed time. Only shows jobs cluv
  itself submitted and cached — it's not a general `squeue`/`sacct` wrapper for arbitrary jobs.

`--quiet`/`-q` has no effect on `status` — it always prints its tables.

## Related

- A cluster shows disconnected here for the same reason `sync`/`submit` skip it — no active SSH
  connection. Run `cluv login <cluster>`.
- A cluster that's disabled (`cluv disable`) is also skipped when acting on "all clusters", which
  can look like a disconnect if the user forgot it was disabled — check `cluv status clusters` and
  cross-reference with any disable warnings printed by other commands.
