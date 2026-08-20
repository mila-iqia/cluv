---
name: cluv-status
description: Check cluster GPU/storage availability and cluv-submitted job status across HPC clusters. Use when the user wants an overview of their clusters or jobs, or asks why a cluster shows as disconnected.
---

# Checking status with cluv

```bash
cluv status              # both tables
cluv status clusters     # just the clusters table
cluv status jobs         # just the jobs table
cluv status --all-jobs   # jobs table shows every job, not just the 10 most recent
```

## The two tables

- **`clusters`**: live GPU availability and storage usage per cluster, plus counts of your
  running/pending/failed/completed cluv jobs there. Requires an active connection (see
  [[cluv-clusters]], `cluv login`) to fetch live data — a cluster with no connection shows as
  **disconnected** rather than erroring.
- **`jobs`**: jobs previously submitted with `cluv submit` (read from the local job cache),
  enriched with live Slurm status, wait time, and elapsed time. Only shows jobs cluv itself
  submitted and cached — it's not a general `squeue`/`sacct` wrapper.

## Implementation note worth knowing

As of this writing, `cluv/cli/status.py` sources its data from
`get_mock_cluster_status()` — **entirely mock/stub data**, not real live cluster queries. Treat the
displayed numbers as a placeholder for the eventual real implementation, not as ground truth about
actual cluster state. If the user is debugging based on `cluv status` output looking wrong or
suspiciously static, check whether this is still mocked before assuming a bug. Real job status
(the `jobs` table) is expected to come from Slurm `sacct`/`squeue` once wired in — mirror the same
approach `cluv submit first` already uses via `cluv.slurm.run_sacct`.

## Related

- A cluster shows disconnected here for the same reason `sync`/`submit` skip it — no active SSH
  connection. Run `cluv login <cluster>` (see [[cluv-clusters]]).
- A cluster that's disabled (see [[cluv-clusters]], `cluv disable`) is also skipped by commands
  that act on "all clusters", which can look like a disconnect if you forgot it was disabled.
