---
name: cluv-sync
description: Push a UV-based project (and optionally its datasets) from the local machine to every connected HPC cluster with cluv sync — git push/clone/checkout, remote uv sync, and rsync of results back. Use when the user wants to update code on clusters, fetch back results, or configure dataset replication.
---

# Syncing a project across clusters

```bash
cluv sync                       # sync every cluster with an active connection
cluv sync mila narval            # sync specific clusters only
cluv sync --no-sync-datasets     # skip dataset replication for this run
```

## What it does, per cluster

1. `git push` locally, once, before touching any remote.
2. On each remote cluster (clusters run concurrently; each cluster's own steps run in order):
   - `git clone` the project if it isn't there yet, otherwise `git fetch`.
   - `git checkout` + pull to the current local commit.
   - `uv sync` to update the remote virtualenv/dependencies.
3. `rsync` any new results back from that cluster's `results_path` into the local results dir
   (the `logs` symlink created by `cluv init`).

Only clusters with an **active SSH connection** are touched — run `cluv login` first for any
cluster that isn't connected yet. `sync` deliberately never triggers a new 2FA prompt itself: it
only acts on connections that already exist.

## Dataset replication

Configure a shared dataset source and per-cluster destination in `pyproject.toml`:

```toml
[tool.cluv]
data_source = "mila:/network/datasets/cifar10.var/cifar10_torchvision"   # hostname:/path, or a local path
datasets_path = "$SCRATCH/datasets/cifar10"                              # destination on each cluster

[tool.cluv.clusters.killarney]
datasets_path = "$HOME/datasets/cifar10"   # override for one cluster
```

- `data_source` as `hostname:/path` means cluv pulls the dataset from that cluster first (make sure
  you're logged into it), then pushes it to every target cluster.
- `data_source` as a plain path (no `hostname:` prefix) is read directly from the local machine —
  no pull step, straight push to every cluster.
- Enabled by default whenever `data_source` is set; skip it for one run with `--no-sync-datasets`.

## Troubleshooting

- **A cluster wasn't synced**: it needs an active connection first — run `cluv login <cluster>`.
- **New results aren't showing up locally**: re-run `cluv sync`; results only come back via the
  rsync step of a sync, they aren't pushed proactively by the remote job.
- **Dataset sync did nothing**: `data_source` must be set in `[tool.cluv]`; if it's a remote
  `hostname:/path`, you also need a live connection to that source cluster.
