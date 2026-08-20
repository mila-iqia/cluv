---
name: cluv-sync
description: Push a UV-based project (and optionally its datasets) from the local machine to every configured HPC cluster with cluv, running git push/clone/checkout/pull and uv sync remotely. Use when the user wants to sync/update code on clusters, or set up dataset replication.
---

# Syncing a project across clusters with cluv

```bash
cluv sync                       # sync every cluster with an active connection (see cluv login)
cluv sync mila narval           # sync specific clusters only
cluv sync --no-sync-datasets    # skip dataset replication for this run
```

## What it does, per cluster

1. `git push` locally (once, before touching any remote).
2. On each remote cluster, in sequence for that cluster (clusters run in parallel via
   `asyncio.gather`, each cluster's own steps run in order):
   - `git clone` the project if it isn't there yet, otherwise `git fetch`.
   - `git checkout` + `git pull` to the current commit.
   - `uv sync` to update the remote virtualenv/dependencies.
3. `rsync` any new results back from `results_path` on the remote to the local results
   dir (the `logs` symlink from `cluv init`), for any cluster this machine has synced from before.

Only clusters with an **active SSH connection** are touched — run [[cluv-clusters]] (`cluv login`)
first if a cluster hasn't been connected to yet. `sync` deliberately does not trigger new 2FA
prompts itself (see `get_remote_without_2fa_prompt` in [[cluv-clusters]]); it operates on whatever
is already connected.

## Dataset replication

Enabled by default when `data_source` is configured in `[tool.cluv]`:

```toml
[tool.cluv]
data_source = "mila:/network/datasets/cifar10.var/cifar10_torchvision"   # "host:/path", or a local path
datasets_path = "$SCRATCH/datasets/cifar10"                               # destination on each cluster

[tool.cluv.clusters.killarney]
datasets_path = "$HOME/datasets/cifar10"    # per-cluster override
```

- If `data_source` has a `hostname:` prefix, cluv first pulls the dataset from that remote cluster
  to the local/current machine, then pushes it out to every configured cluster's `datasets_path`.
- If `data_source` is a plain local path (no `hostname:` prefix), the remote-pull step is skipped
  and the local directory is pushed directly to every cluster.
- Make sure the `data_source` cluster (if remote) is logged in — `cluv login mila` — before syncing.
- Skip it for a single run with `cluv sync --no-sync-datasets` without removing the config.

## Things to know

- `sync` is a one-way copy for results: it fetches from clusters to local, but never deletes
  anything on either side. Deleting a run locally and re-syncing just re-downloads it — see
  [[cluv-clean]] for actually removing stale results from clusters.
- `cluv submit` calls the same sync logic before submitting a job, so the remote is always
  up to date with the commit being submitted — no need to `cluv sync` manually right before
  `cluv submit`.
- If a cluster was never synced, `clean` (see [[cluv-clean]]) can't safely determine what's
  deletable there yet — the first `cluv sync` for a cluster establishes that baseline.

## Troubleshooting

- **Nothing happens for a cluster**: it likely has no active connection. Run `cluv login <cluster>`
  first, or check `cluv status` / the disabled-clusters warning (see [[cluv-clusters]]).
- **Dirty working tree on the remote**: sync expects the remote checkout to be a plain clone of
  this repo; uncommitted changes made directly on the cluster can make `git checkout`/`pull` fail.
  Commit or discard changes locally and let `sync` push them, rather than editing on the cluster.
