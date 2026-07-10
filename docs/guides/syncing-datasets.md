# Syncing datasets across clusters

`cluv sync` can also replicate datasets to every cluster listed in your Cluv config.

## 1) Configure dataset sync in `pyproject.toml`

Add `data_source` and `datasets_path` under `[tool.cluv]`:

```toml
[tool.cluv]
# Source cluster and source path (`hostname:/path`).
data_source = "mila:/network/datasets/cifar10.var/cifar10_torchvision"

# Destination path used on each cluster (can use env vars like $SCRATCH).
datasets_path = "$SCRATCH/datasets/cifar10"
```

- `data_source` is where Cluv pulls the dataset from.
- `datasets_path` is where Cluv stores it on each cluster.

You can override `datasets_path` per cluster:

```toml
[tool.cluv.clusters.killarney]
datasets_path = "$HOME/datasets/cifar10"
```

### Using a local path as the data source

If your dataset is already on the machine where you run `cluv sync`, you can point
`data_source` directly at a local path (no `hostname:` prefix):

```toml
[tool.cluv]
# Local path — no hostname prefix.
data_source = "/data/datasets/cifar10"

# Destination path on each cluster.
datasets_path = "$SCRATCH/datasets/cifar10"
```

Cluv will skip the remote-pull step and push the local directory straight to every
configured cluster.

## 2) Login to clusters (including the source)

Before syncing, create reusable SSH connections:

```console
cluv login
```

If your source is `mila:...`, make sure the `mila` connection exists (for example `cluv login mila`).

## 3) Run sync

```console
cluv sync
```

With dataset sync enabled (default), Cluv will:

1. Pull data from `data_source` to your local/current cluster `datasets_path` (remote source only).
2. Push that dataset to each target cluster at its configured `datasets_path`.

## Optional: skip dataset replication for one run

```console
cluv sync --no-sync-datasets
```
