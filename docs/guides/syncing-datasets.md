# Syncing datasets across clusters

[`cluv sync`](../commands.md#cluv-sync) can also replicate datasets to every cluster listed in your Cluv config.

## Configure dataset sync in `pyproject.toml`

Add `data_source` and `datasets_path` under `[tool.cluv]`:

```toml title="pyproject.toml"
[tool.cluv]
# Source cluster and source path (`hostname:/path`).
data_source = "mila:/network/datasets/cifar10.var/cifar10_torchvision"

# Destination path used on each cluster (can use env vars like $SCRATCH).
datasets_path = "$SCRATCH/datasets/cifar10"
```

- `data_source` is where Cluv pulls the dataset from.
- `datasets_path` is where Cluv stores it on each cluster, except the source cluster (see
  [Run sync](#run-sync)).

You can override `datasets_path` per cluster:

```toml title="pyproject.toml"
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


## Login to clusters (including the source)

Before syncing, create reusable SSH connections:

```console
cluv login
```

If your source is `mila:...`, make sure the `mila` connection exists (for example `cluv login mila`).

## Run sync

```console
cluv sync
```

With dataset sync enabled (default), Cluv will:

1. Pull data from `data_source` to the `datasets_path` of the machine where you run Cluv (remote
   source only).
2. Push that dataset to each target cluster at its configured `datasets_path`.

Cluv skips the copies that aren't needed:

- It doesn't push the data to the source cluster. Jobs there read the data at the `data_source`
  path directly: on the source cluster, `cluv.job.get_datasets_path()` returns that path instead of
  `datasets_path`.
- It only pulls the data when a target other than the source cluster needs it. The cluster where you
  run Cluv counts as a target when you name it, when you run `cluv sync` without naming any cluster,
  or when you run `cluv submit first`.

For example, with `data_source = "mila:/network/datasets/imagenet"`:

- `cluv submit mila`, run from your laptop or from narval, copies no data.
- `cluv submit narval`, run on narval, pulls the data to narval.
- `cluv sync narval`, run on mila, pushes the data from `/network/datasets/imagenet` to narval.

## Optional: skip dataset replication for one run

If you need to skip the dataset replication step, specify:
```console
cluv sync --no-sync-datasets
```
