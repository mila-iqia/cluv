# Multi-node / multi-GPU ImageNet example

A port of the [mila-docs ImageNet example](https://github.com/mila-iqia/mila-docs/tree/master/docs/examples/advanced/imagenet)
that runs on any of the clusters configured in `[tool.cluv]`, using a **different job script per
cluster**. Each cluster's `job_script_path` in the `pyproject.toml` points at its own
`scripts/job_<cluster>.sh`, so `cluv submit <cluster>` picks the right one and you never pass a job
script on the command line.

```bash
cd examples/imagenet

cluv login                              # establish the SSH connections
cluv sync mila tamia rorqual fir nibi   # replicate the project and build the virtualenv

# Smoke test on fake data, so no ImageNet archives are needed:
cluv submit tamia --time=0:20:00 -- python main.py --use_fake_data --epochs=1 \
    --limit_train_samples=8192 --limit_val_samples=2048 --batch_size=256 --model_name=vit_b_32

# The real thing:
cluv submit mila -- python main.py --epochs=1 --limit_train_samples=100_000 \
    --limit_val_samples=10_000 --batch_size=256 --use_amp

cluv sync mila                          # pull the results back into logs/
scripts/sync_wandb.sh                   # upload any offline W&B runs to wandb.ai
```

A real run needs the ILSVRC2012 archives (`ILSVRC2012_img_train.tar`, `ILSVRC2012_img_val.tar`,
`ILSVRC2012_devkit_t12.tar.gz`, `md5sums`) in the `datasets_path` of the cluster. mila
(`/network/datasets/imagenet`) and fir (`$HOME/projects/rrg-bengioy-ad/data/curated/imagenet`)
already have a shared copy that `datasets_path` points at; anywhere else you have to put them in
`$SCRATCH/datasets/imagenet` yourself. `cluv sync` copies datasets _through the machine you submit
from_, so use `cluv sync --no-sync-datasets` on the clusters that already have a copy rather than
routing 150GB through your laptop.

Extracting ImageNet into `$SLURM_TMPDIR` takes 10-15 minutes, which is why the real run above trains
on a subset to fit the 1h limit from the config. For real training, ask for more time - the flags are
forwarded straight to `sbatch`:

```bash
cluv submit fir --time=12:00:00 -- python main.py --epochs=10 --use_amp --compile=default
```
