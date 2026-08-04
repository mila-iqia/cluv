# Multi-node / multi-GPU ImageNet example

A port of the [mila-docs ImageNet example](https://github.com/mila-iqia/mila-docs/tree/master/docs/examples/advanced/imagenet)
that runs on any of the clusters configured in `[tool.cluv]`, using a **different job script per
cluster**.

It demonstrates:

- Multi-GPU / multi-node training with `DistributedDataParallel`
- Staging ImageNet onto each node's local disk (`$SLURM_TMPDIR`) before training
- Checkpointing and resuming (so the job survives preemption / `--requeue`)
- Weights & Biases logging (online on clusters with internet, offline elsewhere)
- Profiling with the PyTorch profiler + tensorboard
- Per-cluster job scripts via `job_script_path`, so the same code runs on clusters with very
  different node layouts

All of the commands below are run from the root of this example:

```bash
cd examples/imagenet
```

## Layout

| File | Role |
|---|---|
| `main.py` | The training script. Gets its run id and results dir from `cluv.job`. |
| `prepare_data.py` | Extracts the ImageNet archives into `$SLURM_TMPDIR`, reading them from the `datasets_path` cluv resolved for the current cluster. |
| `scripts/train.sh` | Shared job body: stage the data, set up the `torch.distributed` env, `srun` the training script. |
| `scripts/job_<cluster>.sh` | Thin per-cluster wrappers: only `#SBATCH` directives, then `exec scripts/train.sh`. |
| `scripts/job.sh` | Generic 1-node/1-GPU fallback for clusters without a wrapper of their own. |

The mapping from cluster to job script lives in the `pyproject.toml`:

```toml
[tool.cluv]
job_script_path = "scripts/job.sh"          # default for all clusters

[tool.cluv.clusters.tamia]
job_script_path = "scripts/job_tamia.sh"    # used only on tamia
```

so you never need to pass a job script on the command line - `cluv submit <cluster>` picks the right
one.

## Running it

First, replicate the project (and build the virtualenv) on the clusters you want to use:

```bash
cluv login                # establish the SSH connections
cluv sync mila tamia rorqual fir nibi
```

### Quick smoke test (no dataset needed)

`--use_fake_data` trains on `torchvision.datasets.FakeData`, and `scripts/train.sh` skips the dataset
staging step in that case. This is the fastest way to check that a cluster's job script, distributed
setup and results path all work:

```bash
cluv submit tamia -- python main.py --use_fake_data --epochs=1 \
    --limit_train_samples=2048 --limit_val_samples=512 --no_wandb --model_name=resnet18
```

### The real thing

`prepare_data.py` needs the ILSVRC2012 archives (`ILSVRC2012_img_train.tar`,
`ILSVRC2012_img_val.tar`, `ILSVRC2012_devkit_t12.tar.gz`, `md5sums`) in the `datasets_path` of the
current cluster. On the Mila cluster that is `/network/datasets/imagenet`, so there is nothing to do.
Elsewhere, `cluv sync` replicates them to `$SCRATCH/datasets/imagenet`:

```bash
cluv sync fir            # ~150GB the first time - this takes a while
```

Then submit as usual:

```bash
cluv submit mila                          # uses scripts/job_mila.sh
cluv submit fir   -- python main.py --epochs=10 --use_amp --compile=default
cluv submit first -- python main.py       # submit everywhere, keep the first job to start
```

Results (checkpoints, wandb files, the slurm output) land in `results_path`, which cluv symlinks to
`logs/` in this folder:

```bash
cluv sync fir            # pull the results back
ls logs/fir_<job_id>/
uvx tensorboard --with=torch_tb_profiler --logdir logs
```

## Running it interactively

You can also skip `cluv submit` and run the same scripts inside an interactive job. On the Mila
cluster:

```bash
ssh -tt mila salloc --nodes=1-2 --ntasks=4 --gpus-per-task=l40s:1 --cpus-per-task=4 \
    --mem=32G --tmp=200G --time=02:59:00 --partition=short-unkillable

cd repos/cluv/examples/imagenet
# Prepare the dataset once per node:
srun --ntasks-per-node=1 uv run python prepare_data.py
# Then run the training script on each GPU of each node:
srun uv run python main.py
```

To open the example in VsCode on a compute node:

```bash
mila code repos/cluv/examples/imagenet --alloc --ntasks=4 --gpus-per-task=l40s:1 --mem=32G \
    --tmp=200G --time=02:59:00 --partition=short-unkillable
```

In the VsCode terminal you have to spell out the nodes/tasks explicitly, since the SLURM environment
variables aren't set there:

```bash
srun --ntasks-per-node=1 --nodes=2 uv run python prepare_data.py
srun --ntasks=4 --nodes=2 uv run python main.py
```

## Notes

- The upstream version of this example ships a `safe_sbatch` script and a `code_checkpointing.sh`
  helper to pin the code to a commit. `cluv submit` already does that job: it refuses to submit with
  a dirty working tree, syncs the cluster to your current commit, and exports `$GIT_COMMIT` into the
  job. If you also want the job to run from a private clone in `$SLURM_TMPDIR`, see
  `examples/hydra_example/scripts/safe_job.sh`.
- Don't add `#SBATCH --output=` to the job scripts: cluv overrides it so that results land under the
  cluster's `results_path`.
- The per-cluster resource requests (CPUs, memory, GPU model) in `scripts/job_<cluster>.sh` are a
  starting point - adjust them to what each cluster actually offers.
