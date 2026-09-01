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
| `scripts/code_checkpointing.sh` | Clones the project at `$GIT_COMMIT` onto each node's `$SLURM_TMPDIR` and creates the virtualenv there. Prints the directory for `uv run --directory`. |
| `scripts/train.sh` | Shared job body: code checkpointing, stage the data, set up the `torch.distributed` env, `srun` the training script. |
| `scripts/job_<cluster>.sh` | Thin per-cluster wrappers: only `#SBATCH` directives, then `exec scripts/train.sh`. |
| `scripts/sync_wandb.sh` | Uploads offline W&B runs pulled back by `cluv sync` to wandb.ai. |

The mapping from cluster to job script lives in the `pyproject.toml`:

```toml
[tool.cluv.clusters.tamia]
job_script_path = "scripts/job_tamia.sh"    # used only on tamia

[tool.cluv.clusters.rorqual]
job_script_path = "scripts/job_rorqual.sh"  # used only on rorqual
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
cluv submit tamia --time=0:20:00 -- python main.py --use_fake_data --epochs=1 \
    --limit_train_samples=8192 --limit_val_samples=2048 --batch_size=256 --logging_interval=5 \
    --model_name=vit_b_32 --no_wandb
```

That takes well under a minute once the job starts (a short `--time` also helps it get scheduled
sooner). It leaves `epoch_0.pt` / `epoch_1.pt` and one profiler trace per rank in the run's results
directory, so you can tell that training, checkpointing and profiling all worked. `--batch_size`
matches a real run (see "The real thing" below); `--limit_train_samples`/`--logging_interval` are
picked to get at least 5 `train/*` points in wandb (32 batches / `--logging_interval=5` = 7,
verified locally) without needing anywhere near a real run's sample count just for that.

Drop `--no_wandb` to also check the W&B integration. Each cluster's job script exports
`WANDB_MODE=offline` by default (`online` on mila, fir and nibi, which have internet on their
compute nodes), so the run's files land next to the checkpoints in `results_path` instead of
streaming out live:

```bash
cluv submit tamia --time=0:20:00 -- python main.py --use_fake_data --epochs=1 \
    --limit_train_samples=8192 --limit_val_samples=2048 --batch_size=256 --logging_interval=5 \
    --model_name=vit_b_32
```

### Uploading offline W&B runs

`cluv sync` pulls a run's `wandb/` directory back along with its checkpoints (`main.py` points
`wandb.init(dir=...)` at `results_path`), but an *offline* run still needs `wandb sync` to actually
upload it to wandb.ai - pulling the files back to your machine isn't the same as syncing them to the
service:

```bash
cluv sync tamia               # pull the run(s) back
scripts/sync_wandb.sh         # upload any offline runs found under results_path
```

`wandb sync` marks each run as synced after a successful upload and skips already-synced runs on
later calls, so re-running `scripts/sync_wandb.sh` after every `cluv sync` is cheap and never
re-uploads (or floods wandb.ai with) the same run twice.

### The real thing

`prepare_data.py` needs the ILSVRC2012 archives (`ILSVRC2012_img_train.tar`,
`ILSVRC2012_img_val.tar`, `ILSVRC2012_devkit_t12.tar.gz`, `md5sums`) in the `datasets_path` of the
current cluster. Two clusters already have a shared copy, so `datasets_path` just points at it:

| Cluster | `datasets_path` |
|---|---|
| mila | `/network/datasets/imagenet` |
| fir | `$HOME/projects/rrg-bengioy-ad/data/curated/imagenet` |
| anywhere else | `$SCRATCH/datasets/imagenet` (you have to put the archives there) |

Note that `cluv sync` copies datasets _through the machine you submit from_, which is fine for
CIFAR-10 (see the hydra example) but means the example's `data_source`
(`mila:/network/datasets/imagenet`) would route 150GB through it. Several clusters already have a
shared copy: point `datasets_path` at it for that cluster and run `cluv sync --no-sync-datasets`.

Then submit as usual (the job script needs the command to run after `--`):

```bash
cluv submit mila -- python main.py        # uses scripts/job_mila.sh
cluv submit first -- python main.py       # submit everywhere, keep the first job to start
```

Extracting ImageNet into `$SLURM_TMPDIR` takes 10-15 minutes (11m37s on a Mila L40S node), so a run
that has to fit in the configured 1h limit should train on a subset:

```bash
cluv submit mila -- python main.py --epochs=1 --limit_train_samples=100_000 \
    --limit_val_samples=10_000 --batch_size=256 --use_amp
```

That is a ~13 minute job end to end: extraction, then ~2500 images/second on 2 L40S.

For real training, ask for more time - the flags are forwarded straight to `sbatch`:

```bash
cluv submit fir --time=12:00:00 -- python main.py --epochs=10 --use_amp --compile=default
```

Results (checkpoints, wandb files, the slurm output) land in `results_path`, which cluv symlinks to
`logs/` in this folder:

```bash
cluv sync fir            # pull the results back
scripts/sync_wandb.sh    # upload any offline runs to wandb.ai (see above)
ls logs/fir_<job_id>/
uvx tensorboard --with=torch_tb_profiler --logdir logs
```

## Running it interactively

You can also skip `cluv submit` and run the same scripts inside an interactive job. On the Mila
cluster:

```bash
ssh -tt mila salloc --nodes=1 --ntasks=4 --gpus-per-node=l40s:4 --cpus-per-task=6 \
    --mem=64G --tmp=200G --time=02:59:00 --partition=short-unkillable

cd repos/cluv/examples/imagenet
# Prepare the dataset once per node:
srun --ntasks-per-node=1 uv run python prepare_data.py
# Then run the training script on each GPU of each node:
srun uv run python main.py
```

To open the example in VsCode on a compute node:

```bash
mila code repos/cluv/examples/imagenet --alloc --ntasks=4 --gpus-per-node=l40s:4 --mem=64G \
    --tmp=200G --time=02:59:00 --partition=short-unkillable
```

In the VsCode terminal you have to spell out the nodes/tasks explicitly, since the SLURM environment
variables aren't set there:

```bash
srun --ntasks-per-node=1 --nodes=1 uv run python prepare_data.py
srun --ntasks=4 --nodes=1 uv run python main.py
```

## Verified on

Every row below is a real job submitted with `cluv submit` from this branch (no job script named on
the command line - the per-cluster `job_script_path` picks it), with W&B logging **enabled** end to
end: `cluv sync` pulled the run back and, for the offline-mode clusters, `scripts/sync_wandb.sh`
uploaded it to wandb.ai. GPU count is 1 on clusters that don't enforce whole-node allocation
(narrowed via `--gpus-per-node`/`--ntasks-per-node` on the command line, on top of the cluster's own
job script) - tamia and trillium-gpu do enforce it, so those keep the whole node.

| Cluster | GPUs used | Job ID | Runtime | W&B |
|---|---|---|---|---|
| mila | 2x L40S | `10286668` | 13m24s | [mila_10286668](https://wandb.ai/lebrice/cluv-imagenet-example/runs/mila_10286668) (real ImageNet) |
| tamia | 4x H100 (whole node) | `419511` | 52s | [tamia_419511](https://wandb.ai/lebrice/cluv-imagenet-example/runs/tamia_419511) |
| rorqual | 1x H100 | `19281877` | 40s | [rorqual_19281877](https://wandb.ai/lebrice/cluv-imagenet-example/runs/rorqual_19281877) |
| fir | 1x H100 | `55655345` | 8m52s | [fir_55655345](https://wandb.ai/lebrice/cluv-imagenet-example/runs/fir_55655345) |
| nibi | 1x H100 | `20120942` | 2m05s | [nibi_20120942](https://wandb.ai/lebrice/cluv-imagenet-example/runs/nibi_20120942) |
| narval | 1x A100 | `1286134` | 1m05s | [narval_1286134](https://wandb.ai/lebrice/cluv-imagenet-example/runs/narval_1286134) |
| trillium-gpu | 4x H100 (whole node) | `814260` | 1m10s | [trillium-gpu_814260](https://wandb.ai/lebrice/cluv-imagenet-example/runs/trillium-gpu_814260) |

The real-ImageNet subset run was checked on **mila**: 13m29s in total, of which 11m37s was extracting
the archives into `$SLURM_TMPDIR`.

### Not yet working: killarney, vulcan

Both clusters accept and run the job, but hit issues unrelated to this example's code or to W&B:

- **killarney**: every attempt (with a bare 1-GPU request, and with a typed `l40s:1` one) hangs on
  the very first `srun` inside the job with `step creation temporarily disabled ... Requested nodes
  are busy`, retried until the job is killed by its own time limit without ever running any Python.
  Adding `--overlap` to every `srun` in this example - the standard fix for clusters using Slurm's
  `cons_tres` select plugin, which killarney does (`SelectTypeParameters=CR_CORE_MEMORY`) - didn't
  resolve it either, so it isn't kept in the scripts. Needs deeper investigation into killarney's
  Slurm/GRES configuration than fits this PR.
- **vulcan**: `uv sync` inside the job intermittently fails within 1-2 seconds with "Network
  connectivity is disabled, but the requested data wasn't found in the cache" for a package that
  `cluv sync` had just warmed into the shared uv cache seconds earlier from the login node -
  reproduced across 4 attempts, 3 different packages, 2 different compute nodes. Looks like an
  NFS cache-visibility race between the login node and compute nodes rather than anything cluv or
  this example does.

## Notes

- **Code checkpointing.** `scripts/code_checkpointing.sh` clones the project onto each node's
  `$SLURM_TMPDIR`, checks out the `$GIT_COMMIT` that `cluv submit` exported, and runs `uv sync`
  there; the `uv run` calls in `scripts/train.sh` are then pointed at that clone with `--directory`.
  The project folder on the cluster is what `cluv sync` writes into, so without this a later
  `cluv sync` (or another `cluv submit`) could move it to a different commit while the job was still
  queued, and the job would train whatever happened to be checked out by then. `requeue = true`
  widens that window further, since a requeued job re-runs the job script. This is the same helper
  the [mila-docs example](https://github.com/mila-iqia/mila-docs/tree/master/docs/examples/advanced/imagenet)
  uses, with `cluv submit` playing the role of its `safe_sbatch`.
  - The directory it returns keeps `$SLURM_TMPDIR` *unexpanded* on purpose: that path is node-local
    and can differ between the nodes of one job, so each task has to expand it itself. That is why
    every `uv run` in `scripts/train.sh` goes through a `bash -c "..."`.
  - Because `uv sync` runs inside the clone, the dependencies are pinned along with the code. It
    needs either internet access on the compute nodes or a warm uv cache - the `cluv sync` that
    precedes every `cluv submit` already warms that cache on the cluster.
  - Nothing is copied back out of `$SLURM_TMPDIR` at the end of the job. Everything the run produces
    (checkpoints, profiler traces, wandb files) is written straight to the absolute `results_path`
    that cluv resolved for the cluster, and `main.py` refuses to start if that path did not resolve
    to an absolute one - otherwise the results would be written into the clone and deleted with it.
- Don't add `#SBATCH --output=` to the job scripts: cluv overrides it so that results land under the
  cluster's `results_path`.
- The per-cluster resource requests (CPUs, memory, GPU model) in `scripts/job_<cluster>.sh` match
  the GPU nodes of each cluster (`sinfo -o "%D %c %m %G"`); adjust them if you want a different
  share of a node.
- The job scripts ask for GPUs with `--gpus-per-node`, not `--gpus-per-task`. With
  `--gpus-per-task`, Slurm's cgroups show each task only its own GPU, and
  `torch.cuda.set_device(LOCAL_RANK)` then fails with `invalid device ordinal` in every rank but the
  first.
- Building the virtualenv on each node's local disk also avoids a performance trap: when the ranks
  run out of a virtualenv on the networked `$HOME`, they all fault the same ~2GB of torch libraries
  in at once, which on the Lustre-backed clusters stalls the job for many minutes.
- **All transforms (including `ToDtype`/`Normalize`) run on CPU, per-sample, in the dataset
  transform.** Moving the purely-elementwise ones to run once per batch on the GPU instead was
  tried and worked, but wasn't kept: for this dataset/model, CPU-side transforms aren't the
  bottleneck, so it wasn't worth the extra code path or the CUDA-stream subtleties below.
  `RandomResizedCrop`/`Resize` couldn't have moved regardless - real ImageNet images have arbitrary
  native sizes, so there's no fixed-size tensor to stack into a batch until each image has been
  resized individually - and `RandomHorizontalFlip` has its own trap if you ever try: torchvision
  v2 transforms pick their random parameters once per call, so calling one on an already-batched
  tensor flips (or doesn't) the *whole batch* together, not each sample independently (verified
  locally, not assumed).
  - Watch out if you touch the training loop's async prefetch (`data_transfer_cuda_stream`,
    following [this recipe](https://docs.pytorch.org/tutorials/intermediate/pinmem_nonblock.html)):
    the consuming (default) stream needs an explicit
    `torch.cuda.current_stream().wait_stream(data_transfer_cuda_stream)` before it's safe to touch
    the tensors the `.to(..., non_blocking=True)` copy produced on the side stream. This was
    missing and only reliably surfaced (as a CUDA assertion inside the loss computation) while
    briefly experimenting with running GPU compute on that side stream - reproduced consistently
    outside `CUDA_LAUNCH_BLOCKING=1` and consistently didn't under it, a classic sign of an actual
    race. `main.py` keeps the `wait_stream()` call even after that experiment was reverted, since
    without it the copy and its first use on the default stream aren't actually ordered.
- **The Slurm output file is streamed into the W&B run.** Right after `wandb.init()`, the master
  rank calls `run.save(f"{RESULTS_DIR}/*.out", policy="live")`, which re-uploads it every time it
  changes - so the training log is visible from the run's page (Files tab) while the job is still
  running, not just once it's done. A second call with `policy="now"` right before
  `wandb.run.finish()` closes most of the gap left by "live" not always catching up to the latest
  content before the run is torn down. Using a glob sidesteps having to know the exact filename
  (chunked or job-packed submissions use a different `--output` pattern than a single job
  does) - one that matches nothing is a silent no-op, not an error. Slurm keeps writing to that
  file for a moment after this process exits (it appends its own trailer), so the very last couple
  of lines are the one part that never makes it into the upload.

### Per-cluster quirks worked around in the config

| Cluster | Quirk | Worked around with |
|---|---|---|
| killarney | Refuses jobs submitted from a directory under `/home` | `project_dir` on `$SCRATCH` |
| trillium-gpu | `/home` isn't mounted on compute nodes | `project_dir` on `$SCRATCH` |
| trillium-gpu | Rejects `--mem` entirely (186 GiB/GPU is implicit) | no `--mem` in the job script |
| killarney, vulcan | Slurm doesn't create the parent directory of `--output`, so cluv's default `{results_path}/{cluster}_%j/slurm-%j.out` kills the job at launch | explicit `output` in `sbatch_args` |
| killarney, vulcan | `$SCRATCH` in a path handed to `sbatch` expands to *nothing*, because cluv's command is assembled so that paths are expanded by the non-login ssh shell (see below) | an `output` path relative to the job's working directory, via cluv's `logs` symlink |
| trillium-gpu | Reports `CC_CLUSTER=trillium`, and Slurm's `ClusterName` is `grillium` | `cluv submit` exports `$CLUV_CLUSTER` |
| killarney, vulcan | `$CC_CLUSTER` and `$SCRATCH` are only set in a *login* shell | same |
| trillium-gpu | The login node shadows `sbatch` with a wrapper hardcoding `--export=NONE --get-user-env`, dropping the whole submitting environment (`GIT_COMMIT`, `CLUV_CLUSTER`, ...) before the job starts | `cluv submit` also passes `--export=ALL,KEY=VALUE,...` explicitly; `sbatch` uses the last `--export` on its command line |
| trillium-gpu | `$HOME` isn't writable from compute nodes, so `uv`'s default cache dir (`$HOME/.cache/uv`) fails with "Permission denied" | `export UV_CACHE_DIR="$SLURM_TMPDIR/uv-cache"` in `scripts/job_trillium-gpu.sh` |

About that `$SCRATCH` expansion: `cluv submit` runs
`bash --login -c '<env vars> sbatch ... <args>'`, but the arguments are `shlex`-quoted and then
concatenated *into* that single-quoted string, which closes it. So `$SCRATCH` is expanded by the
non-login shell that ssh starts, not by the login shell. On most clusters `$SCRATCH` is set in both,
so this goes unnoticed; on Killarney and Vulcan it is login-shell-only, and Slurm ends up recording
`StdOut=/logs/imagenet/<jobid>.out`. Anything else that relies on `$SCRATCH` in a cluv-computed path
has the same problem on those clusters.
