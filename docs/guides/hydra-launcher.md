# Using Cluv with Hydra

The Cluv Hydra Launcher lets you run [Hydra](https://hydra.cc/) multi-run sweeps directly on
remote Slurm clusters, using the same `pyproject.toml`-based config that drives `cluv submit`.

It is a drop-in replacement for the
[Submitit launcher plugin](https://hydra.cc/docs/plugins/submitit_launcher/) — the same
`gpus_per_node`, `cpus_per_task`, `mem_gb`, `timeout_min`, etc. parameters all work as-is.

What it adds on top of Submitit:

- **Automatic sync**: the project is synced to the target cluster before submission (via `cluv sync`).
- **Automatic result fetch**: results are rsynced back locally once jobs finish.
- **Cluster selection**: set `cluster: mila` (or any cluster in your config) to pick the target.
- **`${cluv:...}` resolver**: access live cluv job info (e.g. `results_path`) from Hydra configs.


## 1. Installation

Add the `hydra` extra when installing cluv:

```console
uv add cluv[hydra]
```

Until cluv is published on PyPI, install from the Git repository:

```console
uv add "git+https://github.com/mila-iqia/cluv[hydra]"
```

This pulls in `hydra-core`, `hydra-submitit-launcher`, `hydra-zen`, and `remote-slurm-executor`
alongside the launcher plugin.


## 2. Configure your project

Your `pyproject.toml` needs a `[tool.cluv]` section with at least a `results_path` and
the clusters you want to target. A minimal setup:

```toml
[tool.cluv]
results_path = "$SCRATCH/logs/my_project"
results_symlink = "logs"

[tool.cluv.env]
SBATCH_TIME = "3:00:00"
SBATCH_REQUEUE = "1"

[tool.cluv.clusters.mila]
env = {UV_OFFLINE="0", WANDB_MODE="online"}

[tool.cluv.clusters.narval]
env = {SBATCH_ACCOUNT="def-myallocation"}
```

See [config reference](../reference/config.md) for all available fields.


## 3. Add a job script

The launcher submits jobs using a shell script (just like `cluv submit`). The script receives
the Python command as positional arguments via `$@`:

```bash
# scripts/job.sh
#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=1:00:00

# --output is set automatically by Cluv.
srun uv run "$@"
```

!!! tip
    The `--output` flag is injected by the launcher, so you don't need it in the script.


## 4. Add the launcher config

Create a Hydra config file that selects the Cluv launcher. This is typically placed in
`configs/launcher/cluv.yaml` so it can be activated with `+launcher=cluv` on the command line:

```yaml
# configs/launcher/cluv.yaml
# @package _global_
defaults:
  - override /hydra/launcher: cluv_launcher

hydra:
  mode: MULTIRUN
  sweep:
    dir: ${cluv:results_path,/tmp/cluv_logs/${now:%Y-%m-%d}/${now:%H-%M-%S}}
    subdir: ${hydra.job.num}

  launcher:
    # --- Cluv-specific options ---
    cluster: mila             # which cluster to submit to
    job_script: scripts/job.sh
    vram_gb: 10               # enables job packing when set (future feature)
    checkpointing: false

    # --- Standard Submitit options (same as submitit_slurm) ---
    stderr_to_stdout: true
    timeout_min: 60
    gpus_per_node: 1
    cpus_per_task: 2
    mem_gb: 16
```

!!! note "`cluster: first`"
    Use `cluster: first` to automatically pick the first cluster that already has an active SSH
    connection (i.e. the first result of `cluv status`). This avoids hardcoding a cluster name.


### Migrating from the Submitit launcher

If you already have a `configs/launcher/submitit.yaml`, switching to Cluv only requires two
changes:

```yaml
# Before:
defaults:
  - override /hydra/launcher: submitit_slurm

# After:
defaults:
  - override /hydra/launcher: cluv_launcher

hydra:
  launcher:
    cluster: mila     # add this
    # everything else stays the same
```


## 5. Run a sweep

First, make sure you have active SSH connections:

```console
cluv login
```

Then launch your sweep the normal Hydra way, activating the launcher with `+launcher=cluv`:

```console
python main.py -m +launcher=cluv lr=0.01,0.001 seed=1,2,3
```

The launcher will:

1. Sync your project to the target cluster (`cluv sync`).
2. Submit one `sbatch` job per config combination.
3. Monitor jobs until all complete.
4. Rsync results back to your local `results_symlink` directory.


## 6. The `${cluv:...}` resolver

The launcher registers a custom OmegaConf resolver so Hydra configs can read live cluv job info:

```
${cluv:<attribute>,<default>}
```

| Attribute | Description |
|-----------|-------------|
| `results_path` | The resolved results path for the current job |
| `cluster` | Name of the cluster the job is running on |
| `run_id` | Unique run identifier (`{cluster}_{job_id}_{task_id}`) |

Example — point Hydra's output dir to the cluv-managed results directory:

```yaml
hydra:
  sweep:
    dir: ${cluv:results_path,/tmp/cluv_logs/${now:%Y-%m-%d}/${now:%H-%M-%S}}
    subdir: ${hydra.job.num}
```

The second argument (after the comma) is the default value, used when the job is **not** running
inside Slurm — for example, during a local dry-run.


## 7. Reading cluster info inside your script

Use `cluv.job.current_run_info()` to access cluster-specific settings at runtime:

```python
import cluv.job
import cluv.config

run_info = cluv.job.current_run_info()

if run_info:
    # Running on Slurm — use per-cluster config
    datasets_path = run_info.cluster_config.datasets_path
else:
    # Running locally
    datasets_path = cluv.config.get_cluv_config().datasets_path
```

`current_run_info()` returns `None` when the script is not running inside a Slurm job, so this
pattern works both locally and on the cluster without any changes.


## Full example

See [`examples/hydra_example/`](https://github.com/mila-iqia/cluv/tree/master/examples/hydra_example)
for a complete working example with CIFAR-10, Weights & Biases logging, and multi-cluster config.
