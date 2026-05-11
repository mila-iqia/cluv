"""ImageNet Distributed training script.

# Features:
- Multi-GPU / Multi-node training with DDP
- Wandb logging
- Checkpointing
- Profiling with the PyTorch profiler and tensorboard
- Good sanity checks
- Automatic mixed precision (AMP) support

# Potential Improvements - to be added as an exercise! 😉
- Use a larger model from HuggingFace or change the dataset from ImageNet to a language dataset from HuggingFace
- Use FSDP to train a larger model that doesn't fit inside a single GPU


Example:

```bash
srun --ntasks=2 --pty uv run python main.py --epochs=1 --limit_train_samples=50_000 \
    --limit_val_samples=2000 --batch_size=512 --use_amp --compile=default \
    --run_name=1024_amp_compile_default
```
"""

import contextlib
import dataclasses
import datetime
import logging
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, TypeVar

import numpy as np
import rich.logging
import rich.pretty
import simple_parsing
import sklearn
import sklearn.model_selection
import torch
import torchvision
import tqdm
import tqdm.rich
import wandb
from torch import Tensor, nn
from torch.distributed import ReduceOp
from torch.nn import functional as F
from torch.profiler import profile, tensorboard_trace_handler
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from torchvision.datasets import ImageNet
from torchvision.transforms import v2 as transforms

JOB_ID = os.environ["SLURM_JOB_ID"]  # you absolutely need to be within a slurm job!
SCRATCH = Path(os.environ["SCRATCH"])
SLURM_TMPDIR = Path(os.environ.get("SLURM_TMPDIR", "/tmp"))
assert SLURM_TMPDIR.exists(), f"SLURM_TMPDIR (assumed {SLURM_TMPDIR}) should exist!"

# Set any missing environment variables so that `torch.distributed.init_process_group`
# works properly, namely RANK, WORLD_SIZE, MASTER_ADDR, MASTER_PORT, (LOCAL_RANK).
#
# The accompanying sbatch script already does this in bash, which is preferable, since
# you need to make sure that these environment variables are set before any torch operations
# are executed. (Some modules might inadvertently initialize cuda when imported which is a problem).
#
# Also doing this here just in case you're using a different sbatch script or running this from
# the vscode terminal or with the vscode debugger.
# Using the Vscode debugger to debug multi-gpu jobs is very convenient.
#
# Note: here by using .setdefault we don't overwrite env variables that are already set,
# so you could in principle use this in a workflow based on srun + torchrun or
# srun + 'accelerate launch'.

if "SLURM_PROCID" not in os.environ and "RANK" not in os.environ:
    # If neither the SLURM nor the torch distributed env vars are set, raise an error.
    raise RuntimeError(
        "Both the SLURM and the torch distributed env vars are not set! "
        "This indicates that you might be running this script in something like the "
        "vscode terminal with `python main.py>`.\n"
        f"Consider relaunching the same command with srun instead, like so: \n"
        f"➡️    srun --pty python main.py {' '.join(sys.argv)}\n"
        "See https://slurm.schedmd.com/srun.html for more info."
    )

# This will raise an error if both are unset. This is expected (see above).
RANK = int(os.environ.setdefault("RANK", os.environ.get("SLURM_PROCID", "")))
LOCAL_RANK = int(os.environ.setdefault("LOCAL_RANK", os.environ.get("SLURM_LOCALID", "")))
WORLD_SIZE = int(os.environ.setdefault("WORLD_SIZE", os.environ.get("SLURM_NTASKS", "")))
MASTER_PORT = int(os.environ.setdefault("MASTER_PORT", str(10000 + int(JOB_ID) % 10000)))
if "SLURM_JOB_NODELIST" in os.environ:
    # Get the hostname of the first node, for example: "cn-l[084-085]" --> cn-l084
    _first_node = subprocess.check_output(
        f"scontrol show hostnames {os.environ['SLURM_JOB_NODELIST']}", text=True, shell=True
    ).split()[0]
    MASTER_ADDR = os.environ.setdefault("MASTER_ADDR", _first_node)
else:
    MASTER_ADDR = os.environ.setdefault("MASTER_ADDR", "127.0.0.1")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format=f"[{RANK + 1}/{WORLD_SIZE}] - %(message)s ",
    handlers=[rich.logging.RichHandler(markup=True)],
    force=True,
)
logger = logging.getLogger(__name__)


class DummyModel(nn.Module):
    """Dummy model used while debugging - uses almost no compute or memory.

    Examples of when this is useful:
    -   to check if data loading is the bottleneck, we can pull samples from the dataloader
        as fast as possible and compare that throughput (in samples/second) to the same
        during training. If the two are similar, then the dataloader is the bottleneck.
        Using a dummy model like this makes it so we don't have to modify our training loop
        to do this kind of sanity check.
    """

    def __init__(self, num_classes: int, **_kwargs):
        super().__init__()
        self.num_classes = num_classes
        # A dummy weight..
        self.linear = nn.Linear(1, num_classes)

    def forward(self, x: Tensor) -> Tensor:
        mean_of_each_xi = x.flatten(1).mean(1, keepdim=True)  # [batch_size, 1]
        return self.linear(mean_of_each_xi)  # [batch_size, num_classes]


models: dict[str, Callable[..., nn.Module]] = {
    "dummy": DummyModel,
    "resnet18": torchvision.models.resnet18,
    "resnet34": torchvision.models.resnet34,
    "resnet50": torchvision.models.resnet50,
    "resnet101": torchvision.models.resnet101,
    "resnet152": torchvision.models.resnet152,
    "vit_b_16": torchvision.models.vit_b_16,
    "vit_b_32": torchvision.models.vit_b_32,  # default model
    "vit_l_16": torchvision.models.vit_l_16,
    "vit_l_32": torchvision.models.vit_l_32,
}


@dataclass
class Args:
    """Dataclass that contains the command-line arguments for this script."""

    epochs: int = 10
    learning_rate: float = 3e-4
    weight_decay: float = 1e-4
    batch_size: int = 512

    pretrained: bool = False
    """Whether to use a pretrained model or start from a random initialization."""

    checkpoint_dir: Path | None = SCRATCH / "logs" / "imagenet" / str(JOB_ID)
    """Where checkpoints are stored."""

    checkpoint_interval_epochs: int = 1
    """Interval (in epochs) between saving checkpoints."""

    dataset_path: Path = SLURM_TMPDIR / "data"
    """Where to look for the dataset."""

    use_fake_data: bool = False
    """If true, use torchvision.datasets.FakeData instead of ImageNet.

    Useful for debugging.
    """

    limit_train_samples: int = 0
    """ If > 0, limit the number of training samples to this value.

    This can be very useful to debug the training loop, checkpointing, and validation, or to check that
    the model can overfit on a small number of samples.
    """

    limit_val_samples: int = 0
    """ If > 0, limit the number of validation samples to this value."""

    num_workers: int = int(os.environ.get("SLURM_CPUS_PER_TASK", len(os.sched_getaffinity(0))))
    """Number of dataloader workers."""

    seed: int = 42
    """Base random seed for everything except the train/validation split."""

    val_seed: int = 0
    """Random seed used to create the train/validation split."""

    model_name: str = simple_parsing.choice(*models.keys(), default="vit_b_32")
    """Which model function to use."""

    compile: str = ""
    """If set, use torch.compile to compile the model with the given string as the "mode" argument."""

    verbose: int = simple_parsing.field(alias="-v", action="count", default=0)
    """Increase logging verbosity (can be specified multiple times)."""

    # IDEA: Can we instead use a logging interval in seconds?
    # One problem is that this would make it hard to compare metric values at the same step.
    logging_interval: int = 20
    """Interval (in batches) between logging of training metrics to wandb or to the output file."""

    use_amp: bool = False
    """If True, use automatic mixed precision (AMP) for training."""

    run_name: str | None = JOB_ID + (
        f"_step{_step}" if (_step := int(os.environ.get("SLURM_STEP_ID", "0"))) > 0 else ""
    )
    """Name for the run (in wandb and in tensorboard)."""

    wandb_run_id: str = JOB_ID + (
        f"_step{_step}" if (_step := int(os.environ.get("SLURM_STEP_ID", "0"))) > 0 else ""
    )
    """Unique ID for the Weights & Biases run.

    Used to resume a run if the job is restarted.
    """

    wandb_group: str | None = None

    wandb_project: str = "codingtips_profiling_example"

    no_wandb: bool = False
    """When set, disables wandb logging."""


def main():
    # Use an argument parser so we can pass hyperparameters from the command line.
    # You can use plain argparse if you like. Simple-parsing is an extension of argparse for dataclasses.
    args: Args = simple_parsing.parse(Args)

    # Create a checkpoints directory in $SCRATCH and symlink it so it appears in the current directory.
    if not (_checkpoints_dir := Path("checkpoints")).exists():
        _checkpoints_dir_in_scratch = SCRATCH / "checkpoints"
        _checkpoints_dir_in_scratch.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"Creating a symlink from {_checkpoints_dir} --> {_checkpoints_dir_in_scratch}"
        )
        _checkpoints_dir.symlink_to(_checkpoints_dir_in_scratch)

    if args.checkpoint_dir is None:
        # Use the run name or run_id as the checkpoint folder by default if unset.
        # This makes it so the names in wandb and the names in tensorboard line up nicely.
        args.checkpoint_dir = (
            SCRATCH / "checkpoints" / (args.run_name or args.wandb_run_id or JOB_ID)
        )

    assert torch.cuda.is_available() and torch.cuda.device_count() > 0
    assert torch.distributed.is_available()
    # https://docs.pytorch.org/tutorials/beginner/ddp_series_multigpu.html#constructing-the-process-group
    # Default timeout is 30 minutes. Reducing the timeout here, so the job fails quicker if there's
    # a communication problem between nodes.
    # NOTE: Since preparing imagenet on each node can take about 12-15 minutes on the Mila cluster,
    # we set the timeout to 20 minutes here.
    torch.cuda.set_device(LOCAL_RANK)
    torch.distributed.init_process_group(
        backend="nccl",
        rank=RANK,
        world_size=WORLD_SIZE,
        timeout=datetime.timedelta(minutes=20),
    )
    is_master = RANK == 0

    device = torch.device("cuda", LOCAL_RANK)

    print(f"Using random seed: {args.seed}")
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    logger.setLevel(
        logging.WARNING
        if args.verbose == 0
        else logging.INFO
        if args.verbose == 1
        else logging.DEBUG
    )
    logger.info(f"World size: {WORLD_SIZE}, global rank: {RANK}, local rank: {LOCAL_RANK}")
    if is_master:
        print("Args:")
        rich.pretty.pprint(dataclasses.asdict(args))

    # Create a model and move it to the GPU.
    kwargs = {} if not args.pretrained else {"weights": "DEFAULT"}
    with device:
        model = models[args.model_name](num_classes=1000, **kwargs)
        model = model.to(device=device)
    # https://docs.pytorch.org/tutorials/beginner/ddp_series_multigpu.html#multi-gpu-training-with-ddp
    model = torch.nn.SyncBatchNorm.convert_sync_batchnorm(model)
    if args.compile:
        # TODO: Try different torch.compile modes, see how this affects performance!
        torch.set_float32_matmul_precision("high")  # Use TensorFloat32 tensor cores.
        model = torch.compile(model, mode=args.compile)
    # Wrap the model with DistributedDataParallel
    # (See https://pytorch.org/docs/stable/nn.html#torch.nn.parallel.DistributedDataParallel)
    model = nn.parallel.DistributedDataParallel(
        model, device_ids=[LOCAL_RANK], output_device=LOCAL_RANK
    )

    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.learning_rate, weight_decay=args.weight_decay
    )
    # https://docs.pytorch.org/tutorials/recipes/recipes/amp_recipe.html
    scaler = None
    if args.use_amp:
        scaler = torch.amp.grad_scaler.GradScaler(enabled=True)
        torch.set_float32_matmul_precision("high")
        logger.info("Using automatic mixed precision (AMP) with bfloat16")

    # Setup the dataset.
    train_dataset, valid_dataset, test_dataset = make_datasets(
        args.dataset_path,
        val_split_seed=args.val_seed,
        use_fake_data=args.use_fake_data,
    )
    # IDEA: Use a smaller subset of the dataset for faster debugging of the checkpointing / validation loop or
    # to test if the model can overfit on a small number of samples.
    if args.limit_train_samples:
        train_dataset = torch.utils.data.Subset(
            train_dataset, list(range(args.limit_train_samples))
        )
    if args.limit_val_samples:
        valid_dataset = torch.utils.data.Subset(valid_dataset, list(range(args.limit_val_samples)))

    # Restricts data loading to a subset of the dataset exclusive to the current process
    train_sampler = DistributedSampler(
        dataset=train_dataset, shuffle=True, num_replicas=WORLD_SIZE, rank=RANK, seed=args.seed
    )
    valid_sampler = DistributedSampler(
        dataset=valid_dataset, shuffle=False, num_replicas=WORLD_SIZE, rank=RANK
    )
    test_sampler = DistributedSampler(
        dataset=test_dataset, shuffle=False, num_replicas=WORLD_SIZE, rank=RANK
    )
    # TODO: make sure that the dataloader workers random state is restored properly.
    train_dataloader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        sampler=train_sampler,
        pin_memory=True,
    )
    valid_dataloader = DataLoader(
        valid_dataset,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        sampler=valid_sampler,
        pin_memory=True,
    )
    _test_dataloader = DataLoader(  # Not used in this example.
        test_dataset,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        sampler=test_sampler,
        pin_memory=True,
    )
    effective_batch_size = args.batch_size * WORLD_SIZE
    logger.info(f"Effective (global) batch size: {effective_batch_size}")

    # Load the latest checkpoint if it exists.
    if previous_checkpoints := list(args.checkpoint_dir.glob("*.pt")):
        # Checkpoints are named like `epoch_0.pt`, `epoch_1.pt`. Find the latest.
        # Note: epoch_0 in this case is the initial checkpoint before any training.
        # epoch_1 is after one epoch of training, etc.
        latest_checkpoint = max(previous_checkpoints, key=lambda p: int(p.stem.split("_")[-1]))
        _num_epochs_done, step, num_samples = load_checkpoint(
            latest_checkpoint, model=model, optimizer=optimizer, device=device
        )
        starting_epoch = _num_epochs_done
        total_updates = step
        total_samples = num_samples
        logger.info(
            f"Resuming training from epoch {starting_epoch} (step {step}, {total_samples} total samples)"
        )
    else:
        starting_epoch = 0
        total_updates = 0
        total_samples = 0
        args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Starting training from scratch")

    # Initialize wandb logging.
    setup_wandb(
        args,
        effective_batch_size=effective_batch_size,
        previous_checkpoints=previous_checkpoints,
        total_updates=total_updates,
    )

    # Save an initial checkpoint (epoch 0) before training to make sure we can easily get the exact same initial weights.
    # Since code is supposed to be correctly seeded and reproducible, this is just an additional precaution.
    # Doing this here also makes it so if there is a checkpoint, there is also a wandb run, so we can resume the wandb run
    # more correctly than with just `resume="allow"`.
    if not previous_checkpoints and RANK == 0:
        save_checkpoint(
            checkpoint_path=args.checkpoint_dir / "epoch_0.pt",
            model=model,
            optimizer=optimizer,
            device=device,
            epoch=0,
            step=0,
            num_samples=0,
        )

    # Create the PyTorch profiler with a schedule that will output some traces that can be inspected with tensorboard.
    # https://docs.pytorch.org/tutorials/recipes/recipes/profiler_recipe.html#using-profiler-to-analyze-long-running-jobs
    # To view the traces, run `uvx tensorboard --with=torch_tb_profiler --logdir checkpoints`
    profiler = profile(
        schedule=torch.profiler.schedule(wait=2, warmup=2, active=2, repeat=1),
        on_trace_ready=tensorboard_trace_handler(
            str(args.checkpoint_dir), worker_name=f"rank_{RANK}"
        ),
        record_shapes=True,
        profile_memory=True,
        # Warning: This can be a bit too verbose while debugging. Only enable this if you really need it.
        # with_stack=True if "debugpy" not in sys.modules else True,
        with_stack=True if args.verbose >= 3 else False,
        with_flops=True,
        with_modules=True,
    )

    ###################
    ## Training loop ##
    ###################

    # Used at the end to display overall samples per second.
    t0 = time.time()
    starting_num_samples = total_samples

    for epoch in range(starting_epoch, args.epochs):
        logger.debug(f"Starting epoch {epoch}/{args.epochs}")
        # Important so each epoch uses a different ordering for the training samples.
        train_sampler.set_epoch(epoch)
        model.train()

        # Using a progress bar when in an interactive terminal. It also shows the throughput in samples/second.
        # If we're going to enable verbose logging within an epoch (for example to help identify issues),
        # it makes sense to use the progress bar from rich so that the logs are displayed nicely.
        # However, it doesn't support the `unit_scale` and `unit` arguments atm so we disable those arguments.
        pbar_type = tqdm.rich.tqdm_rich if args.verbose >= 2 else tqdm.tqdm
        assert isinstance(train_dataloader.batch_size, int)
        progress_bar = pbar_type(
            train_dataloader,
            desc=f"Train epoch {epoch + 1}/{args.epochs}",
            # Don't use a progress bar if outputting to a slurm output file or when not in task 0
            disable=(not sys.stdout.isatty() or not is_master),
            unit_scale=False if pbar_type is tqdm.rich.tqdm_rich else effective_batch_size,
            unit="batches" if pbar_type is tqdm.rich.tqdm_rich else "samples",
            dynamic_ncols=True,  # allow window resizing
        )
        data_transfer_cuda_stream = torch.cuda.Stream(device=device)
        epoch_loss = 0.0
        t = time.perf_counter()
        for batch_index, batch in enumerate(
            # We only create the profiling traces in the first two epochs.
            profile_loop(progress_bar, profiler) if epoch <= 1 else progress_bar
        ):
            # This allows the GPU to keep working on the previous step while the data is copied from CPU to GPU!
            # https://docs.pytorch.org/tutorials/intermediate/pinmem_nonblock.html
            with torch.cuda.stream(data_transfer_cuda_stream):
                # Move the batch to the GPU before we pass it to the model
                batch = tuple(item.to(device, non_blocking=True) for item in batch)
                x, y = batch

            loss, accuracy, n_samples = training_step(
                model,
                x,
                y,
                optimizer,
                scaler=scaler,
                batch_index=batch_index,
            )

            epoch_loss += loss
            total_updates += 1
            total_samples += n_samples

            # Simple training speed calculation in samples/sec using the effective batch size.
            new_t = time.perf_counter()
            dt = new_t - t
            samples_per_sec = n_samples / dt
            t = new_t

            # Move the tensors to CPU so we can log them in the progress bar and to wandb.
            # Perform some logging, but only on the first task, and only every `logging_interval` batches.
            # Also only do this if wandb logging is enabled or if the progress bar is enabled.
            if (
                is_master
                and ((wandb.run and not wandb.run.disabled) or (not progress_bar.disable))
                and (batch_index == 0 or ((batch_index + 1) % args.logging_interval) == 0)
            ):
                # TODO: if --limit_train_samples=100_000, the logs in wandb have their last logged metrics
                # at samples=89_600 (7(updates) * 50(log interval) * 256(batch_size)). It would be nice to
                # also log metrics at the last batch (when we reach the limit_num_steps) even if batch_index
                # is not a multiple of logging interval.

                _loss = loss.item()
                _accuracy = accuracy.item()
                progress_bar.set_postfix(loss=f"{_loss:.3f}", accuracy=f"{_accuracy:.2%}")
                wandb.log(
                    {
                        "train/loss": _loss,
                        "train/accuracy": _accuracy,
                        "train/samples_per_sec": samples_per_sec,
                        "epoch": epoch,
                        "updates": total_updates,
                        "samples": total_samples,
                    }
                )
        progress_bar.close()

        t = time.perf_counter()
        val_loss, val_accuracy, val_samples = validation_loop(model, valid_dataloader, device)
        dt = time.perf_counter() - t
        val_sps = val_samples / dt
        if RANK == 0:
            rich.print(
                f"Epoch {epoch}: Val loss: {val_loss:.3f} accuracy: {val_accuracy:.2%} samples/sec: {val_sps:.1f}"
            )
        wandb.log(
            {
                "val/loss": val_loss,
                "val/accuracy": val_accuracy,
                "val/samples_per_sec": val_sps,
                "epoch": epoch,
            }
        )

        # Only save the checkpoint from the master process.
        # Make sure this doesn't cause a torch.distributed.timeout if it takes too long.
        if is_master and (epoch % args.checkpoint_interval_epochs) == 0:
            # save as epoch_1 after having done 1 epoch of training.
            save_checkpoint(
                checkpoint_path=args.checkpoint_dir / f"epoch_{epoch + 1}.pt",
                model=model,
                optimizer=optimizer,
                device=device,
                epoch=epoch,
                step=total_updates,
                num_samples=int(total_samples),
            )

    torch.distributed.destroy_process_group()
    total_time = time.time() - t0
    overall_samples = int(total_samples) - starting_num_samples
    overall_sps = overall_samples / total_time
    if wandb.run:
        wandb.run.summary["overall_train_samples_per_sec"] = overall_sps
        wandb.run.finish()
    print(f"Done in {total_time:.1f} seconds, with {overall_sps:.1f} images/second")


def training_step(
    model: nn.Module,
    x: Tensor,
    y: Tensor,
    optimizer: torch.optim.Optimizer,
    scaler: torch.amp.grad_scaler.GradScaler | None = None,
    batch_index: int | None = None,
):
    with torch.autocast(
        device_type="cuda",
        dtype=torch.bfloat16,
        enabled=scaler is not None and scaler.is_enabled(),
    ):
        # Forward pass
        logits: Tensor = model(x)

        local_loss = F.cross_entropy(logits, y, reduction="mean")

    if scaler is not None:
        # https://docs.pytorch.org/tutorials/recipes/recipes/amp_recipe.html#all-together-automatic-mixed-precision
        scaler.scale(local_loss).backward()
        scaler.step(optimizer)
        scaler.update()
    else:
        # nn.DistributedDataParallel automatically averages the gradients across devices.
        local_loss.backward()
        optimizer.step()
    optimizer.zero_grad()

    # Calculate some metrics

    # local metrics calculated with the tensors on the current GPU.
    local_n_correct_predictions = logits.detach().argmax(-1).eq(y).sum()
    local_n_samples = logits.shape[0]
    local_accuracy = local_n_correct_predictions / local_n_samples

    # Global metrics calculated with the results from all workers
    # Creating new tensors to hold the "global" values, but this isn't required.
    # Reduce the local metrics across all workers, sending the result to rank 0.
    # Summing n_correct and n_samples to get accuracy is resilient to
    # workers having different number of samples.
    # This could happen if the number of batches is not divisible by the number of batches
    # and if the distributed sampler is not set to drop the last incomplete batch.
    n_correct_predictions = local_n_correct_predictions.clone()
    n_samples = local_n_samples * torch.ones(1, device=local_loss.device, dtype=torch.int32)
    loss = local_loss.clone().detach()

    torch.distributed.reduce(loss, dst=0, op=ReduceOp.AVG)
    torch.distributed.reduce(n_correct_predictions, dst=0, op=ReduceOp.SUM)
    torch.distributed.reduce(n_samples, dst=0, op=ReduceOp.SUM)
    accuracy = n_correct_predictions / n_samples

    # Using lazy formatting so these tensors are only moved to cpu when necessary.
    if RANK == 0:
        logger.debug("(local) Loss: %.2f Accuracy: %.2f", local_loss, local_accuracy)
        logger.debug("Average Loss: %.2f Accuracy: %.2%", loss, accuracy)
    return loss, accuracy, n_samples


@torch.no_grad()
def validation_loop(model: nn.Module, dataloader: DataLoader, device: torch.device):
    model.eval()

    epoch_loss = torch.zeros(1, device=device)
    num_samples = torch.zeros(1, device=device, dtype=torch.int32)
    correct_predictions = torch.zeros(1, device=device, dtype=torch.int32)
    assert isinstance(dataloader.batch_size, int)

    progress_bar = tqdm.tqdm(
        dataloader,
        desc="Validation",
        unit_scale=dataloader.batch_size * WORLD_SIZE,
        unit="samples",
        # Don't use a progress bar if outputting to a slurm output file or when not in task 0
        disable=(not sys.stdout.isatty() or RANK != 0),
    )
    # NOTE: Because of DDP and distributed sampler, the last batch might have repeated samples,
    # leading to slightly imprecise metrics.
    for batch in progress_bar:
        batch = tuple(item.to(device) for item in batch)
        x, y = batch

        logits: Tensor = model(x)
        loss = F.cross_entropy(logits, y, reduction="sum")

        batch_n_samples = x.shape[0]
        batch_correct_predictions = logits.argmax(-1).eq(y).sum()

        epoch_loss += loss
        num_samples += batch_n_samples
        correct_predictions += batch_correct_predictions
    # Here we only need to reduce metrics once, after iterating over the entire dataset.
    torch.distributed.reduce(epoch_loss, dst=0, op=ReduceOp.SUM)
    torch.distributed.reduce(num_samples, dst=0, op=ReduceOp.SUM)
    torch.distributed.reduce(correct_predictions, dst=0, op=ReduceOp.SUM)
    epoch_average_loss = epoch_loss / num_samples
    accuracy = correct_predictions / num_samples
    return epoch_average_loss.item(), accuracy.item(), num_samples.item()


def setup_wandb(
    args: Args, effective_batch_size: int, previous_checkpoints: list[Path], total_updates: int
):
    """Calls `wandb.init` with the appropriate arguments."""
    # Normally you would only do this in the first task (rank 0), but here we do it in all tasks
    # using the new "shared" feature of wandb. This makes it much easier to track the GPU util of
    # all gpus on all nodes in the job.
    # See this link for more info:
    # - https://docs.wandb.ai/guides/track/log/distributed-training/#track-all-processes-to-a-single-run
    is_master = RANK == 0
    with goes_first(is_master):
        run = wandb.init(
            project=args.wandb_project,
            # if None, wandb will use a random name.
            name=args.run_name if args.run_name else None,
            id=args.wandb_run_id,
            # It's a good idea to log the SLURM environment variables to wandb.
            config=(
                dataclasses.asdict(args)
                | {k: v for k, v in os.environ.items() if k.startswith("SLURM_")}
                | dict(
                    effective_batch_size=effective_batch_size,
                    WORLD_SIZE=WORLD_SIZE,
                    MASTER_ADDR=MASTER_ADDR,
                    MASTER_PORT=MASTER_PORT,
                )
            ),
            group=args.wandb_group,
            # Use the new "shared" mode to log system utilization metrics from all tasks in the job:
            settings=wandb.Settings(
                mode="disabled" if args.no_wandb else os.environ.get("WANDB_MODE", "shared"),  # type: ignore
                x_primary=is_master,
                x_label=f"task_{RANK}",
                x_stats_gpu_device_ids=[LOCAL_RANK],
                x_update_finish_state=not is_master,
            ),
            # Resume an existing run with the same ID if the job is restarting after being preempted.
            # It would be *really* nice to use this resume feature, but this is new
            # at the time of writing (2025-09) and needs to be enabled for your project
            # by contacting wandb support.
            # resume_from=(
            #     f"{args.wandb_run_id}?_step={total_updates}"
            #     if previous_checkpoints and args.wandb_run_id
            #     else None
            # ),
            # resume=None if previous_checkpoints and args.wandb_run_id else "allow",
            # Use this for the time being instead:
            resume="allow",
        )
        # Wait a bit to make sure the run is created properly in wandb by the first task before other workers try to
        # also create it. Otherwise we can get a 409 error from the wandb server.
        time.sleep(5)

    # Specify the step metric (x-axis) and the metric to log against it (y-axis)
    run.define_metric("train/*", step_metric="updates")
    run.define_metric("val/*", step_metric="epoch")
    # https://docs.wandb.ai/guides/track/log/log-summary/#customize-summary-metrics
    run.define_metric("train/samples_per_sec", summary="max")
    run.define_metric("train/samples_per_sec", summary="mean")
    run.define_metric("train/samples_per_sec", summary="min")
    run.define_metric("val/samples_per_sec", summary="max")
    run.define_metric("val/samples_per_sec", summary="mean")
    run.define_metric("val/samples_per_sec", summary="min")


T = TypeVar("T")


def profile_loop(dataloader: Iterable[T], profiler: torch.profiler.profile) -> Iterable[T]:
    """Wraps the dataloader (or progress bar) and calls .step after each batch.

    This is used to save one level of indentation (with profiler block) and to call prof.step() at each step.

    Note, this doesn't need to be done at every epoch. It creates files used by tensorboard.
    """
    with profiler as prof:
        for batch in dataloader:
            yield batch
            prof.step()


def make_datasets(
    path: Path,
    val_split: float = 0.1,
    val_split_seed: int = 42,
    use_fake_data: bool = False,
):
    """Returns the training, validation, and test splits."""
    if use_fake_data:
        train_dataset = torchvision.datasets.FakeData(
            size=1_281_167,
            image_size=(3, 224, 224),
            num_classes=1000,
            transform=transforms.ToTensor(),
        )
        valid_dataset = torchvision.datasets.FakeData(
            size=20_000,
            image_size=(3, 224, 224),
            num_classes=1000,
            transform=transforms.Compose(
                [transforms.ToImage(), transforms.ToDtype(torch.float32, scale=True)]
            ),
        )
        test_dataset = torchvision.datasets.FakeData(
            size=50_000,
            image_size=(3, 224, 224),
            num_classes=1000,
            transform=transforms.Compose(
                [transforms.ToImage(), transforms.ToDtype(torch.float32, scale=True)]
            ),
        )
        return train_dataset, valid_dataset, test_dataset
    # todo: torchvision transforms can apparently moved to the GPU now? Would that speed up the training?
    train_transforms = torch.nn.Sequential(
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToImage(),
        transforms.ToDtype(torch.float32, scale=True),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    )
    test_transforms = torch.nn.Sequential(
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToImage(),
        transforms.ToDtype(torch.float32, scale=True),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    )
    # This takes ~12-15 minutes on the Mila cluster. The timeout value for the distributed process group
    # needs to be higher than this to avoid a timeout.
    # TODO: Could we setup a new process group just for this operation, with a high enough timeout,
    # that way the default process group can keep a short timeout value to waste less time in case of errors.
    with goes_first(LOCAL_RANK == 0):
        from prepare_data import prepare_imagenet

        logging.info(f"Preparing the ImageNet dataset in {path}")
        prepare_imagenet(path)
        logging.info(f"Done preparing the ImageNet dataset in {path}")

    train_dataset = ImageNet(root=path, transform=train_transforms, split="train")
    valid_dataset = ImageNet(root=path, transform=test_transforms, split="train")
    test_dataset = ImageNet(root=path, transform=test_transforms, split="val")

    # Split the training dataset into a training and validation set, based on a stratified split.
    # This is important to have a balanced distribution of classes in both sets.
    # See the sklearn.model_selection.train_test_split documentation for more info.
    n_samples = len(train_dataset)
    n_valid = int(val_split * n_samples)
    n_train = n_samples - n_valid
    train_indices, val_indices = sklearn.model_selection.train_test_split(
        np.arange(n_samples),
        train_size=n_train,
        test_size=n_valid,
        random_state=np.random.RandomState(val_split_seed),
        shuffle=True,
        stratify=train_dataset.targets,
    )
    train_dataset = torch.utils.data.Subset(train_dataset, train_indices)
    valid_dataset = torch.utils.data.Subset(valid_dataset, val_indices)

    return train_dataset, valid_dataset, test_dataset


def load_checkpoint(
    checkpoint_path: Path,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
) -> tuple[int, int, int]:
    logger.info(f"Loading checkpoint {checkpoint_path}")
    checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=False)
    model.load_state_dict(checkpoint["model"])
    optimizer.load_state_dict(checkpoint["optimizer"])
    epoch = checkpoint["epoch"]
    step = checkpoint["step"]
    nsamples = checkpoint["num_samples"]
    random.setstate(checkpoint["python_rng_state"])
    np.random.set_state(checkpoint["numpy_rng_state"])
    cpu_rng_state = checkpoint["torch_rng_state_cpu"]
    torch.random.set_rng_state(cpu_rng_state.cpu())
    torch.cuda.random.set_rng_state_all([t.cpu() for t in checkpoint["torch_rng_state_gpu"]])
    return epoch, step, nsamples


def save_checkpoint(
    checkpoint_path: Path,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
    epoch: int,
    step: int,
    num_samples: int,
):
    logger.info(f"Saving checkpoint at {checkpoint_path}")
    checkpoint = {
        "model": model.state_dict(),
        "optimizer": optimizer.state_dict(),
        "epoch": epoch,
        "step": step,
        "num_samples": num_samples,
        "python_rng_state": random.getstate(),
        "numpy_rng_state": np.random.get_state(),
        "torch_rng_state_cpu": torch.random.get_rng_state(),
        "torch_rng_state_gpu": torch.cuda.random.get_rng_state_all(),
    }
    tmp_checkpoint_path = checkpoint_path.with_suffix(".temp")
    torch.save(checkpoint, tmp_checkpoint_path)
    tmp_checkpoint_path.rename(checkpoint_path)


@contextlib.contextmanager
def goes_first(condition: bool, group: torch.distributed.ProcessGroup | None = None):
    if condition:
        yield
        torch.distributed.barrier(group=group, device_ids=[LOCAL_RANK])
    else:
        torch.distributed.barrier(group=group, device_ids=[LOCAL_RANK])
        yield


if __name__ == "__main__":
    main()
