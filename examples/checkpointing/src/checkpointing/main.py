"""Checkpointing example."""

import argparse
import logging
import os
import random
import shutil
import signal
import sys
import uuid
import warnings
from pathlib import Path
from types import FrameType
from typing import Any, TypedDict

import numpy as np
import rich.logging
import torch
from torch import Tensor, nn
from torch.nn import functional as F
from torch.utils.data import DataLoader, random_split
from torchvision import transforms
from torchvision.datasets import CIFAR10
from torchvision.models import resnet18
from tqdm import tqdm

logger = logging.getLogger(__name__)

SCRATCH = Path(os.environ["SCRATCH"])
SLURM_TMPDIR = Path(os.environ["SLURM_TMPDIR"])
SLURM_JOBID = os.environ["SLURM_JOBID"]

CHECKPOINT_FILE_NAME = "checkpoint.pth"


# To make your code as much reproducible as possible, uncomment the following
# block:
## === Reproducibility ===
## Be warned that this can make your code slower. See
## https://pytorch.org/docs/stable/notes/randomness.html#cublas-and-cudnn-deterministic-operations
## for more details.
# torch.use_deterministic_algorithms(True)
## === Reproducibility (END) ===


class RunState(TypedDict):
    """Typed dictionary containing the state of the training run which is saved at each epoch.

    Using type hints helps prevent bugs and makes your code easier to read for both humans and
    machines (e.g. Copilot). This leads to less time spent debugging and better code suggestions.
    """

    epoch: int
    best_acc: float
    model_state: dict[str, Tensor]
    optimizer_state: dict[str, Tensor]

    random_state: tuple[Any, ...]
    numpy_random_state: dict[str, Any]
    torch_random_state: Tensor
    torch_cuda_random_state: list[Tensor]

def signal_handler(signum: int, frame: FrameType | None):
    """Called before the job gets pre-empted or reaches the time-limit.

    This should run quickly. Performing a full checkpoint here mid-epoch is not recommended.
    """
    signal_enum = signal.Signals(signum)
    logger.error(f"Job received a {signal_enum.name} signal!")
    # Perform quick actions that will help the job resume later.
    # If you use Weights & Biases: https://docs.wandb.ai/guides/runs/resuming#preemptible-sweeps
    # if wandb.run:
    #     wandb.mark_preempting()

def main():
    # Use an argument parser so we can pass hyperparameters from the command line.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--learning-rate", type=float, default=5e-4)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument(
        "--run-dir", type=Path, default=SCRATCH / "checkpointing_example" / SLURM_JOBID
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    epochs: int = args.epochs
    learning_rate: float = args.learning_rate
    weight_decay: float = args.weight_decay
    batch_size: int = args.batch_size
    run_dir: Path = args.run_dir
    seed: int = args.seed

    checkpoint_dir = run_dir / "checkpoints"
    start_epoch: int = 0
    best_acc: float = 0.0

    # Seed the random number generators as early as possible for reproducibility
    random.seed(seed)
    np.random.seed(seed)
    torch.random.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)

    # Check that the GPU is available
    assert torch.cuda.is_available() and torch.cuda.device_count() > 0
    device = torch.device("cuda", 0)

    # Setup logging (optional, but much better than using print statements)
    # Uses the `rich` package to make logs pretty.
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[
            rich.logging.RichHandler(
                markup=True,
                console=rich.console.Console(
                    # Allower wider log lines in sbatch output files than on the terminal.
                    width=120 if not sys.stdout.isatty() else None
                ),
            )
        ],
    )

    # Create a model and move it to the GPU.
    model = resnet18(num_classes=10)
    model.to(device=device)

    optimizer = torch.optim.AdamW(
        model.parameters(), lr=learning_rate, weight_decay=weight_decay
    )

    # Try to resume from a checkpoint, if one exists.
    checkpoint: RunState | None = load_checkpoint(checkpoint_dir, map_location=device)
    if checkpoint:
        start_epoch = checkpoint["epoch"] + 1  # +1 to start at the next epoch.
        best_acc = checkpoint["best_acc"]
        model.load_state_dict(checkpoint["model_state"])
        optimizer.load_state_dict(checkpoint["optimizer_state"])
        random.setstate(checkpoint["random_state"])
        np.random.set_state(checkpoint["numpy_random_state"])
        # NOTE: Need to move those tensors to CPU before they can be loaded.
        torch.random.set_rng_state(checkpoint["torch_random_state"].cpu())
        torch.cuda.random.set_rng_state_all(
            t.cpu() for t in checkpoint["torch_cuda_random_state"]
        )
        logger.info(
            f"Resuming training at epoch {start_epoch} (best_acc={best_acc:.2%})."
        )
    else:
        logger.info(f"No checkpoints found in {checkpoint_dir}. Training from scratch.")

    # Setup CIFAR10
    num_workers = get_num_workers()
    dataset_path = (SLURM_TMPDIR or Path("..")) / "data"

    train_dataset, valid_dataset, test_dataset = make_datasets(str(dataset_path))
    train_dataloader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=True,
        # generator=torch.Generator().manual_seed(seed),
    )
    valid_dataloader = DataLoader(
        valid_dataset,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=False,
        # generator=torch.Generator().manual_seed(seed),
    )
    _test_dataloader = DataLoader(  # NOTE: Not used in this example.
        test_dataset,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=False,
    )

    signal.signal(
        signal.SIGTERM, signal_handler
    )  # Before getting pre-empted and requeued.
    signal.signal(
        signal.SIGUSR1, signal_handler
    )  # Before reaching the end of the time limit.

    for epoch in range(start_epoch, epochs):
        logger.debug(f"Starting epoch {epoch}/{epochs}")

        # Set the model in training mode (important for e.g. BatchNorm and Dropout layers)
        model.train()

        # NOTE: using a progress bar from tqdm because it's nicer than using `print`.
        progress_bar = tqdm(
            total=len(train_dataloader),
            desc=f"Train epoch {epoch}",
            disable=not sys.stdout.isatty(),  # Disable progress bar in non-interactive environments.
        )

        # Training loop
        for batch in train_dataloader:
            # Move the batch to the GPU before we pass it to the model
            batch: tuple[Tensor, ...] = tuple(item.to(device) for item in batch)
            x, y = batch

            # Forward pass
            logits: Tensor = model(x)

            loss = F.cross_entropy(logits, y)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            # Calculate some metrics:
            n_correct_predictions = logits.detach().argmax(-1).eq(y).sum()
            n_samples = y.shape[0]
            accuracy = n_correct_predictions / n_samples

            logger.debug(f"Accuracy: {accuracy.item():.2%}")
            logger.debug(f"Average Loss: {loss.item()}")

            # Advance the progress bar one step, and update the text displayed in the progress bar.
            progress_bar.update(1)
            progress_bar.set_postfix(loss=loss.item(), accuracy=accuracy.item())
        progress_bar.close()

        val_loss, val_accuracy = validation_loop(model, valid_dataloader, device)
        logger.info(
            f"Epoch {epoch}: Val loss: {val_loss:.3f} accuracy: {val_accuracy:.2%}"
        )

        # Remember best accuracy and save the current state.
        is_best = val_accuracy > best_acc
        best_acc = max(val_accuracy, best_acc)

        if checkpoint_dir is not None:
            save_checkpoint(
                checkpoint_dir,
                is_best,
                RunState(
                    epoch=epoch,
                    model_state=model.state_dict(),
                    optimizer_state=optimizer.state_dict(),
                    random_state=random.getstate(),
                    numpy_random_state=np.random.get_state(legacy=False),
                    torch_random_state=torch.random.get_rng_state(),
                    torch_cuda_random_state=torch.cuda.random.get_rng_state_all(),
                    best_acc=best_acc,
                ),
            )

    print("Done!")


@torch.no_grad()
def validation_loop(model: nn.Module, dataloader: DataLoader, device: torch.device):
    model.eval()

    total_loss = 0.0
    n_samples = 0
    correct_predictions = 0

    for batch in dataloader:
        batch = tuple(item.to(device) for item in batch)
        x, y = batch

        logits: Tensor = model(x)
        loss = F.cross_entropy(logits, y)

        batch_n_samples = x.shape[0]
        batch_correct_predictions = logits.argmax(-1).eq(y).sum().item()

        total_loss += loss.item()
        n_samples += batch_n_samples
        correct_predictions += int(batch_correct_predictions)

    accuracy = correct_predictions / n_samples
    return total_loss, accuracy


def make_datasets(
    dataset_path: str,
    val_split: float = 0.1,
    val_split_seed: int = 42,
):
    """Returns the training, validation, and test splits for CIFAR10.

    NOTE: We don't use image transforms here for simplicity.
    Having different transformations for train and validation would complicate things a bit.
    Later examples will show how to do the train/val/test split properly when using transforms.
    """
    train_dataset = CIFAR10(
        root=dataset_path, transform=transforms.ToTensor(), download=True, train=True
    )
    test_dataset = CIFAR10(
        root=dataset_path, transform=transforms.ToTensor(), download=True, train=False
    )
    # Split the training dataset into a training and validation set.
    train_dataset, valid_dataset = random_split(
        train_dataset,
        ((1 - val_split), val_split),
        torch.Generator().manual_seed(val_split_seed),
    )
    return train_dataset, valid_dataset, test_dataset


def get_num_workers() -> int:
    """Gets the optimal number of DataLoader workers to use in the current job."""
    if "SLURM_CPUS_PER_TASK" in os.environ:
        return int(os.environ["SLURM_CPUS_PER_TASK"])
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return torch.multiprocessing.cpu_count()


def load_checkpoint(checkpoint_dir: Path, **torch_load_kwargs) -> RunState | None:
    """Loads the latest checkpoint if possible, otherwise returns `None`."""
    logger = logging.getLogger(__name__)

    checkpoint_file = checkpoint_dir / CHECKPOINT_FILE_NAME
    restart_count = int(os.environ.get("SLURM_RESTART_COUNT", 0))
    if restart_count:
        logger.info(
            f"NOTE: This job has been restarted {restart_count} times by SLURM."
        )

    if not checkpoint_file.exists():
        logger.debug(f"No checkpoint found in checkpoints dir ({checkpoint_dir}).")
        if restart_count:
            logger.warning(
                RuntimeWarning(
                    f"This job has been restarted {restart_count} times by SLURM, but no "
                    "checkpoint was found! This either means that your checkpointing code is "
                    "broken, or that the job did not reach the checkpointing portion of your "
                    "training loop."
                )
            )
        return None

    torch_load_kwargs.setdefault("weights_only", False)
    checkpoint_state: dict = torch.load(checkpoint_file, **torch_load_kwargs)

    missing_keys = set(checkpoint_state.keys()) - RunState.__required_keys__
    if missing_keys:
        warnings.warn(
            RuntimeWarning(
                f"Checkpoint at {checkpoint_file} is missing the following keys: {missing_keys}. "
                f"Ignoring this checkpoint."
            )
        )
        return None

    logger.debug(f"Resuming from the checkpoint file at {checkpoint_file}")
    state: RunState = checkpoint_state  # type: ignore
    return state


def save_checkpoint(checkpoint_dir: Path, is_best: bool, state: RunState):
    """Saves a checkpoint with the current state of the run in the checkpoint dir.

    The best checkpoint is also updated if `is_best` is `True`.

    Parameters:
        checkpoint_dir (Path): The checkpoint directory.
        is_best (bool): Whether this is the best checkpoint so far.
        state (RunState): The dictionary containing all the things to save.
    """
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = checkpoint_dir / CHECKPOINT_FILE_NAME

    # Use a unique ID to avoid any potential collisions.
    unique_id = uuid.uuid1()
    temp_checkpoint_file = checkpoint_file.with_suffix(f".temp{unique_id}")

    torch.save(state, temp_checkpoint_file)
    os.replace(temp_checkpoint_file, checkpoint_file)

    if is_best:
        best_checkpoint_file = checkpoint_file.with_name("model_best.pth")
        temp_best_checkpoint_file = best_checkpoint_file.with_suffix(
            f".temp{unique_id}"
        )
        shutil.copyfile(checkpoint_file, temp_best_checkpoint_file)
        os.replace(temp_best_checkpoint_file, best_checkpoint_file)


if __name__ == "__main__":
    main()
