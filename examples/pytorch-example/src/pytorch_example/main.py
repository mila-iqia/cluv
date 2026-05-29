"""A script that reads something, and produces some output.

This is a simplified job script, used to test the syncing of the 'dataset' across clusters.
"""

import dataclasses
import math
import os
import random
import sys
import time
from dataclasses import dataclass

import simple_parsing
import torch
import torch.backends
import tqdm
import wandb
from torchvision.datasets import CIFAR10

from cluv.config import current_cluster_config
from cluv.job import current_job_info
from cluv.utils import current_cluster


@dataclass(frozen=True)
class Args:
    """Command-line arguments for this example."""

    # Time to wait before producing the result.
    # Can be useful to test and simulate preemption or cancelling jobs.
    wait_duration_seconds: int = 60

    seed: int = int(os.environ.get("SLURM_PROCID", "0"))


def main(args: Args | None = None):
    cluster = current_cluster()
    cuda_built = torch.backends.cuda.is_built()
    cuda_avail = torch.cuda.is_available()
    device_count = torch.cuda.device_count()

    print(f"Run on cluster:       {cluster}")
    print(f"PyTorch built with CUDA:         {cuda_built}")
    print(f"PyTorch detects CUDA available:  {cuda_avail}")
    print(f"PyTorch-detected #GPUs:          {device_count}")
    if device_count == 0:
        print("    No GPU detected.")
    else:
        for i in range(device_count):
            print(f"    GPU {i}:      {torch.cuda.get_device_name(i)}")

    args = args or simple_parsing.parse(Args, description=__doc__)

    job_info = current_job_info()
    cluster_info = current_cluster_config()
    assert job_info and cluster_info, "This example should be run in a slurm job."

    print(f"Job {job_info.run_id} starts.")
    wandb.init(
        project="cluv-example",
        name=job_info.run_id,
        id=job_info.run_id,
        config=vars(args)
        | {"job": dataclasses.asdict(job_info)}
        | {"env": {k: v for k, v in os.environ.items() if k.startswith("SLURM")}},
        resume="allow",
    )
    random.seed(args.seed)
    torch.manual_seed(args.seed)

    # Test that we can load a dataset from the dataset_path (that was synced by Cluv)
    assert cluster_info.datasets_path, "This example requires a datasets_path to be set."
    dataset = CIFAR10(cluster_info.datasets_path, download=False)
    print(dataset)

    # model = torchvision.models.resnet18(num_classes=10)
    # optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    # TODO: Make this a distributed example, so that it can also run on Tamia and others with
    # full-node job allocations.
    # from torch.distributed.fsdp import FullyShardedDataParallel as FSDP
    # from torch.nn.parallel import DistributedDataParallel
    # model = DistributedDataParallel(model)

    for i in tqdm.tqdm(range(args.wait_duration_seconds), disable=(not sys.stdout.isatty())):
        # Some fake, loss that varies a bit between seeds and decreases over time.
        fake_loss = math.exp(-i / 10) + random.random() * 0.1
        time.sleep(1)
        wandb.log({"step": i, "loss": fake_loss})
        print(f"Step {i}: loss={fake_loss}")

    print(f"Job {job_info.run_id} is about to end.")

    job_info.results_path.mkdir(parents=True, exist_ok=True)
    results_file = job_info.results_path / "results.txt"
    with results_file.open("a") as f:
        f.write(f"This is the result of job {job_info.run_id}\n")


if __name__ == "__main__":
    main()
