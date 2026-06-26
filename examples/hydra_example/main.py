"""Example of using Cluv with Hydra."""

import dataclasses
import logging
import math
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import hydra
import hydra.core.hydra_config
import rich
import torch
import tqdm
import wandb
from omegaconf import DictConfig
from torchvision.datasets import CIFAR10

import cluv
import cluv.config
import cluv.job
import cluv.utils

# from cluv.job import current_run_info, get_datasets_path

# OmegaConf.register_new_resolver("eval", eval)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    """Command-line arguments / config for this example."""

    # Time to wait before producing the result.
    # Can be useful to test and simulate preemption or cancelling jobs.
    job_duration_seconds: int = 60

    seed: int = int(os.environ.get("SLURM_PROCID", "0"))

    lr: float = 0.1


@hydra.main(version_base="1.3", config_path="configs", config_name="config")
def main(config_dict: DictConfig):
    print("Config: ", config_dict)
    config = Config(**hydra.utils.instantiate(config_dict))
    rich.print(config)

    # Use the 'usual' way to get the Hydra output dir:
    output_dir = hydra.core.hydra_config.HydraConfig.get().runtime.output_dir
    print(f"Output directory: {output_dir}")

    # job_info = cluv.job.current_run_info()
    run_info = cluv.job.current_run_info()

    if run_info:
        # Running on a Slurm cluster. Use the setting from the cluster config.
        print(f"Running on cluster {run_info.cluster}!")
        datasets_path = run_info.cluster_config.datasets_path
    else:
        # Not running on a Slurm cluster. Use the setting from the cluv config.
        datasets_path = cluv.config.get_cluv_config().datasets_path
    if not datasets_path:
        raise ValueError(
            "A datasets_path must be set either in the config for this example to work."
        )

    datasets_path = Path(os.path.expandvars(datasets_path))

    run_id = None
    run_dir = None
    if job_info := cluv.job.current_run_info():  # if we are in a Slurm job:
        run_id = job_info.run_id
        run_dir = Path(os.path.expandvars(job_info.results_path))

    wandb_run = wandb.init(
        project="cluv-example",
        name=run_id,
        id=run_id,
        # dir=run_dir,
        config={"config": dataclasses.asdict(config)}
        | ({"job": dataclasses.asdict(job_info)} if job_info else {})
        | {"env": {k: v for k, v in os.environ.items() if k.startswith("SLURM")}},
        resume="allow",
        # if using Hydra multirun to run multiple jobs in sequence, create a new run for each.
        reinit="create_new",
        # settings=wandb.Settings(),
    )
    run_id = wandb_run.id
    run_dir = Path(wandb_run.dir)

    print(f"Job {run_id} starts.")

    random.seed(config.seed)
    torch.manual_seed(config.seed)

    # Test that we can load a dataset from the dataset_path (that was synced by Cluv)
    dataset = CIFAR10(datasets_path, download=False)
    print(dataset)

    # model = torchvision.models.resnet18(num_classes=10)
    # optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    # TODO: Make this a distributed example, so that it can also run on Tamia and others with
    # full-node job allocations.
    # from torch.distributed.fsdp import FullyShardedDataParallel as FSDP
    # from torch.nn.parallel import DistributedDataParallel
    # model = DistributedDataParallel(model)

    for i in tqdm.tqdm(range(config.job_duration_seconds), disable=(not sys.stdout.isatty())):
        # Some fake, loss that varies a bit between seeds and decreases over time.
        fake_loss = math.exp(-i / 10) + random.random() * 0.1
        time.sleep(1)
        wandb_run.log({"step": i, "loss": fake_loss})
        # print(f"Step {i}: loss={fake_loss}")

    print(f"Job {run_id} is about to end.")

    run_dir.mkdir(parents=True, exist_ok=True)
    results_file = run_dir / "results.txt"
    with results_file.open("a") as f:
        f.write(f"This is the result of job {run_id}\n")

    wandb_run.finish()


if __name__ == "__main__":
    main()
