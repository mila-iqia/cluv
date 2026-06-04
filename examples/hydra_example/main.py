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
import omegaconf
import rich
import torch
import tqdm
import wandb
from omegaconf import DictConfig
from torchvision.datasets import CIFAR10

from cluv.job import JobInfo, current_job_info, get_datasets_path

job: JobInfo | None = None


def cluv_resolver(attr: str, default: str | None = None) -> str | None:
    """OmegaConf resolver to access Cluv job info in Hydra configs.

    Usage in Hydra config: ${cluv:attr, default} where `attr` is an attribute of the current job (e.g. "results_path")
    and `default` is an optional default value to return if the attribute is not set in the current job.
    """
    global job
    if job is None:
        job = current_job_info()

    if default is not None:
        return getattr(job, attr, default)
    return getattr(job, attr)


omegaconf.OmegaConf.register_new_resolver("cluv", cluv_resolver)

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

    job_info = current_job_info()

    datasets_path = get_datasets_path()
    assert datasets_path, "A datasets_path must be set in the config for this example to work."
    datasets_path = Path(os.path.expandvars(datasets_path))

    run_id = None
    run_dir = None
    if job_info:  # if we are in a Slurm job:
        print(f"Running on cluster {job_info.cluster} with job_id={job_info.run_id}")
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
    assert datasets_path, "This example requires a datasets_path to be set."
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
