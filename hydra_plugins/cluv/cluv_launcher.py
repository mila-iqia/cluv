# https://github.com/facebookresearch/hydra/blob/main/examples/plugins/example_launcher_plugin/hydra_plugins/example_launcher_plugin/example_launcher.py

import logging
from collections.abc import Sequence
from typing import Any, Callable, ClassVar

import hydra_zen
from hydra.core.utils import JobReturn
from hydra.plugins.launcher import Launcher
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


# Made this a dataclass to avoid having an ugly default repr, but it causes issues with
# hydra-auto-schema because it tries to create a schema for everything here.
# @dataclasses.dataclass(init=False)
class CluvLauncher(Launcher):
    _EXECUTOR: ClassVar[str] = ""

    params: dict[str, Any]
    config: DictConfig | None = None
    task_function: TaskFunction | None = None
    sweep_configs: TaskFunction | None = None
    hydra_context: HydraContext | None = None
    executor: None

    # same signature as the submitit plugin to make it easier for people to transition.
    def __init__(
        self,
        # executor: Callable[[], RemoteSlurmExecutor],
        account: str | None = None,
        array_parallelism: int = 256,
        comment: str | None = None,
        constraint: str | None = None,
        cpus_per_gpu: int | None = None,
        cpus_per_task: int | None = None,
        dependency: str | None = None,
        exclude: str | None = None,
        exclusive: bool | None = None,
        gpus_per_node: int | str | None = None,
        gpus_per_task: int | str | None = None,
        gres: str | None = None,
        # job_name: str = "submitit",
        job_name: str = "submitit-${hydra.job.name}",
        mail_type: str | None = None,
        mail_user: str | None = None,
        mem: str | None = None,
        mem_per_cpu: str | None = None,
        mem_per_gpu: str | None = None,
        nodelist: str | None = None,
        nodes: int = 1,
        ntasks_per_node: int | None = None,
        num_gpus: int | None = None,
        partition: str | None = None,
        qos: str | None = None,
        setup: list[str] | None = None,
        signal_delay_s: int = 90,
        srun_args: list[str] | None = None,
        stderr_to_stdout: bool = True,  # changed!
        time: str | int = 5,
        use_srun: bool = True,
        wckey: str = "submitit",
        additional_parameters: dict | None = None,
        tasks_per_node: int | None = None,
        mem_gb: int | None = None,
    ) -> None:
        setup = setup or []
        additional_parameters = additional_parameters or {}

        if mem_gb is not None:
            assert mem is None, "can't use both mem and mem_gb"
            mem = f"{mem_gb}GB"
        if tasks_per_node is not None:
            assert ntasks_per_node is None, "can't use both tasks_per_node and ntasks_per_node"
            ntasks_per_node = tasks_per_node
        if ntasks_per_node is not None:
            additional_parameters["ntasks-per-node"] = ntasks_per_node
        super().__init__()
        params = dict(
            account=account,
            array_parallelism=array_parallelism,
            comment=comment,
            constraint=constraint,
            cpus_per_gpu=cpus_per_gpu,
            cpus_per_task=cpus_per_task,
            dependency=dependency,
            exclude=exclude,
            exclusive=exclusive,
            gpus_per_node=gpus_per_node,
            gpus_per_task=gpus_per_task,
            gres=gres,
            job_name=job_name,
            mail_type=mail_type,
            mail_user=mail_user,
            mem=mem,
            mem_per_cpu=mem_per_cpu,
            mem_per_gpu=mem_per_gpu,
            nodelist=nodelist,
            nodes=nodes,
            num_gpus=num_gpus,
            partition=partition,
            qos=qos,
            setup=setup,
            signal_delay_s=signal_delay_s,
            srun_args=srun_args,
            stderr_to_stdout=stderr_to_stdout,
            time=time,
            use_srun=use_srun,
            wckey=wckey,
            additional_parameters=additional_parameters,
        )
        self.params = {}
        for k, v in params.items():
            if OmegaConf.is_config(v):
                v = OmegaConf.to_container(v, resolve=True)
            self.params[k] = v

    def setup(
        self,
        *,
        hydra_context: HydraContext,
        task_function: Callable[[Any], Any],
        config: DictConfig,
    ) -> None:
        self.hydra_context = hydra_context
        self.task_function = task_function
        self.config = config
        # raise NotImplementedError(
        #     f"This launcher is not implemented yet. ({hydra_context=}, {task_function=}, {config=})"
        # )

    def launch(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> Sequence[JobReturn]:
        # lazy import to ensure plugin discovery remains fast

        raise NotImplementedError(
            f"This launcher is not implemented yet. ({job_overrides=}, {initial_job_idx=})"
        )


@hydra_zen.hydrated_dataclass(
    target=CluvLauncher, populate_full_signature=True, hydra_convert="object"
)
class CluvLauncherConfig: ...


# CluvLauncherConfig = hydra_zen.builds(
#     CluvLauncher,
#     populate_full_signature=True,
#     # zen_partial=True,
#     hydra_convert="object",
#     zen_dataclass={"cls_name": "CluvLauncherConfig"},
# )

# # Interesting idea: Create the config based on the signature of that function directly.
# from submitit.slurm.slurm import _make_sbatch_string
# _AddedArgumentsConf = hydra_zen.builds(
#     _make_sbatch_string,
#     populate_full_signature=True,
#     hydra_convert="object",
#     zen_exclude=["command", "folder", "map_count"],
# )
