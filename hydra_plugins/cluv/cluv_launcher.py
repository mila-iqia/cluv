# https://github.com/facebookresearch/hydra/blob/main/examples/plugins/example_launcher_plugin/hydra_plugins/example_launcher_plugin/example_launcher.py

import asyncio
import collections
import logging
import time
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Callable, ClassVar

import hydra_zen
import rich
import rich.box
import rich.table
from hydra.core.utils import JobReturn, JobStatus
from hydra.plugins.launcher import Launcher
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig, OmegaConf
from remote_slurm_executor.slurm_remote import RemoteSlurmJob
from submitit.helpers import _default_custom_logging

from cluv.cli.submit import submit
from cluv.cli.sync import fetch_results, get_active_remotes, sync
from cluv.config import CluvConfig, find_pyproject, get_cluv_config
from cluv.job import JobInfo, RunInfo, get_results_path, get_run_id
from cluv.remote import Remote

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
        ## NEW args:
        cluster: str = "first",  # which cluster to submit to.
        job_script: str
        | Path = "scripts/job.sh",  # the job script to run on the cluster. It should be set up to run the command passed to `submit` in its arguments.
        vram_gb: int | None = None,  # Enables job packing!
        checkpointing: bool = True,  # Enables job chunking (via job arrays!)
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
        super().__init__()
        self.cluster = cluster
        self.job_script = job_script
        # todo:
        self.vram_gb = vram_gb
        self.checkpointing = checkpointing

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

        self.synced_clusters: set[str] = set()
        self.cluster_remotes: dict[str, Remote] = {}
        self.cluv_config: CluvConfig | None = None

        self.chunking = False
        self.packing = False

        self._loop = asyncio.new_event_loop()

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
        logger.debug(f"{hydra_context=}, {task_function=}, {config=}")
        self.cluv_config = get_cluv_config()
        self._loop.run_until_complete(self.setup_async())

    async def setup_async(self) -> None:
        # Perhaps we could connect to all clusters here?
        if self.cluster == "first":
            remotes = await get_active_remotes()
        else:
            remotes = [await Remote.connect(self.cluster)]
        self.cluster_remotes = {remote.hostname: remote for remote in remotes}
        await sync([self.cluster] if self.cluster != "first" else None)

    def __del__(self):
        self._loop.close()

    def launch(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> Sequence[JobReturn]:
        return self._loop.run_until_complete(self.launch_jobs(job_overrides, initial_job_idx))

    async def launch_jobs(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> list[JobReturn]:
        assert self.cluv_config
        assert self.cluster_remotes
        cluster = self.cluster

        # TODO: Remove any 'hydra/launcher'-related configs!
        new_job_overrides = []
        for overrides in job_overrides:
            new_override = [
                override
                for override in overrides
                if not override.startswith(("hydra/launcher", "hydra.launcher", "launcher"))
            ]
            new_job_overrides.append(new_override)
        job_overrides = new_job_overrides

        # if self.vram_gb:
        # _packing_factor = 5
        # self.params["ntasks_per_gpu"] = 5
        # pack the jobs based on their VRAM requirements and the packing factor
        # job_specs = job_packing(job_overrides, packing_factor)
        cluster_results_dir = self.cluv_config.get_cluster_config(cluster).results_path
        assert self.cluster != "first", "todo"
        cluster_remote = self.cluster_remotes[self.cluster]
        cluster_results_dir = PurePosixPath(
            await cluster_remote.get_output(f"echo {cluster_results_dir}")
        )
        local_results_dir = get_results_path()

        _runid_template = get_run_id(
            cluster=cluster,
            job_id="%j",
            task_index="%t",
            array_job_id=None,
            doing_job_packing=False,
            doing_job_chunking=False,
        )

        sbatch_args = convert_submitit_style_params_to_sbatch_flags(self.params)
        # Drop the flags we don't want.
        sbatch_args = [
            arg
            for arg in sbatch_args
            if not arg.startswith(("--output=", "--wckey", "--job-name"))
            or "{folder}" in arg
            or "{command}" in arg
        ]

        job_infos: list[JobInfo] = []
        for override in job_overrides:
            # Use this so the output is where it would be if we used submitit.
            # It seems hard to configure the folder otherwise (Paths.stdout is a read-only property)
            # TODO: Save the command used for submission in the output folder as well, since we
            # don't generate a job script.
            job = await submit(
                cluster=cluster,
                job_script=Path(self.job_script),
                sbatch_args=[f"--output={cluster_results_dir}/{_runid_template}/%j_%t_log.out"],
                program_args=["python", "main.py", *override],
                _skip_sync=True,
            )
            assert job is not None
            job_id = job.job_id

            assert not self.chunking and not self.packing  # jobid is the "run id" for now.
            run_id = get_run_id(
                cluster=cluster,
                job_id=job_id,
                task_index=0,
                array_job_id=None,
                doing_job_packing=False,
                doing_job_chunking=False,
            )

            _cluster_job_results_path = cluster_results_dir / run_id
            # The path where the remote results will be synced locally.
            local_job_results_path = local_results_dir / run_id

            # TODO: Unclear if we should just use Job or if we actually need something like JobInfo.
            job = JobInfo(
                cluster=cluster,
                job_id=job_id,
                array_job_id=None,
                tasks=[
                    RunInfo(
                        cluster=cluster,
                        run_id=run_id,
                        results_path=local_job_results_path,
                        command=override,
                    )
                ],
            )
            job_infos.append(job)

        await monitor_jobs_async(job_infos, poll_interval_seconds=30)
        # await asyncio.gather(*(job.awaitable().wait(poll_interval=30) for job in submitit_jobs))

        await asyncio.gather(
            *(
                fetch_results(cluster_remote, self.cluv_config)
                for cluster_remote in self.cluster_remotes.values()
            )
        )

        # TODO: What is the 'results' in our case? We don't want to pickle/unpickle stuff.
        job_results: list[JobReturn] = []
        table = rich.table.Table(
            title="Jobs",
            box=rich.box.ROUNDED,
            show_lines=True,
            header_style="bold white on #1a1a2e",
            title_style="bold cyan",
            expand=True,
        )

        table.add_column("Run id", style="bold")
        table.add_column("Command", justify="right")
        table.add_column("State", justify="right")
        table.add_column("Output File", justify="right")
        cluv_config = get_cluv_config()
        for job in job_infos:
            for task_id, run in enumerate(job.tasks):
                out = next(
                    (
                        find_pyproject().parent
                        / Path(cluv_config.results_symlink)
                        / run.results_path
                    ).glob("*.out"),
                    run.results_path,
                )
                try:
                    out = out.relative_to(Path.cwd())
                except ValueError:
                    pass

                logger.info(f"Run {run.run_id} finished ({job.state}): Output: {out}")
                job_status = JobStatus.COMPLETED if job.state == "COMPLETED" else JobStatus.FAILED
                job_results.append(
                    JobReturn(
                        overrides=run.command,
                        working_dir=str(run.results_path),
                        status=job_status,
                    )
                )
                table.add_row(
                    run.run_id,
                    " ".join(run.command),
                    job.state,
                    str(out),
                    # style=row_style
                    end_section=(task_id == len(job.tasks) - 1),
                )
        rich.print(table)
        return job_results


async def monitor_jobs_async(
    jobs: Sequence[JobInfo],
    poll_interval_seconds: float = 30,
    test_mode: bool = False,
    custom_logging: Callable = _default_custom_logging,
) -> None:
    """Async version of `monitor_jobs` from submitit.

    Continuously monitors given jobs until they are all done or failed.

    Parameters
    ----------
    jobs: List[Jobs]
        A list of jobs to monitor
    poll_frequency: int
        The time (in seconds) between two refreshes of the monitoring.
        Can't be inferior to 30s.
    test_mode: bool
        If in test mode, we do not check the length of poll_frequency
    """

    if not test_mode:
        assert poll_interval_seconds >= 30, (
            "You can't refresh too often (>= 30s) to avoid overloading squeue"
        )

    n_jobs = len(jobs)
    if n_jobs == 0:
        print("There are no jobs to monitor")
        return

    job_arrays = [job.job_id for job in jobs]
    # job_arrays = ", ".join(sorted(set(str(job.job_id).split("_", 1)[0] for job in jobs)))
    print(f"Monitoring {n_jobs} jobs from job arrays {job_arrays} \n")

    submitit_jobs = [
        RemoteSlurmJob(
            job.cluster,
            folder="",
            job_id=str(job.job_id),
            tasks=list(range(len(job.tasks))),
            remote_dir_sync=None,
        )
        for job in jobs
    ]

    monitoring_start_time = time.time()
    while True:
        if not test_mode:
            submitit_jobs[0].get_info(mode="force")  # Force update once to sync the state
        state_jobs = collections.defaultdict(set)
        for i, job in enumerate(submitit_jobs):
            state_jobs[job.state.upper()].add(i)
            if job.done():
                state_jobs["DONE"].add(i)

        failed_job_indices = sorted(state_jobs["FAILED"])
        if len(state_jobs["DONE"]) == len(submitit_jobs):
            print(f"All jobs finished, jobs with indices {failed_job_indices} failed", flush=True)
            break

        custom_logging(monitoring_start_time, n_jobs, state_jobs)
        await asyncio.sleep(poll_interval_seconds)

    print(
        f"Whole process is finished, took {int((time.time() - monitoring_start_time) / 60)} minutes"
    )


@hydra_zen.hydrated_dataclass(
    target=CluvLauncher, populate_full_signature=True, hydra_convert="object"
)
class CluvLauncherConfig:
    ...

    # cluster: str


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
def convert_submitit_style_params_to_sbatch_flags(
    submitit_launcher_params: dict[str, Any],
) -> list[str]:
    from submitit.slurm.slurm import _make_sbatch_string

    generated_sbatch_script = _make_sbatch_string(
        "{command}", folder="{folder}", **submitit_launcher_params
    )
    return [
        line.removeprefix("#SBATCH").strip()
        for line in generated_sbatch_script.splitlines()
        if line.startswith("#SBATCH")
    ]
