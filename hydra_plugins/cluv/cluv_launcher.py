# https://github.com/facebookresearch/hydra/blob/main/examples/plugins/example_launcher_plugin/hydra_plugins/example_launcher_plugin/example_launcher.py

import asyncio
import collections
import inspect
import logging
import shlex
import time
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Callable, ClassVar, Literal

import hydra_zen
import omegaconf
import rich
import rich.box
import rich.live
import rich.table
from hydra.core.utils import JobReturn, JobStatus
from hydra.plugins.launcher import Launcher
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig, OmegaConf
from remote_slurm_executor.slurm_remote import RemoteSlurmJob
from submitit.slurm.slurm import SlurmExecutor, _make_sbatch_string

from cluv.cache import ProjectStateOnCluster, read_cache, write_cache
from cluv.cli.submit import display_commands, submit
from cluv.cli.sync import expandvars, fetch_results, get_active_remotes, sync
from cluv.config import CluvConfig, find_pyproject, get_cluv_config
from cluv.job import JobInfo, RunInfo, current_run_info, get_results_path, get_run_id
from cluv.remote import Remote
from cluv.utils import set_context

logger = logging.getLogger(__name__)

current_job: RunInfo | None = None


def cluv_resolver(attr: str, default: str | None = None) -> str | None:
    """OmegaConf resolver to access Cluv job info in Hydra configs.

    Usage in Hydra config: ${cluv:attr, default} where `attr` is an attribute of the current job (e.g. "results_path")
    and `default` is an optional default value to return if the attribute is not set in the current job.
    """
    global current_job
    if current_job is None:
        current_job = current_run_info()

    if default is not None:
        return getattr(current_job, attr, default)
    return getattr(current_job, attr)


omegaconf.OmegaConf.register_new_resolver("cluv", cluv_resolver, replace=True)


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
        # The job script to run on the cluster.
        # It should use "$@" to capture and pass down the arguments to the python script.
        # When unset, will use the value from the Cluv config.
        job_script: str | Path | None = None,
        vram_gb: int | None = None,  # Enables job packing!
        checkpointing: bool = False,  # Enables job chunking (via job arrays!)
        ## Submitit arguments:
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
        job_name: str = "cluv-${hydra.job.name}",
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
        time: str | int | None = None,  # sbatch native way of passing it.
        # TODO: Used by submitit, needs to be translated somehow.
        timeout_min: str | int | None = None,
        use_srun: bool = True,
        wckey: str = "cluv",
        additional_parameters: dict | None = None,
        tasks_per_node: int | None = None,
        mem_gb: int | None = None,
        # **kwargs,
    ) -> None:
        super().__init__()
        self.cluster = cluster
        self.job_script = PurePosixPath(job_script) if job_script else None
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

        if timeout_min is not None:
            assert time is None, "can't use both time and timeout_min"

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

        self.chunking = self.checkpointing
        self.packing = self.vram_gb is not None

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
        if hasattr(self, "_loop"):
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
        assert self.task_function
        cluster = self.cluster
        # NOTE: Assumes that passing "python path/to/script.py *overrides" to the job script will work.
        # (It does work for the example).
        # TODO: Couldn't we use `sys.argv` or some info about the run command from the Hydra context to help?
        assert inspect.isfunction(self.task_function)
        module_path = inspect.getsourcefile(self.task_function)
        assert module_path
        module_path = Path(module_path).relative_to(find_pyproject().parent)
        prefix = ["python", str(module_path)]

        # TODO: Remove any 'hydra/launcher'-related configs. This isn't as easy as it sounds!
        new_job_overrides = []
        for overrides in job_overrides:
            new_override = [
                override
                for override in overrides
                if not override.startswith(("hydra/launcher", "hydra.launcher", "launcher"))
            ]
            new_override = prefix + new_override
            new_job_overrides.append(new_override)
        job_overrides = new_job_overrides

        # TODO: Add job packing! :)
        # if self.vram_gb:
        # _packing_factor = 5
        # self.params["ntasks_per_gpu"] = 5
        # pack the jobs based on their VRAM requirements and the packing factor
        # job_specs = job_packing(job_overrides, packing_factor)

        job_script = self.job_script

        if job_script is None and cluster != "first":
            # Find the job_script to use for the chose cluster from the cluv config.
            # This avoids doing it for every job.
            job_script = self.cluv_config.get_cluster_config(cluster).job_script_path
            if job_script is None:
                raise ValueError(
                    f"No job script specified for cluster {cluster}. Please specify one using the "
                    f"`job_script` either in the Hydra launcher config, or in your pyproject.toml "
                    f"config for Cluv (for all clusters), or in the overrides block for the "
                    f"{cluster} cluster."
                )

        job_infos = await run_sweep(
            job_overrides,
            cluster,
            cluv_config=self.cluv_config,
            cluster_remotes=self.cluster_remotes,
            job_script=job_script,
            params=self.params,
            chunking=self.checkpointing,
            packing=self.vram_gb is not None,
        )

        job_returns = _jobs_to_hydra_jobreturn_format(job_infos, get_results_path())
        rich.print(get_jobs_table(job_infos, get_results_path()))
        return job_returns


async def run_sweep(
    job_commands: list[list[str]],
    cluster: str | Literal["first"],
    cluv_config: CluvConfig,
    cluster_remotes: dict[str, Remote],
    job_script: PurePosixPath | None,
    params: dict[str, Any],
    chunking: bool,
    packing: bool,
) -> list[JobInfo]:
    if cluster == "first":
        # submit_first adds the `SBATCH_OUTPUT` env var that should work as expected.
        output_args = []
    else:
        _cluster_remote = cluster_remotes[cluster]
        _cluster_results_dir = cluv_config.get_cluster_config(cluster).results_path
        _cluster_results_dir = await expandvars(_cluster_remote, _cluster_results_dir)
        _runid_template = get_run_id(
            cluster=cluster,
            job_id="%j",
            task_index="%t",
            array_job_id="%A" if chunking else None,
            doing_job_packing=packing,
            doing_job_chunking=chunking,
        )
        # TODO: If we leave the '%t' in the output file path, there are files
        output_args = [f"--output={_cluster_results_dir}/{_runid_template}/%j.out"]

    local_results_dir = get_results_path()

    sbatch_args = convert_submitit_style_params_to_sbatch_flags(params)

    async def _submit_one(job_command: list[str]) -> JobInfo:
        # Use this so the output is where it would be if we used submitit.
        # It seems hard to configure the folder otherwise (Paths.stdout is a read-only property)
        # TODO: Save the command used for submission in the output folder as well, since we
        # don't generate a job script.
        with set_context(display_commands, True):
            job = await submit(
                # NOTE: Always the originally requested cluster (e.g. "first") for every job in
                # the sweep. Each job independently races across clusters; the cluster that one
                # job happens to land on must never be reused for another job's submission.
                cluster=cluster,
                job_script=Path(job_script) if job_script is not None else None,
                # TODO: Ugly. This passes all the sbatch args as flags. There might be a cleaner way
                # to do this, but I can't see it right now.
                sbatch_args=sbatch_args + output_args,
                program_args=job_command,
                _skip_sync=True,
            )
        if job is None:
            raise RuntimeError("Unable to submit jobs! See the error traces above for details.")
        # The concrete hostname this job actually landed on (e.g. when `cluster` is "first"),
        # needed so we know where to later query this job's state. Scoped to this job only.
        resolved_cluster = job.cluster
        job_id = job.job_id

        assert not chunking and not packing  # jobid is the "run id" for now.
        run_id = get_run_id(
            cluster=resolved_cluster,
            job_id=job_id,
            task_index=0,
            # TODO: unsure about this one:
            array_job_id=job_id if chunking else None,
            doing_job_packing=packing,
            doing_job_chunking=chunking,
        )

        # The path where the remote results will be synced locally.
        local_job_results_path = local_results_dir / run_id
        # where they came from on the remote.
        # _cluster_job_results_path = cluster_results_dir / run_id

        # TODO: Unclear if we should just reuse Job or if we actually need something like JobInfo.
        return JobInfo(
            cluster=resolved_cluster,
            job_id=job_id,
            array_job_id=None,
            tasks=[
                RunInfo(
                    cluster=resolved_cluster,
                    run_id=run_id,
                    results_path=local_job_results_path,
                    command=job_command,
                )
            ],
        )

    # Submit every job in the sweep concurrently instead of waiting for each one before starting
    # the next. Each submission is its own named Task so concurrent `submit_first()` calls can be
    # told apart (see `cluv.tui`, which fuses their live "waiting for a job to start" tables).
    submit_tasks = [
        asyncio.create_task(_submit_one(job_command), name=shlex.join(job_command))
        for job_command in job_commands
    ]
    job_infos: list[JobInfo] = list(await asyncio.gather(*submit_tasks))

    # Creates a rich.Live table and updates it as the status of the jobs change.
    await monitor_jobs_async(job_infos, poll_interval_seconds=30)
    project_states = {
        cluster: read_cache().project_states.get(cluster) or ProjectStateOnCluster()
        for cluster in cluster_remotes.keys()
    }

    await asyncio.gather(
        *(
            fetch_results(cluster_remote, cluv_config, project_states[cluster])
            for cluster, cluster_remote in cluster_remotes.items()
        )
    )
    cache = read_cache()
    for cluster, updated_project_state in project_states.items():
        cache.project_states[cluster] = updated_project_state
    write_cache(cache)

    return job_infos


def _jobs_to_hydra_jobreturn_format(
    job_infos: list[JobInfo], local_results_dir: Path
) -> list[JobReturn]:
    job_returns: list[JobReturn] = []
    for job in job_infos:
        for _task_id, run in enumerate(job.tasks):
            out = next(
                (
                    find_pyproject().parent
                    / Path(get_cluv_config().results_symlink)
                    / run.results_path.relative_to(local_results_dir)
                ).glob("*.out"),
                run.results_path,
            )
            try:
                out = out.relative_to(Path.cwd())
            except ValueError:
                pass

            logger.info(f"Run {run.run_id} finished ({job.state}): Output: {out}")
            job_status = JobStatus.COMPLETED if job.state == "COMPLETED" else JobStatus.FAILED
            job_returns.append(
                JobReturn(
                    overrides=run.command,
                    working_dir=str(run.results_path),
                    status=job_status,
                )
            )
    return job_returns


def get_jobs_table(job_infos: list[JobInfo], local_results_dir: Path) -> rich.table.Table:
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
                    / run.results_path.relative_to(local_results_dir)
                ).glob("*.out"),
                run.results_path,
            )
            try:
                out = out.relative_to(Path.cwd())
            except ValueError:
                pass
            table.add_row(
                run.run_id,
                " ".join(run.command),
                job.state,
                str(out),
                # style=row_style
                end_section=(task_id == len(job.tasks) - 1),
            )

    return table


def _build_monitoring_table(
    jobs: Sequence[JobInfo],
    submitit_jobs: Sequence[RemoteSlurmJob],
    monitoring_start_time: float,
) -> rich.table.Table:
    elapsed_minutes = int((time.time() - monitoring_start_time) / 60)
    table = rich.table.Table(
        title=f"Monitoring {len(jobs)} jobs (elapsed: {elapsed_minutes}min)",
        box=rich.box.ROUNDED,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
        expand=True,
    )
    table.add_column("Run id", style="bold")
    table.add_column("Command", justify="right")
    table.add_column("State", justify="right")
    for job, submitit_job in zip(jobs, submitit_jobs):
        run = job.tasks[0]
        table.add_row(run.run_id, " ".join(run.command), submitit_job.state)
    return table


async def monitor_jobs_async(
    jobs: Sequence[JobInfo],
    poll_interval_seconds: float = 30,
    test_mode: bool = False,
) -> None:
    """Async version of `monitor_jobs` from submitit.

    Continuously monitors given jobs until they are all done or failed, displaying their
    status in a live-updating table.

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

    submitit_jobs = [
        RemoteSlurmJob(
            job.cluster,
            folder="",
            job_id=str(job.job_id),
            tasks=list(range(len(job.tasks))),
            remote_dir_sync=None,  # type: ignore
        )
        for job in jobs
    ]

    monitoring_start_time = time.time()
    with rich.live.Live(
        _build_monitoring_table(jobs, submitit_jobs, monitoring_start_time),
        refresh_per_second=4,
    ) as live:
        while True:
            if not test_mode:
                submitit_jobs[0].get_info(mode="force")  # Force update once to sync the state
            state_jobs = collections.defaultdict(set)
            for i, job in enumerate(submitit_jobs):
                state_jobs[job.state.upper()].add(i)
                if job.done():
                    state_jobs["DONE"].add(i)

            live.update(_build_monitoring_table(jobs, submitit_jobs, monitoring_start_time))

            if len(state_jobs["DONE"]) == len(submitit_jobs):
                break

            await asyncio.sleep(poll_interval_seconds)

    failed_job_indices = sorted(state_jobs["FAILED"])
    print(f"All jobs finished, jobs with indices {failed_job_indices} failed", flush=True)
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

    # note: translate the parameters the same way they would have been through the SlurmExecutor.
    eq_dict = SlurmExecutor._equivalence_dict()
    params = submitit_launcher_params.copy()
    params = {eq_dict.get(k, k): v for k, v in params.items()}
    generated_sbatch_script = _make_sbatch_string("{command}", folder="{folder}", **params)
    raw = [
        line.removeprefix("#SBATCH").strip()
        for line in generated_sbatch_script.splitlines()
        if line.startswith("#SBATCH")
    ]
    # Drop the flags we don't want.
    sbatch_args = [
        arg
        for arg in raw
        if not any(
            thing in arg
            for thing in ("--output=", "--wckey", "--job-name", "{folder}", "{command}")
        )
    ]
    return sbatch_args
