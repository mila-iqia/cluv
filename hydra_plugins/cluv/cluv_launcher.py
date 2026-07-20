"""Cluv launcher plugin for Hydra.

Follows the same signature as the submitit launcher to make it easier to transition for researchers.

+ Allows launching jobs on remote slurm clusters.
+ Syncs back results

TODO: Also allows job packing (multiple runs per GPU) and job chunking (splitting a long job into multiple shorter jobs).
"""

import asyncio
import collections
import inspect
import itertools
import logging
import os.path
import time
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Callable, ClassVar, Literal, NewType

import hydra_zen
import omegaconf
import rich
import rich.box
import rich.live
import rich.table
import submitit.core.utils
from hydra.core.utils import JobReturn, JobStatus
from hydra.plugins.launcher import Launcher
from hydra.types import HydraContext, TaskFunction
from omegaconf import DictConfig, OmegaConf
from remote_slurm_executor.slurm_remote import RemoteSlurmJob
from submitit import SlurmJob
from submitit.slurm.slurm import SlurmExecutor, _make_sbatch_string

from cluv.cache import ProjectStateOnCluster, read_cache, write_cache
from cluv.cli.submit import display_commands, submit
from cluv.cli.sync import expandvars, fetch_results, get_active_remotes, sync
from cluv.config import CluvConfig, find_pyproject, get_cluv_config
from cluv.job import JobInfo, RunInfo, current_run_info, get_results_path, get_run_id
from cluv.remote import Remote
from cluv.utils import current_cluster, set_context

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

ClusterHostname = NewType("ClusterHostname", str)


# Made this a dataclass to avoid having an ugly default repr, but it causes issues with
# hydra-auto-schema because it tries to create a schema for everything here.
# @dataclasses.dataclass(init=False)
class CluvLauncher(Launcher):
    _EXECUTOR: ClassVar[str] = ""

    submitit_params: dict[str, Any]
    config: DictConfig | None = None
    task_function: TaskFunction | None = None
    sweep_configs: TaskFunction | None = None
    hydra_context: HydraContext | None = None
    executor: None

    # same signature as the submitit plugin to make it easier for people to transition.
    def __init__(
        self,
        ## NEW args:
        cluster: Literal["current", "first"] | ClusterHostname = "current",
        job_script: str | Path | None = None,
        autocommit: bool = False,
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
        num_gpus: int | None = None,  # deprecated in the submitit launcher, removed here.
        partition: str | None = None,
        qos: str | None = None,
        setup: list[str] | None = None,
        signal_delay_s: int = 90,
        srun_args: list[str] | None = None,
        stderr_to_stdout: bool = True,  # changed!
        time: str | int | None = None,  # sbatch native way of passing it.
        timeout_min: str | int | None = None,
        use_srun: bool = True,
        wckey: str = "cluv",
        additional_parameters: dict | None = None,
        tasks_per_node: int | None = None,
        mem_gb: int | None = None,
        # **kwargs,
    ) -> None:
        """

        Args:
            cluster: Which cluster to run the job on.
                - 'current' (default): Use the current cluster (same behaviour as the submitit launcher)
                - 'first': Try all enabled clusters (listed in the cluv config in pyproject.toml), keep the first running job.
                - hostname (str): Use a specific cluster (for example 'mila'/'rorqual'/'tamia'/etc.)
            job_script: The path to the job script to run on the cluster.
                It should use something like `srun uv run "$@"` to capture and pass down the arguments to the python script.
                When unset, selects the job script based on the Cluv config in pyproject.toml
            autocommit: Whether to create a commit instead of raising an error, if the git workspace is dirty.
            vram_gb:  The required amount of GPU memory (VRAM) per run.
                TODO: This will be used to automatically stack multiple runs per GPU in the future.
            checkpointing: Whether the submitted job has checkpointing support.
                TODO: This will be used to automatically chunk the jobs into shorter slices for faster execution in the future.
            array_parallelism: Maximum number of simultaneously running jobs.
            comment: Passed down to `sbatch` as the argument of the same name. (Same as the submitit launcher).
            constraint:     Passed down to `sbatch`. Same as the submitit launcher.
            cpus_per_gpu:   Passed down to `sbatch`.
            cpus_per_task:  Passed down to `sbatch`.
            dependency:     Passed down to `sbatch`.
            exclude:        Passed down to `sbatch`.
            exclusive:      Passed down to `sbatch`.
            gpus_per_node:  Passed down to `sbatch`.
            gpus_per_task:  Passed down to `sbatch`.
            gres:           Passed down to `sbatch`.
            job_name:       The job name. Will always be prepended with 'cluv-' for book-keeping/visibility purposes.
            mail_type:      Passed down to `sbatch`.
            mail_user:      Passed down to `sbatch`.
            mem:            Passed down to `sbatch`.
                            Prefer using this over `mem_gb` if possible, since this matches the `sbatch` argument of the same name.
            mem_per_cpu:    Passed down to `sbatch`.
            mem_per_gpu:    Passed down to `sbatch`.
            nodelist:       Passed down to `sbatch`.
            nodes:          Passed down to `sbatch`.
            ntasks_per_node:    Passed down to `sbatch`.
            num_gpus:       Copied from the submitit launcher, where it is deprecated. Consider using `gpus-per-task` and scaling `ntasks` instead.
            partition:      Passed down to `sbatch`.
            qos:            Passed down to `sbatch`.
            setup:  NOT used.
                    This argument is copied from the submitit launcher, but it is not supported.
                    Add your setup lines directly in the job script file instead. (See `job_script`).

            signal_delay_s: Argument copied from the submitit launcher. Not used at the moment.
            srun_args:      NOT used.
                            This argument is copied from the submitit launcher, but it is not supported.
                            Add your setup lines directly in the job script file instead. (See `job_script`).
            stderr_to_stdout: Argument copied from the submitit launcher. Not used at the moment.
            use_srun:         Argument copied from the submitit launcher. Not used, since whether or not to
                              use `srun` is simply whether you are using it in your job script or not (we
                              recommend that you do!)
            wckey:      Argument copied from the submitit launcher. Not used at the moment.
            additional_parameters: Argument copied from the submitit launcher. Additional parameters to pass to `sbatch`.
                                Will override any of the other passed parameters. Can be used to pass argument to sbatch
                                that are not listed in the constructor arguments here.
            tasks_per_node:  Copied from the submitit launcher. Can either use this or `ntasks_per_node`, not both.\
                            Consider use `ntasks_per_node` instead, since it matches the sbatch argument of the same name.
            mem_gb:         Copied from the submitit launcher. Memory request in gigabytes (GB).
                            Consider use `mem` instead, since it matches the sbatch argument of the same name.
        """
        super().__init__()

        this_cluster = current_cluster()
        if cluster == "current":
            if this_cluster is None:
                raise RuntimeError(
                    f"The `cluster` argument passed to {CluvLauncher.__name__} was 'current', "
                    f"but you don't appear to be on a Slurm cluster (or the current slurm cluster is unknown).\n"
                    f"If you are on a local machine, set `cluster` to the hostname of a cluster, or use 'first' to use "
                    f"all clusters and keep the fastest to start your job."
                )
            cluster = ClusterHostname(this_cluster)
        self.cluster = cluster
        self.job_script = PurePosixPath(job_script) if job_script else None
        self.autocommit = autocommit

        self.vram_gb = vram_gb
        self.checkpointing = checkpointing

        if setup:
            # TODO: Check if the lines are already in the job script, and if so, just ignore the fact that it is set here.
            if (
                self.job_script
                and Path(self.job_script).exists()
                and "\n".join(setup) in Path(self.job_script).read_text()
            ):
                # ignore
                pass
            else:
                raise RuntimeError(
                    f"The `setup` argument usually used with the submitit launcher is not supported (or needed)!\n"
                    f"The cluv launcher instead reuses a provided job script (pointed to by the `job_script` argument, "
                    f"or in the `[tool.cluv]` section of your project's `pyproject.toml` file) for all jobs.\n"
                    f"Consider adding these lines to your job script instead. Once you add them to the file, "
                    f"you can leave them in your config, and this error will not be raised."
                    f"({job_script=}, {setup=})"
                )
        setup = setup or []

        if srun_args:
            logger.warning(
                UserWarning("The cluv launcher ignores the value of the 'srun_args' argument.")
            )

        additional_parameters = additional_parameters or {}

        if mem_gb is not None:
            if mem is not None:
                raise ValueError("can't use both mem and mem_gb")
            mem = f"{mem_gb}GB"
            mem_gb = None

        if timeout_min is not None:
            if time is not None:
                raise ValueError(f"Can't use both timeout_min ({timeout_min}) and time ({time}).")
            # An int is interpreted as a number of minutes (See https://slurm.schedmd.com/sbatch.html#OPT_time)
            time = str(timeout_min)
            timeout_min = None

        if tasks_per_node is not None:
            if ntasks_per_node is not None:
                raise ValueError("can't use both tasks_per_node and ntasks_per_node")
            ntasks_per_node = tasks_per_node
            tasks_per_node = None

        if ntasks_per_node is not None:
            additional_parameters["ntasks-per-node"] = ntasks_per_node

        self.array_parallelism = array_parallelism
        submitit_params = dict(
            account=account,
            array_parallelism=self.array_parallelism,
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
        # Imitating the submitit launcher.
        self.submitit_params = {}
        for k, v in submitit_params.items():
            if OmegaConf.is_config(v):
                v = OmegaConf.to_container(v, resolve=True)
            self.submitit_params[k] = v

        self.cluster_remotes: dict[str, Remote | None] = {}
        self.cluv_config: CluvConfig | None = None

        self.chunking = self.checkpointing
        self.packing = self.vram_gb is not None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
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
        # TODO: Perhaps be a bit more forgiving and allow using the launcher without a cluv config, in which case
        # we print a warning and assume some defaults when possible?
        # ALSO: If the user doesn't have an SSH config on the current cluster, we could probably set one up by importing
        # the functions from `milatools`! (and display instructions for adding `AddKeysToAgent yes` in your local SSH config, so
        # that your local DRAC keys can also be used to connect from Mila to DRAC).
        self.cluv_config = get_cluv_config()
        self._loop.run_until_complete(self.setup_async())

    async def setup_async(self) -> None:
        """Async setup. happens once before submitting any jobs. Connects to clusters if needed.

        Raises:
            RuntimeError: If unable to connect to the requested cluster, or using 'current' while not on a Slurm cluster.
        """
        this_cluster = current_cluster()

        cluster_remotes: dict[str, Remote | None]
        if self.cluster == "current":
            if this_cluster is None:
                raise RuntimeError(
                    f"The `cluster` argument passed to the {CluvLauncher.__name__} was 'current', "
                    f"but you don't appear to be on a Slurm cluster (or the current slurm cluster is unknown).\n"
                    f"If you are on a local machine, set 'cluster' to the hostname of a cluster, or 'first' to use "
                    f"all clusters and keep the fastest to start your job."
                )
            cluster_remotes = {this_cluster: None}
        elif self.cluster == "first":
            remotes = await get_active_remotes()
            cluster_remotes = {remote.hostname: remote for remote in remotes}
            if this_cluster:
                # Also consider the current cluster as a potential place to run jobs on.
                cluster_remotes[this_cluster] = None
        elif this_cluster and self.cluster == this_cluster:
            cluster_remotes = {this_cluster: None}
        else:
            cluster_remotes = {self.cluster: await Remote.connect(self.cluster)}
        self.cluster_remotes = cluster_remotes

        clusters_to_sync_with = [
            hostname for hostname, remote in self.cluster_remotes.items() if remote
        ]
        if clusters_to_sync_with:
            await sync(clusters_to_sync_with)

    def __del__(self):
        if hasattr(self, "_loop"):
            self._loop.close()

    def launch(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> Sequence[JobReturn]:
        logger.debug(
            f"About to launch {len(job_overrides)} jobs, with {initial_job_idx=} and {self.cluster=}."
        )

        # Small wrapper around `self.launch_jobs` that respects the `array_parallelism` throttling.
        async def _launch_jobs(job_overrides, array_parallelism: int):
            first_job_idx = initial_job_idx
            job_results: list[JobReturn] = []
            for batch_index, job_overrides_batch in enumerate(
                itertools.batched(job_overrides, array_parallelism or len(job_overrides))
            ):
                logger.debug(f"Launching batch #{batch_index} of {len(job_overrides_batch)} jobs.")
                job_batch_results = await self.launch_jobs(job_overrides_batch, first_job_idx)
                job_results.extend(job_batch_results)

                first_job_idx += len(job_overrides_batch)

            return job_results

        return self._loop.run_until_complete(
            _launch_jobs(job_overrides, array_parallelism=self.array_parallelism)
        )

    async def launch_jobs(
        self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int
    ) -> list[JobReturn]:
        assert self.cluv_config, "setup should have been called"
        assert self.cluster_remotes, "setup should have been called"
        assert self.task_function, "setup should have been called"
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
        # This will definitely not work if an "experiment" config is used, that includes the hydra.launcher settings!
        # What we CRITICALLY don't want to happen is for the `cluv` launcher to then be used by that job.
        # Submitit side-steps this issue probably because of the fact that it pickles something to run on the cluster.
        new_job_overrides = []
        for overrides in job_overrides:
            new_override = [
                override
                for override in overrides
                if not override.startswith(("hydra/launcher", "hydra.launcher", "launcher"))
            ]
            # TODO: Perhaps adding 'hydra.mode=RUN' explicitly would counteract this and disabling any launcher-related settings?
            # Need to take a closer look and test out the combinations from https://hydra.cc/docs/1.3/tutorials/basic/running_your_app/multi-run/
            new_override = prefix + new_override  # + ["hydra.mode=RUN"]
            new_job_overrides.append(new_override)
        job_overrides = new_job_overrides

        sbatch_args = convert_submitit_style_params_to_sbatch_flags(self.submitit_params)

        # TODO: Add job packing! :)
        # if self.packing:
        #    assert self.vram_gb
        #    requested_gpu_type = ...
        #    gpu_vram = ...
        #    ntasks_per_gpu = gpu_vram // self.vram_gb
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
            job_commands=job_overrides,
            cluster=cluster,
            cluv_config=self.cluv_config,
            cluster_remotes=self.cluster_remotes,
            job_script=job_script,
            autocommit=self.autocommit,
            sbatch_args=sbatch_args,
            chunking=self.checkpointing,
            packing=self.packing,
        )
        results_path = get_results_path()
        job_returns = _jobs_to_hydra_jobreturn_format(job_infos, results_path)
        rich.print(
            get_jobs_table(
                job_infos,
                results_path,
                results_symlink_from_cluv_config=self.cluv_config.results_symlink,
            )
        )
        return job_returns


async def run_sweep(
    *,
    job_commands: list[list[str]],
    cluster: str | Literal["first"],
    cluv_config: CluvConfig,
    cluster_remotes: dict[str, Remote | None],
    job_script: PurePosixPath | None,
    sbatch_args: dict[str, str | None],
    autocommit: bool,
    chunking: bool,
    packing: bool,
) -> list[JobInfo]:
    if cluster == "first":
        # submit_first adds the `SBATCH_OUTPUT` env var that should work as expected.
        output_args = []
    else:
        _cluster_remote = cluster_remotes[cluster]
        _cluster_results_dir = cluv_config.get_cluster_config(cluster).results_path
        if _cluster_remote:
            _cluster_results_dir = await expandvars(_cluster_remote, _cluster_results_dir)
        else:
            # no remote, we are using the current cluster
            _cluster_results_dir = Path(os.path.expandvars(_cluster_results_dir))
        _runid_template = get_run_id(
            cluster=cluster,
            job_id="%j",
            task_index="%t",
            array_job_id="%A" if chunking else None,
            doing_job_packing=packing,
            doing_job_chunking=chunking,
        )
        # TODO: If we leave the '%t' in the output file path, there are weid generated files?
        output_args = [f"--output={_cluster_results_dir}/{_runid_template}/%j.out"]

    local_results_dir = get_results_path()
    sbatch_flags = [f"{k}={v}" if v is not None else f"{k}" for k, v in sbatch_args.items()]

    job_infos: list[JobInfo] = []
    for job_command in job_commands:
        # Use this so the output is where it would be if we used submitit.
        # It seems hard to configure the folder otherwise (Paths.stdout is a read-only property)
        # TODO: Save the command used for submission in the output folder as well, since we
        # don't generate a job script?
        with set_context(display_commands, True):
            job = await submit(
                cluster=cluster,
                job_script=Path(job_script) if job_script is not None else None,
                # This passes all the sbatch args as flags.
                # TODO: Some of these flags might conflict with the header of the job script! 🤔
                # We will have to think about this once we add job packing with --ntasks-per-gpu, which will
                # conflict with the `--gpus-per-task` flag which might be hard-coded in the job script header.
                # At that point, maybe we shouldn't have much in the job script header, and have cluv add almost everything via sbatch args?
                sbatch_args=sbatch_flags + output_args,
                program_args=job_command,
                autocommit=autocommit,
                _skip_sync=True,
            )
        if job is None:
            raise RuntimeError("Unable to submit jobs! See the error traces above for details.")
        # In the case where `cluster` was 'first', it now gets updated to the cluster that was actually
        # selected to run the job. This avoids later calls doing things like `ssh first`.
        cluster = job.cluster
        job_id = job.job_id

        assert not chunking and not packing  # jobid is the "run id" for now.
        run_id = get_run_id(
            cluster=cluster,
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
        job = JobInfo(
            cluster=cluster,
            job_id=job_id,
            array_job_id=None,
            tasks=[
                RunInfo(
                    cluster=cluster,
                    run_id=run_id,
                    results_path=local_job_results_path,
                    command=job_command,
                )
            ],
        )
        job_infos.append(job)

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
            if cluster_remote is not None
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
        for task_id, run in enumerate(job.tasks):
            output_file = next(
                (
                    find_pyproject().parent
                    / Path(get_cluv_config().results_symlink)
                    / run.results_path.relative_to(local_results_dir)
                ).glob("*.out"),
                run.results_path,
            )
            try:
                output_file = output_file.relative_to(Path.cwd())
            except ValueError:
                pass

            logger.info(f"Run {run.run_id} finished ({job.state}): Output: {output_file}")
            job_status = JobStatus.COMPLETED if job.state == "COMPLETED" else JobStatus.FAILED
            job_returns.append(
                JobReturn(
                    overrides=run.command,
                    working_dir=str(run.results_path),
                    status=job_status,
                    _return_value=(
                        # submitit.core.utils.FailedJobError(
                        #     f"Job {run.run_id} failed, see the output file {out} for more info."
                        # )
                        # Mimic the output produced by the submitit launcher in case of error, which includes the error file.
                        submitit.core.utils.FailedJobError(
                            f"Job (task={task_id}) failed during processing with trace:\n"
                            f"----------------------\n{output_file.read_text()}\n"
                            "----------------------\n"
                            f"You can check full logs with 'job.stderr({task_id})' and 'job.stdout({task_id})'"
                            f"or at paths:\n  - {output_file}\n"
                        )
                        if job_status is JobStatus.FAILED
                        else None
                    ),
                )
            )
    return job_returns


def get_jobs_table(
    job_infos: list[JobInfo], local_results_dir: Path, results_symlink_from_cluv_config: str
) -> rich.table.Table:
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

    for job in job_infos:
        for task_id, run in enumerate(job.tasks):
            out = next(
                (
                    find_pyproject().parent
                    / Path(results_symlink_from_cluv_config)
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
    submitit_jobs: Sequence[SlurmJob | RemoteSlurmJob],
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
    table.add_column("Cluster", style="bold")
    table.add_column("Run id", style="bold")
    table.add_column("Command", justify="right")
    table.add_column("State", justify="right")
    for job, submitit_job in zip(jobs, submitit_jobs):
        run = job.tasks[0]
        table.add_row(run.cluster, run.run_id, " ".join(run.command), submitit_job.state)
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
    here = current_cluster()
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
            # TODO: Unclear if this makes sense when tasks>1 (for example when doing job packing).
            folder=job.tasks[0].results_path,
            job_id=str(job.job_id),
            tasks=list(range(len(job.tasks))),
            remote_dir_sync=None,  # type: ignore
        )
        if job.cluster != here
        else SlurmJob(
            # TODO: Unclear if this makes sense when tasks>1 (for example when doing job packing).
            folder=job.tasks[0].results_path,
            job_id=str(job.job_id),
            tasks=list(range(len(job.tasks))),
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
                # Call [Remote]SlurmJob.get_info(mode="force") once for each cluster.
                # For RemoteSlurmJob, this calls sacct over ssh. For SlurmJob, it uses
                # sacct locally.
                for cluster, job in {
                    (job.cluster if isinstance(job, RemoteSlurmJob) else None): job
                    for job in submitit_jobs
                }.items():
                    logger.debug(f"Calling sacct for cluster {cluster} to update job info.")
                    job.get_info(mode="force")  # Force update once to sync the state
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
) -> dict[str, str | None]:
    """Get the sbatch flags given the submitit-launcher-style 'params' dict.

    When using the submitit launcher, there is this `params` dict with a mishmash of slurm/submitit arguments and very bad info.
    (You have to dig down into the submitit internals (SlurmExecutor to be exact) to find all the values, their types, etc.
    Submitit uses this dictionary to generate an sbatch script.

    Here, we don't generate a new job script, so we want to get the sbatch arguments that would be equivalent.

    Instead of writing something new, we instead just reuse the whole submitit thing, and extract out the arguments
    from their generated sbatch script.
    """

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
    sbatch_args_dict = {}
    for flag in sbatch_args:
        key, _, val = flag.partition("=")
        if val:
            sbatch_args_dict[key] = val
        else:
            sbatch_args_dict[key] = None
    return sbatch_args_dict
