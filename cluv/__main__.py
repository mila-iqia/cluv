"""CLUV: Tool to use UV with multiple clusters."""

# todo: typer doesn't quite work for commands that need the argparse.REMAINDER feature like `run` and `launch`.
# It requires you to pass the command in quotes, but I'd like to be able
# to do `cluv run ls -l` for example. Here it errors out saying "no -l option".
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import subprocess
import sys
import typing
from typing import Callable

import rich
import rich.logging
import rich_argparse
import simple_parsing

from .cli.init import init
from .cli.login import login
from .cli.run import run
from .cli.status import status
from .cli.submit import submit
from .cli.sync import sync
from .utils import console

logger = logging.getLogger(__name__)
if typing.TYPE_CHECKING:
    Subparsers = argparse._SubParsersAction[simple_parsing.ArgumentParser]


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    # argparse consumes '--' before REMAINDER sees it, so we extract program
    # args (everything after the first '--' following 'submit') before parsing.
    submit_program_args: list[str] = []
    try:
        sub_idx = argv.index("submit")
        sep_idx = argv.index("--", sub_idx + 1)
        submit_program_args = list(argv[sep_idx + 1 :])
        argv = list(argv[:sep_idx])
    except ValueError:
        pass

    parser = simple_parsing.ArgumentParser(
        description=__doc__,
        formatter_class=rich_argparse.RichHelpFormatter,
        epilog="For more information, see the documentation. You rock.",
    )
    _add_v_arg(parser)  # add -v/--verbose on the top-level parser.

    subparsers = parser.add_subparsers(dest="<command>", required=True)

    # add -v/--verbose to each subparser as well.
    init_parser = add_init_args(subparsers)
    _add_v_arg(init_parser)

    run_parser = add_run_args(subparsers)
    _add_v_arg(run_parser)

    login_parser = add_login_args(subparsers)
    _add_v_arg(login_parser)

    sync_parser = add_sync_args(subparsers)
    _add_v_arg(sync_parser)

    submit_parser = add_submit_args(subparsers)
    _add_v_arg(submit_parser)

    status_parser = add_status_args(subparsers)
    _add_v_arg(status_parser)

    args = parser.parse_args(argv)
    args_dict = vars(args)

    verbose: int = args_dict.pop("verbose")
    setup_logging(verbose=verbose, force=True)
    subcommand = args_dict.pop("<command>")
    function: Callable = args_dict.pop("func")

    if subcommand == "submit":
        args_dict["program_args"] = submit_program_args

    try:
        if inspect.iscoroutinefunction(function):
            asyncio.run(function(**args_dict))
        else:
            function(**args_dict)
    except subprocess.CalledProcessError as err:
        logger.error(f"Command '{err.cmd}' failed with exit code {err.returncode}:")
        if err.output:
            logger.error(f"Standard output:\n{err.output}")
        else:
            logger.error("No standard output.")
        if err.stderr:
            logger.error(f"Standard error:\n{err.stderr}")
        else:
            logger.error("No standard error.")
        sys.exit(err.returncode)

def add_submit_args(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit a SLURM job on a remote cluster.",
        formatter_class=rich_argparse.RichHelpFormatter,
        usage="cluv submit <cluster> <job_script> [sbatch_args...] -- [program_args...]",
    )
    submit_parser.add_argument(
        "cluster",
        metavar="<cluster>",
        default=None,
        help=(
            "The cluster to submit the job on. "
            "Set at 'first' to submit the job on all clusters, and wait until one of them starts. "
            "Once one starts, cancel the others."
        ),
    )
    submit_parser.add_argument(
        "job_script",
        metavar="<job.sh>",
        help="Path to the sbatch job script (relative to project root).",
    )
    submit_parser.add_argument(
        "sbatch_args",
        nargs=argparse.REMAINDER,
        metavar="...",
        help="sbatch flags (before --) and/or program arguments (after --).",
    )
    submit_parser.set_defaults(func=submit)
    return submit_parser


def add_status_args(subparsers: Subparsers) -> argparse.ArgumentParser:
    status_parser = subparsers.add_parser(
        "status",
        help="Get the status of available clusters.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    status_parser.add_argument(
        "clusters",
        nargs="*",
        default=None,
        metavar="<cluster>",
        help=("Cluster(s) to query. Leave empty to query all clusters with an active connection."),
    )
    # TODO: Add sub-commands to query the status with respect to different things, GPUs, storage, jobs, etc?
    # Or just display everything?
    status_parser.set_defaults(func=status)
    return status_parser


def add_sync_args(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    sync_parser = subparsers.add_parser(
        "sync",
        help="Synchronizes the current project across clusters.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    sync_parser.add_argument(
        "clusters",
        nargs="*",
        default=None,
        metavar="<cluster>",
        help=(
            "The cluster(s) to synchronize with. "
            "Leave empty to synchronize with all currently logged in clusters. "
            "Use a comma to separate multiple clusters."
        ),
    )
    # TODO: Try to add a 'remainder' arg to pass extra args to `uv sync` on the remote cluster, but it seems to be a bit tricky.
    # sync_parser.add_argument(
    #     "--",
    #     dest="_",
    #     # type=str,
    #     # help="The arguments to pass to `uv sync` on the remote cluster.",
    #     # dest=argparse.SUPPRESS,
    # )
    # sync_parser.add_argument(
    #     "--",
    #     dest="uv_sync_args",
    #     # type=str,
    #     # metavar="<uv sync arguments>",
    #     help="The arguments to pass to `uv sync` on the remote cluster.",
    #     nargs=argparse.REMAINDER,
    # )
    sync_parser.set_defaults(func=sync)
    return sync_parser


def add_login_args(subparsers: Subparsers) -> argparse.ArgumentParser:
    login_parser = subparsers.add_parser(
        "login",
        help="Login to the specified clusters.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    login_parser.add_argument(
        "clusters",
        nargs="*",
        help="The cluster(s) to login to. Leave empty to login to all clusters.",
    )
    login_parser.set_defaults(func=login)
    return login_parser


def add_init_args(subparsers: Subparsers) -> argparse.ArgumentParser:
    init_parser = subparsers.add_parser(
        "init",
        help="Initialize the current project across clusters.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    init_parser.set_defaults(func=init)
    return init_parser


def add_run_args(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> argparse.ArgumentParser:
    run_parser = subparsers.add_parser(
        "run",
        help="Run a command on a cluster",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    run_parser.add_argument(
        "cluster",
        # default=,
        metavar="<cluster>",
        help="The cluster to run the command on",
    )
    run_parser.add_argument(
        "command",
        type=str,
        metavar="<command>",
        help="The command to run",
        nargs=argparse.REMAINDER,
    )
    run_parser.set_defaults(func=run)
    return run_parser


def setup_logging(verbose: int | None, force: bool = False) -> None:
    verbose = verbose or 0
    handler = rich.logging.RichHandler(
        console=console,
        show_time=console is not None,
        rich_tracebacks=True,
        markup=True,
    )
    # logging.basicConfig(
    #     level=logging.WARNING,
    #     format="%(message)s",
    #     handlers=[handler],
    #     force=force,
    # )
    cluv_logger = logging.getLogger("cluv")
    cluv_logger.addHandler(handler)

    # if verbose == 0:
    #     # logger.setLevel(logging.ERROR)
    #     logger.setLevel(logging.WARNING)
    #     cluv_logger.setLevel(logging.WARNING)
    # elif verbose == 1:
    #     logger.setLevel(logging.INFO)
    #     cluv_logger.setLevel(logging.INFO)
    # elif verbose >= 2:
    #     logger.setLevel(logging.DEBUG)
    #     cluv_logger.setLevel(logging.DEBUG)


def _add_v_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="count",
        help="Increase logging verbosity",
    )


if __name__ == "__main__":
    main()
