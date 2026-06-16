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
from pathlib import Path
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

logger = logging.getLogger("cluv")
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
    _add_v_arg(parser, _root=True)  # add -v/--verbose on the top-level parser.

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

    # These flags can be passed either to the root logger or the subcommand loggers.
    verbose: int = max(args_dict.pop("verbose", 0), args_dict.pop("_verbose", 0))
    quiet: bool = max(args_dict.pop("quiet", False), args_dict.pop("_quiet", False))
    setup_logging(verbose=verbose, quiet=quiet)
    subcommand = args_dict.pop("<command>")
    function: Callable = args_dict.pop("func")

    if subcommand == "submit":
        # job script is an optional positional argument. When not passed, an sbatch argument like
        # --gpus will be parsed as the job script. We rectify that here.
        job_script: Path | None = args_dict["job_script"]
        if job_script and str(job_script).startswith("-") and not job_script.exists():
            args_dict["sbatch_args"] = [str(job_script), *args_dict["sbatch_args"]]
            job_script = None
            args_dict["job_script"] = None
        args_dict["program_args"] = submit_program_args

    if subcommand == "status" and quiet:
        console.print("[yellow]Warning: --quiet has no effect with the 'status' command.[/yellow]")
        quiet = False
    console.quiet = quiet

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


def add_submit_args(subparsers: Subparsers):
    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit a SLURM job on a remote cluster.",
        formatter_class=rich_argparse.RichHelpFormatter,
        usage="cluv submit <cluster> [<job.sh>] [sbatch-args...] [-- program-args...]",
    )
    submit_parser.add_argument(
        "--autocommit",
        action="store_true",
        help="Create a local commit with tracked changes before submitting the job.",
    )
    submit_parser.add_argument(
        "cluster",
        metavar="<cluster>",
        help=(
            "The cluster to submit the job on. "
            "Set at 'first' to submit a job on all clusters, and wait until one of them starts. "
            "Once one starts, cancel the others."
        ),
    )
    submit_parser.add_argument(
        "job_script",
        metavar="<job.sh>",
        nargs="?",
        default=None,
        type=Path,
        help="Path to the sbatch job script (relative to project root). Defaults to the job script specified in the config at 'job_script_path'.",
    )
    submit_parser.add_argument(
        "sbatch_args",
        nargs=argparse.REMAINDER,
        metavar="...",
        help="sbatch flags (before --) and/or program arguments (after --).",
    )
    submit_parser.set_defaults(func=submit)
    return submit_parser


def add_status_args(subparsers: Subparsers):
    status_parser = subparsers.add_parser(
        "status",
        help="Get the status of clusters and jobs.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    status_parser.add_argument(
        "table",
        nargs="?",
        choices=["clusters", "jobs", "all"],
        default="all",
        metavar="<table>",
        help="Which table to display: cluster overview, jobs overview, or both (default: all).",
    )
    status_parser.set_defaults(func=status)
    return status_parser


def add_sync_args(subparsers: Subparsers):
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
    sync_parser.add_argument(
        "--sync-datasets",
        dest="sync_datasets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Push datasets from data_source to each cluster. Requires data_source in config.",
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


def add_login_args(subparsers: Subparsers):
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


def add_init_args(subparsers: Subparsers):
    init_parser = subparsers.add_parser(
        "init",
        help="Initialize the current project across clusters.",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    init_parser.add_argument(
        "path",
        nargs="?",
        default=None,
        metavar="<path>",
        type=Path,
        help="Path to initialize the project in. Creates the directory if it doesn't exist. Defaults to the current directory.",
    )
    init_parser.set_defaults(func=init)
    return init_parser


def add_run_args(subparsers: Subparsers):
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


def setup_logging(verbose: int | None, quiet: bool = False) -> None:
    verbose = verbose or 0
    handler = rich.logging.RichHandler(
        console=console,
        show_time=console is not None,
        rich_tracebacks=True,
        markup=True,
    )
    logger.addHandler(handler)
    if quiet:
        logger.setLevel(logging.CRITICAL)
    elif verbose == 0:
        logger.setLevel(logging.WARNING)
    elif verbose == 1:
        logger.setLevel(logging.INFO)
    elif verbose >= 2:
        logger.setLevel(logging.DEBUG)


def _add_v_arg(parser: argparse.ArgumentParser, _root: bool = False) -> None:
    parser.add_argument(
        "-v",
        "--verbose",
        dest="_verbose" if _root else "verbose",
        action="count",
        default=0,
        help="Increase logging verbosity",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        dest="_quiet" if _root else "quiet",
        action="store_true",
        help="Disable command output.",
    )


if __name__ == "__main__":
    main()
