"""CLUV: Tool to use UV with multiple clusters."""

# todo: typer doesn't quite work for commands that need the argparse.REMAINDER feature like `run` and `launch`.
# It requires you to pass the command in quotes, but I'd like to be able
# to do `cluv run ls -l` for example. Here it errors out saying "no -l option".

import argparse
import asyncio
import inspect
import logging
import subprocess
import sys
from typing import Callable

import rich
import rich.logging
import rich_argparse
import simple_parsing

from cluv.config import get_config

from .cli.init import init
from .cli.login import login
from .cli.run import add_run_args
from .cli.status import status
from .cli.sync import sync

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None):
    setup_logging(verbose=0, force=False)
    parser = simple_parsing.ArgumentParser(
        description=__doc__,
        formatter_class=rich_argparse.RichHelpFormatter,
        epilog="For more information, see the documentation. You rock.",
    )

    _add_v_arg(parser)
    subparsers = parser.add_subparsers(dest="<command>", required=True)

    init_parser = subparsers.add_parser(
        "init",
        help="Initialize the current project across clusters.",
        formatter_class=parser.formatter_class,
    )
    init_parser.set_defaults(func=init)

    config = get_config()
    add_run_args(subparsers)

    login_parser = subparsers.add_parser(
        "login",
        help="Login to the specified clusters.",
        formatter_class=parser.formatter_class,
    )
    _add_v_arg(login_parser)
    login_parser.add_argument(
        "clusters",
        choices=(config.clusters) if config.clusters else None,
        nargs="*",
        help="The cluster(s) to login to. Leave empty to login to all clusters.",
    )
    login_parser.set_defaults(func=login)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Synchronize the current project across clusters.",
        formatter_class=parser.formatter_class,
    )
    _add_v_arg(sync_parser)
    sync_parser.add_argument(
        "clusters",
        choices=(config.clusters) if config.clusters else None,
        # default="all",
        # dest="clusters",
        nargs="*",
        # metavar="<cluster(s)>",
        help="The cluster(s) to synchronize with. Leave empty to synchronize with all clusters.",
    )
    sync_parser.set_defaults(func=sync)

    status_parser = subparsers.add_parser(
        "status",
        help="Get the status of available clusters.",
        formatter_class=parser.formatter_class,
    )
    # IDEA: Add sub-commands to query the status with respect to different things, GPUs, storage, jobs, etc?
    # Or just display everything?
    status_parser.set_defaults(func=status)

    args = parser.parse_args(argv)
    args_dict = vars(args)

    verbose: int = args_dict.pop("verbose")
    setup_logging(verbose=verbose, force=True)
    args_dict.pop("<command>")
    function: Callable = args_dict.pop("func")

    try:
        if inspect.iscoroutinefunction(function):
            asyncio.run(function(**args_dict))
            return
        function(**args_dict)
        return
    except subprocess.CalledProcessError as err:
        logger.error(f"Command '{err.cmd}' failed with exit code {err.returncode}:")
        logger.error(f"Standard output:\n{err.output}")
        logger.error(f"Standard error:\n{err.stderr}")
        sys.exit(err.returncode)


def setup_logging(verbose: int | None, force: bool = False):
    verbose = verbose or 0
    if not sys.stdout.isatty():
        # Widen the log width when running in an sbatch script.
        console = rich.console.Console(width=140)
    else:
        console = None
    logging.basicConfig(
        level=logging.WARNING,
        format="%(message)s",
        handlers=[
            rich.logging.RichHandler(
                console=console,
                show_time=console is not None,
                rich_tracebacks=True,
                markup=True,
            )
        ],
        force=force,
    )
    cluv_logger = logging.getLogger("cluv")
    if verbose == 0:
        # logger.setLevel(logging.ERROR)
        logger.setLevel(logging.WARNING)
        cluv_logger.setLevel(logging.WARNING)
    elif verbose == 1:
        logger.setLevel(logging.INFO)
        cluv_logger.setLevel(logging.INFO)
    elif verbose >= 2:
        logger.setLevel(logging.DEBUG)
        cluv_logger.setLevel(logging.DEBUG)

    logging.getLogger("milatools").setLevel(
        logging.DEBUG
        if verbose == 3
        else logging.INFO
        if verbose == 2
        else logging.WARNING
    )


def _add_v_arg(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="count",
        help="Increase logging verbosity",
    )


if __name__ == "__main__":
    main()
