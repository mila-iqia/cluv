"""CLUV: Tool to use UV with multiple clusters."""

# todo: typer doesn't quite work for commands that need the argparse.REMAINDER feature like `run` and `launch`.
# It requires you to pass the command in quotes, but I'd like to be able
# to do `cluv run ls -l` for example. Here it errors out saying "no -l option".

import argparse
from typing import Callable

import rich_argparse
import simple_parsing

from .cli.init import init
from .cli.run import run
from .cli.status import status
from .cli.sync import sync
from .config import get_cluster_choices, get_default_cluster


def main(argv: list[str] | None = None):
    parser = simple_parsing.ArgumentParser(
        description=__doc__,
        formatter_class=rich_argparse.RichHelpFormatter,
        epilog="For more information, see the documentation. You rock.",
    )
    subparsers = parser.add_subparsers(dest="<command>", required=True)

    init_parser = subparsers.add_parser(
        "init",
        help="Initialize the current project across clusters.",
        formatter_class=parser.formatter_class,
    )
    init_parser.set_defaults(func=init)

    cluster_choices = get_cluster_choices()
    run_default_cluster = get_default_cluster(cluster_choices)

    run_parser = subparsers.add_parser(
        "run",
        help="Run a command on a cluster.",
        formatter_class=parser.formatter_class,
    )
    run_parser.add_argument(
        "cluster",
        choices=cluster_choices,
        default=run_default_cluster,
        metavar="<cluster>",
        help="The cluster to run the command on.",
    )
    run_parser.add_argument(
        "command",
        type=str,
        metavar="<command>",
        help="The command to run",
        nargs=argparse.REMAINDER,
    )
    run_parser.set_defaults(func=run)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Synchronize the current project across clusters.",
        formatter_class=parser.formatter_class,
    )
    sync_parser.add_argument(
        "cluster",
        choices=cluster_choices,
        default=(),
        nargs="*",
        metavar="<cluster>",
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
    args_dict.pop("<command>")
    function: Callable = args_dict.pop("func")
    return function(**args_dict)


if __name__ == "__main__":
    main()
