import argparse

import rich_argparse

from ..config import get_cluster_choices


def add_run_args(subparsers: argparse._SubParsersAction):
    cluster_choices = get_cluster_choices()
    run_default_cluster = "all" if "all" in cluster_choices else cluster_choices[0]
    run_parser = subparsers.add_parser(
        "run",
        help="Run a command on a cluster",
        formatter_class=rich_argparse.RichHelpFormatter,
    )
    run_parser.add_argument(
        "cluster",
        choices=cluster_choices,
        default=run_default_cluster,
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


def run(command: str, cluster: str = "all"):
    """Runs a command in the synced project on a potentially remote cluster.

    Similar in spirit to `uv run`, but runs a command in the synced project on a potentially remote cluster.
    - Idea is that this could maybe be a building block for other commands.
    """
    print(f"About to run {command=} on {cluster=}")
    raise NotImplementedError("TODO: " + (run.__doc__ or ""))
