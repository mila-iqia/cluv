"""`cluv clean`: remove run results from clusters once they're gone from the local results dir."""

from __future__ import annotations

from datetime import datetime

# Unused until clean()'s body is implemented (Task 5) -- kept here now as monkeypatch
# targets for tests/test_clean.py's orchestration tests.
from rich.prompt import Confirm  # noqa: F401
from cluv.cache import CacheContent, read_cache  # noqa: F401
from cluv.cli.login import login  # noqa: F401
from cluv.cli.sync import expandvars, get_active_remotes  # noqa: F401
from cluv.config import get_cluv_config  # noqa: F401
from cluv.remote import Remote, list_remote_run_dirs  # noqa: F401
from cluv.utils import console  # noqa: F401

__all__ = ["clean", "compute_runs_to_delete"]


def compute_runs_to_delete(
    local_names: set[str],
    remote_runs: list[tuple[str, datetime]],
    watermark: datetime | None,
) -> list[str]:
    """Returns the names of remote run dirs that are safe to delete.

    A remote run dir is safe to delete only if it has no local counterpart AND it's
    older than `watermark` (the max remote mtime observed during the last successful
    sync of that cluster). A genuinely new, never-fetched run always has
    `mtime >= watermark`, so it's never selected even though it also has no local
    counterpart -- only runs the user pruned locally are.

    Returns an empty list if `watermark` is `None` (the cluster has never been synced).
    """
    raise NotImplementedError


async def clean(
    clusters: list[str] | None = None,
    force: bool = False,
    dry_run: bool = False,
) -> None:
    """Removes run directories from remote clusters that were pruned from the local results dir.

    Does not run `sync` first: it only reads state cached by the last successful sync of each
    cluster. Clusters that have never been synced are skipped, since there is no watermark yet
    to distinguish a pruned run from one that's simply never been fetched. Running or pending
    Slurm jobs, and cross-cluster run-name collisions, are not specially handled (see the design
    spec's "Non-goals" section).
    """
    raise NotImplementedError
