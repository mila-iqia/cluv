"""Regression test: don't sync with more than one cluster that shares a filesystem.

Some cluster hostnames (e.g. `trillium` and `trillium-gpu`) are actually distinct login nodes
of the same cluster that share a single $HOME (confirmed live via SSH: they mount the identical
NFS export). Syncing with both at once is pointless -- it's the same on-disk checkout -- and
unsafe, since concurrent `git`/`uv sync` commands from two hosts race on that shared checkout
(this is what made `checkout -B <branch> FETCH_HEAD` fail intermittently in the `cluster=first`
hydra launcher integration test). Rather than serializing access to whatever path two clusters
happen to resolve to (which could cause unrelated clusters with a coincidentally identical path
to be wrongly treated as sharing a filesystem), `cluv` keeps an explicit list of known
filesystem-sharing groups and only ever syncs with one cluster per group.
"""

from __future__ import annotations

import importlib

import pytest

from cluv.cache import get_disabled_clusters
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.config import CluvConfig, PartialClusterConfig
from cluv.remote import Remote

sync_module = importlib.import_module("cluv.cli.sync")


@pytest.mark.parametrize(
    ("clusters", "expected"),
    [
        (["mila", "trillium", "narval", "trillium-gpu"], ["mila", "trillium", "narval"]),
        (["trillium-gpu", "trillium", "mila"], ["trillium-gpu", "mila"]),
        (["mila", "narval", "tamia"], ["mila", "narval", "tamia"]),
        (["trillium"], ["trillium"]),
    ],
)
def test_dedupe_keeps_only_the_first_cluster_per_shared_filesystem_group(clusters, expected):
    assert sync_module._dedupe_clusters_sharing_a_filesystem(clusters) == expected


async def test_get_active_remotes_only_returns_one_of_trillium_and_trillium_gpu(
    monkeypatch: pytest.MonkeyPatch,
):
    config = CluvConfig(
        results_path="/results",
        clusters={
            "trillium": PartialClusterConfig(),
            "trillium-gpu": PartialClusterConfig(),
            "narval": PartialClusterConfig(),
        },
    )
    monkeypatch.setattr(sync_module, sync_module.get_cluv_config.__name__, lambda: config)
    monkeypatch.setattr(sync_module, sync_module.current_cluster.__name__, lambda: None)
    monkeypatch.setattr(sync_module, get_disabled_clusters.__name__, lambda: set())

    contacted: list[str] = []

    async def fake_get_remote_without_2fa_prompt(hostname: str) -> Remote | None:
        contacted.append(hostname)
        return Remote(hostname=hostname)

    monkeypatch.setattr(
        sync_module,
        get_remote_without_2fa_prompt.__name__,
        fake_get_remote_without_2fa_prompt,
    )

    remotes = await sync_module.get_active_remotes()

    # Only one of the two was even contacted, and only one made it into the result.
    assert not ({"trillium", "trillium-gpu"} <= set(contacted))
    hostnames = {r.hostname for r in remotes}
    assert not ({"trillium", "trillium-gpu"} <= hostnames)
    assert "narval" in hostnames
