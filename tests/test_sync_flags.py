"""Unit tests for the flags of `cluv sync` that don't need a live cluster."""

import importlib
import textwrap
import unittest
import unittest.mock
from pathlib import Path, PurePosixPath

import pytest

from cluv.cli.sync import fetch_results
from cluv.config import get_cluv_config, load_cluv_config

# `cluv/cli/__init__.py` does `from .sync import sync`, which shadows the `cluv.cli.sync`
# submodule attribute with the `sync` function.
sync_module = importlib.import_module("cluv.cli.sync")


async def test_no_sync_datasets_also_skips_the_push(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`--no-sync-datasets` has to skip pushing to the cluster, not just the pull.

    Regression test: `sync_datasets` was only consulted for the pull step, so
    `cluv sync --no-sync-datasets` (and `cluv submit --no-sync-datasets`) still rsynced the entire
    dataset to every cluster.
    """
    for name in ("install_uv", "clone_project", "run_uv_sync"):
        monkeypatch.setattr(sync_module, name, unittest.mock.AsyncMock())
    monkeypatch.setattr(
        sync_module, fetch_results.__name__, unittest.mock.AsyncMock(return_value=[])
    )
    push_mock = unittest.mock.AsyncMock()
    monkeypatch.setattr(sync_module, "_push_datasets_to_remote", push_mock)
    monkeypatch.setattr(
        sync_module, "expandvars", unittest.mock.AsyncMock(return_value=PurePosixPath("/project"))
    )

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(
            """\
            [tool.cluv]
            results_path = "logs"
            project_dir = "/project"
            data_source = "mila:/network/datasets/imagenet"
            datasets_path = "/datasets/imagenet"
            [tool.cluv.clusters.tamia]
            """
        )
    )
    config = load_cluv_config(pyproject)
    monkeypatch.setattr(sync_module, get_cluv_config.__name__, lambda: config)
    remote = unittest.mock.MagicMock(hostname="tamia")

    await sync_module.sync_per_cluster_part(remote, sync_datasets=False)
    push_mock.assert_not_awaited()

    await sync_module.sync_per_cluster_part(remote, sync_datasets=True)
    push_mock.assert_awaited_once()
