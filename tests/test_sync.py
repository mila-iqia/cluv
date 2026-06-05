"""Unit tests for cluv/cli/sync.py — pure, no real SSH connections."""

from __future__ import annotations

import importlib
import subprocess
from pathlib import Path

import pytest

# Import the sync *module* directly via importlib to avoid the name collision with
# the `sync` function that cluv/cli/__init__.py re-exports under the same name.
_sync_module = importlib.import_module("cluv.cli.sync")

from cluv.cli.sync import fetch_results  # noqa: E402
from cluv.remote import Remote  # noqa: E402
from cluv.utils import console  # noqa: E402


@pytest.fixture()
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return tmp_path


@pytest.fixture()
def project_dir(fake_home: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    project = fake_home / "myproject"
    project.mkdir()
    (project / "pyproject.toml").write_text(
        '[tool.cluv]\nresults_path = "logs"\n[tool.cluv.clusters.mila]\n'
    )
    monkeypatch.chdir(project)
    return project


@pytest.fixture()
def mock_rsync(monkeypatch: pytest.MonkeyPatch):
    """Mock the module-level run() used for rsync; returns a list to inject new paths."""
    injected_paths: list[Path] = []

    async def _mock_run(program_and_args, **kwargs):
        for path in injected_paths:
            path.mkdir(parents=True, exist_ok=True)
        return subprocess.CompletedProcess(args=program_and_args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(_sync_module, "run", _mock_run)
    return injected_paths


@pytest.fixture()
def mock_create_symlink(monkeypatch: pytest.MonkeyPatch):
    async def _noop(*args, **kwargs):
        pass

    monkeypatch.setattr(_sync_module, "create_results_dir_with_symlink_to_scratch", _noop)


@pytest.mark.asyncio
async def test_fetch_results_displays_new_runs(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """Newly-synced run directories should be printed after fetch_results."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "run_001").mkdir()

    mock_rsync.append(logs_dir / "run_002")

    remote = Remote(hostname="mila")
    console.export_text()  # drain buffer from previous tests
    await fetch_results(remote, Path("logs"))

    exported = console.export_text()
    assert "run_002" in exported
    assert "run_001" not in exported


@pytest.mark.asyncio
async def test_fetch_results_no_output_when_no_new_runs(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """No output should be shown when rsync brings no new runs."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "run_001").mkdir()

    # mock_rsync injects nothing — rsync finds no new directories.
    remote = Remote(hostname="mila")
    console.export_text()  # drain buffer
    await fetch_results(remote, Path("logs"))

    exported = console.export_text()
    assert "Newly synced" not in exported


@pytest.mark.asyncio
async def test_fetch_results_displays_new_runs_when_results_dir_was_empty(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """New runs should be shown even when the results directory was empty before sync."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()

    mock_rsync.append(logs_dir / "run_001")

    remote = Remote(hostname="mila")
    console.export_text()  # drain buffer
    await fetch_results(remote, Path("logs"))

    exported = console.export_text()
    assert "run_001" in exported


@pytest.mark.asyncio
async def test_fetch_results_displays_new_runs_when_results_dir_did_not_exist(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """New runs should be shown even when the results directory did not exist before sync."""
    logs_dir = project_dir / "logs"
    # logs_dir does NOT exist yet; mock_rsync will create it together with the run dir.

    mock_rsync.append(logs_dir / "run_001")

    remote = Remote(hostname="mila")
    console.export_text()  # drain buffer
    await fetch_results(remote, Path("logs"))

    exported = console.export_text()
    assert "run_001" in exported


@pytest.mark.asyncio
async def test_fetch_results_shows_hostname_in_output(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """The remote hostname should appear in the newly-synced runs message."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()

    mock_rsync.append(logs_dir / "run_001")

    remote = Remote(hostname="rorqual")
    console.export_text()  # drain buffer
    await fetch_results(remote, Path("logs"))

    exported = console.export_text()
    assert "rorqual" in exported
