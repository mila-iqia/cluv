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
async def test_fetch_results_returns_new_runs(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """fetch_results should return newly-synced run directories."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "run_001").mkdir()

    mock_rsync.append(logs_dir / "run_002")

    remote = Remote(hostname="mila")
    new_runs = await fetch_results(remote, Path("logs"))

    assert [p.name for p in new_runs] == ["run_002"]


@pytest.mark.asyncio
async def test_fetch_results_returns_empty_when_no_new_runs(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """fetch_results should return an empty list when rsync brings no new runs."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "run_001").mkdir()

    # mock_rsync injects nothing — rsync finds no new directories.
    remote = Remote(hostname="mila")
    new_runs = await fetch_results(remote, Path("logs"))

    assert new_runs == []


@pytest.mark.asyncio
async def test_fetch_results_returns_new_runs_when_results_dir_was_empty(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """fetch_results should return new runs even when the results directory was empty before sync."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()

    mock_rsync.append(logs_dir / "run_001")

    remote = Remote(hostname="mila")
    new_runs = await fetch_results(remote, Path("logs"))

    assert [p.name for p in new_runs] == ["run_001"]


@pytest.mark.asyncio
async def test_fetch_results_returns_new_runs_when_results_dir_did_not_exist(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """fetch_results should return new runs even when the results directory did not exist before sync."""
    logs_dir = project_dir / "logs"
    # logs_dir does NOT exist yet; mock_rsync will create it together with the run dir.

    mock_rsync.append(logs_dir / "run_001")

    remote = Remote(hostname="mila")
    new_runs = await fetch_results(remote, Path("logs"))

    assert [p.name for p in new_runs] == ["run_001"]


@pytest.mark.asyncio
async def test_fetch_results_does_not_include_files(
    project_dir: Path,
    mock_rsync: list[Path],
    mock_create_symlink,
):
    """fetch_results should only include directories, not plain files."""
    logs_dir = project_dir / "logs"
    logs_dir.mkdir()

    # Simulate rsync creating a new run dir AND a stray file.
    new_run = logs_dir / "run_001"
    stray_file = logs_dir / "metadata.json"
    mock_rsync.extend([new_run, stray_file])

    async def _mock_run_with_file(program_and_args, **kwargs):
        new_run.mkdir(parents=True, exist_ok=True)
        stray_file.touch()
        return subprocess.CompletedProcess(
            args=program_and_args, returncode=0, stdout="", stderr=""
        )

    # Override mock_rsync's own _mock_run to create a file as well.
    _sync_module.run = _mock_run_with_file  # type: ignore[attr-defined]

    remote = Remote(hostname="mila")
    new_runs = await fetch_results(remote, Path("logs"))

    assert [p.name for p in new_runs] == ["run_001"]
    assert not any(p.name == "metadata.json" for p in new_runs)


def test_sync_displays_paths_relative_to_cwd(
    project_dir: Path,
):
    """Paths inside cwd should be displayed as relative paths."""
    cwd = project_dir
    run_path = project_dir / "logs" / "run_001"
    try:
        display_path = run_path.relative_to(cwd)
    except ValueError:
        display_path = run_path
    assert str(display_path) == "logs/run_001"


def test_sync_displays_absolute_path_when_outside_cwd(tmp_path: Path):
    """Paths outside cwd should be displayed as absolute paths."""
    cwd = tmp_path / "project"
    run_path = tmp_path / "other" / "run_001"
    try:
        display_path = run_path.relative_to(cwd)
    except ValueError:
        display_path = run_path
    assert display_path == run_path

