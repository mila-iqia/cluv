import subprocess
import textwrap
import unittest.mock
from pathlib import Path
from typing import Literal

import pytest

from cluv.cache import Job
from cluv.config import CluvConfig, PartialClusterConfig


@pytest.mark.parametrize(
    "python_version",
    [
        pytest.param("3.11", marks=pytest.mark.xfail(reason="TODO: cluv needs 3.13 atm.")),
        pytest.param("3.12", marks=pytest.mark.xfail(reason="TODO: cluv needs 3.13 atm.")),
        "3.13",
        pytest.param(
            "3.14",
            marks=pytest.mark.xfail(
                reason="Hydra seems to not work in python 3.14, getting 'TypeError: argument of type 'LazyCompletionHelp' is not a container or iterable' and an argparse error."
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "install_variant",
    [
        pytest.param(
            "pypi",
            # TODO: Remove this mark after the fix PR #145 is merged and a new PyPI release is made.
            marks=pytest.mark.xfail(
                reason="The 'hydra' extra isn't available on the PyPI release yet."
            ),
        ),
        pytest.param(
            "github",
            # TODO: Remove this mark after the fix PR #145 is merged.
            marks=pytest.mark.xfail(
                reason="The GitHub master branch doesn't include the fix yet."
            ),
        ),
        "source",
    ],
)
def test_hydra_launcher_is_discoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    install_variant: Literal["pypi", "github", "source"],
    python_version: str,
) -> None:
    """Create a new python project that uses Hydra in a tmp directory.
    Install cluv either from pypi, from the github url, or from source.
    Then, check that the Hydra launcher is loaded correctly in that project with
    `python main.py --info plugins`.
    """
    # Create a new python project in the tmp directory
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    repo_root = Path.cwd()
    monkeypatch.chdir(project_dir)
    # Isolate from the environment cluv is being tested in: `uv run pytest` exports
    # VIRTUAL_ENV pointing at the repo venv. `uv run`/`uv add` warn and ignore a
    # mismatched VIRTUAL_ENV (they use the project's own .venv), but `uv pip list`
    # silently honors it, so it would list the repo venv instead of this project's.
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)

    pyproject_file = project_dir / "pyproject.toml"
    (project_dir / "main.py").write_text(
        textwrap.dedent(
            """\
            import hydra
            from omegaconf import DictConfig

            @hydra.main(version_base=None, config_path="configs", config_name=None)
            def main(cfg: DictConfig) -> None:
                print("Hello, Hydra!")
                print(cfg)

            if __name__ == "__main__":
                main()
            """
        )
    )
    launcher_config = project_dir / "configs" / "hydra" / "launcher" / "cluv_mila.yaml"
    launcher_config.parent.mkdir(parents=True)
    launcher_config.write_text(
        textwrap.dedent(
            """\
            defaults:
              - cluv_launcher
            cluster: mila
            """
        )
    )
    subprocess.check_call(f"uv init --python={python_version}", shell=True)
    assert "cluster-uv" not in pyproject_file.read_text()

    if install_variant == "pypi":
        subprocess.check_call("uv add cluster-uv[hydra]", shell=True, text=True)
        assert "cluster-uv[hydra]" in pyproject_file.read_text()

    elif install_variant == "github":
        subprocess.check_call(
            "uv add git+https://github.com/mila-iqia/cluv[hydra]", shell=True, text=True
        )
        assert "cluster-uv[hydra]" in pyproject_file.read_text()
    else:
        subprocess.check_call(f"uv add {repo_root}[hydra]", shell=True)
        assert "cluster-uv[hydra]" in pyproject_file.read_text()

    assert "hydra-core" in subprocess.check_output("uv pip list", shell=True, text=True)

    output = subprocess.check_output("uv run python main.py --info plugins", shell=True, text=True)
    assert "hydra_plugins.hydra_submitit_launcher.submitit_launcher" in output
    assert "hydra_plugins.cluv.cluv_launcher" in output

    error = subprocess.run(
        "uv run python main.py -m hydra/launcher=cluv_mila",
        shell=True,
        text=True,
        capture_output=True,
    ).stderr
    assert f"RuntimeError: No cluv config in {pyproject_file} file." in error

    error = subprocess.run("cluv init", capture_output=True, text=True, shell=True).stderr
    assert (
        "RuntimeError: cluv init should be run in a directory under your home directory." in error
    )


@pytest.fixture
def fake_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    import cluv.cache

    monkeypatch.setattr(cluv.cache, cluv.cache._get_cache_dir.__name__, lambda: cache_dir)
    return cache_dir


async def test_run_sweep_races_every_job_independently_on_first(
    tmp_path: Path, fake_cache_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression test for a bug found while making `run_sweep` submit concurrently: it used to
    feed the cluster that won job 1's "first" race back into the `cluster` argument for jobs 2,
    3, ..., silently skipping their own multi-cluster race. Every job in a sweep must always
    race independently on the originally requested cluster.
    """
    # `hydra_plugins.cluv.cluv_launcher` needs the optional "hydra" extra (hydra-core etc.),
    # which isn't part of the default dev environment — skip gracefully when it's absent, same
    # as the rest of this file relies on an isolated subprocess venv for it.
    cluv_launcher = pytest.importorskip("hydra_plugins.cluv.cluv_launcher")

    submitted_clusters: list[str] = []
    resolved_clusters = iter(["cluster_a", "cluster_b", "cluster_c"])

    async def fake_submit(
        cluster: str,
        job_script,
        sbatch_args,
        program_args,
        _skip_sync: bool = False,
    ) -> Job:
        submitted_clusters.append(cluster)
        return Job(
            job_id=len(submitted_clusters),
            cluster=next(resolved_clusters),
            job_script=str(job_script),
            git_commit="dummy",
            submitted_at="2026-01-01T00:00:00",
            sbatch_args=sbatch_args,
            program_args=program_args,
        )

    monkeypatch.setattr(cluv_launcher, "submit", unittest.mock.AsyncMock(wraps=fake_submit))
    monkeypatch.setattr(cluv_launcher, "monitor_jobs_async", unittest.mock.AsyncMock())
    monkeypatch.setattr(cluv_launcher, "fetch_results", unittest.mock.AsyncMock(return_value=[]))
    monkeypatch.setattr(cluv_launcher, "get_results_path", lambda: tmp_path / "results")

    config = CluvConfig(
        results_path=str(tmp_path / "results"),
        clusters={"cluster_a": PartialClusterConfig(project_dir="/home/user/proj")},
    )

    job_infos = await cluv_launcher.run_sweep(
        job_commands=[
            ["python", "main.py", "lr=0.1"],
            ["python", "main.py", "lr=0.2"],
            ["python", "main.py", "lr=0.3"],
        ],
        cluster="first",
        cluv_config=config,
        cluster_remotes={},
        job_script=None,
        params={},
        chunking=False,
        packing=False,
    )

    # Every job must have been submitted against the *original* "first" target, never against a
    # previously-resolved concrete hostname from another job in the same sweep.
    assert submitted_clusters == ["first", "first", "first"]
    # Each job's own resolved (winning) cluster must still be tracked correctly, though.
    assert {job.cluster for job in job_infos} == {"cluster_a", "cluster_b", "cluster_c"}
