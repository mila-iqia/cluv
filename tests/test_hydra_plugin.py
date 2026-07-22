import subprocess
import textwrap
from pathlib import Path
from typing import Literal

import pytest


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
