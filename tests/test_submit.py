import textwrap
from pathlib import Path

from cluv.cli.submit import get_sbatch_command, get_config

import pytest


@pytest.fixture
def project_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # To avoid that a test reads the cached config of an other, we need to clear the cache between each test.
    get_config.cache_clear()

    monkeypatch.setattr(Path, "home", lambda: tmp_path)  # Set the home directory to tmp_path
    project_dir = tmp_path / "my_project"
    project_dir.mkdir()
    monkeypatch.chdir(project_dir)  # Set current working dir
    return project_dir


class TestGetSbatchCommand:
    def test_generate_command_for_selected_cluster_with_correct_args_and_vars(self, project_dir: Path) -> None:
        p = project_dir / "pyproject.toml"
        p.write_text(
            textwrap.dedent(
                """\
            [tool.cluv]
            results_path = "results"
            [tool.cluv.slurm]
            MY_VAR="1"
            [tool.cluv.clusters.mila]
            SPECIAL_MILA_VAR="xyz"
            [tool.cluv.clusters.vulcan]
            SPECIAL_VULCAN_VAR="kij"
            """
            )
        )

        sbatch_command = get_sbatch_command(
            cluster="mila",
            job_script=Path("scripts/my_script.sh"),
            sbatch_args=["--account=my_account", "--mem=8G"],
            program_args=["program_arg_1", "program_arg_2"],
            git_commit="abecdef",
        )

        assert (
            sbatch_command
            == "bash --login -c 'MY_VAR=1 SPECIAL_MILA_VAR=xyz SBATCH_JOB_NAME=cluv-my_script GIT_COMMIT=abecdef sbatch --parsable --chdir=my_project --account=my_account --mem=8G ~/my_project/scripts/my_script.sh program_arg_1 program_arg_2'"
        )

    def test_only_override_slurm_vars_with_selected_cluster_vars(self, project_dir: Path) -> None:
        p = project_dir / "pyproject.toml"
        p.write_text(
            textwrap.dedent(
                """\
            [tool.cluv]
            results_path = "results"
            [tool.cluv.slurm]
            MY_VAR="1"
            [tool.cluv.clusters.mila]
            MY_VAR="2"
            [tool.cluv.clusters.vulcan]
            MY_VAR="3"
            """
            )
        )

        sbatch_command = get_sbatch_command(
            cluster="mila",
            job_script=Path("scripts/my_script.sh"),
            sbatch_args=[],
            program_args=[],
            git_commit="abecdef",
        )

        assert (
            sbatch_command
            == "bash --login -c 'MY_VAR=2 SBATCH_JOB_NAME=cluv-my_script GIT_COMMIT=abecdef sbatch --parsable --chdir=my_project  ~/my_project/scripts/my_script.sh '"
        )
