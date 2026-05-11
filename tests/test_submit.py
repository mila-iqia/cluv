import textwrap
import subprocess
from pathlib import Path

from cluv.cli.submit import ensure_clean_git_state, get_sbatch_command, get_config

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
            [tool.cluv.env]
            MY_VAR="1"
            [tool.cluv.clusters.mila.env]
            SPECIAL_MILA_VAR="xyz"
            [tool.cluv.clusters.vulcan.env]
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
            [tool.cluv.env]
            MY_VAR="1"
            [tool.cluv.clusters.mila.env]
            MY_VAR="2"
            [tool.cluv.clusters.vulcan.env]
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


class TestEnsureCleanGitState:
    def test_prefers_branch_tip_in_github_actions_detached_head(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("GITHUB_HEAD_REF", "proper_integration_tests")

        def fake_run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            assert kwargs.get("capture_output") is True
            assert kwargs.get("text") is True
            if cmd == ["git", "status", "--porcelain"]:
                return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
            if cmd == ["git", "rev-parse", "--verify", "origin/proper_integration_tests"]:
                return subprocess.CompletedProcess(cmd, 0, stdout="remotebranchsha\n", stderr="")
            raise AssertionError(f"Unexpected subprocess.run call: {cmd}")

        def fake_check_output(cmd: list[str], **kwargs) -> str:
            assert kwargs.get("text") is True
            if cmd == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
                return "HEAD\n"
            if cmd == ["git", "rev-parse", "HEAD"]:
                return "detachedheadsha\n"
            raise AssertionError(f"Unexpected subprocess.check_output call: {cmd}")

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(subprocess, "check_output", fake_check_output)

        assert ensure_clean_git_state() == "remotebranchsha"
