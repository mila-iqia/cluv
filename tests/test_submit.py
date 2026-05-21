import textwrap
import subprocess
from pathlib import Path

from cluv.cli.submit import (
    TERMINAL_JOB_STATES,
    ensure_clean_git_state,
    get_config,
    get_sbatch_command,
)

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


class TestTerminalJobStates:
    """The wait-loop in submit_first uses `state not in TERMINAL_JOB_STATES`.

    Sanity-check the predicate: known terminal states stop the loop, transient
    states (including ones absent from old `RUNNING_JOB_STATES`) keep it going,
    and an empty/unknown state defaults to keep-polling.
    """

    @pytest.mark.parametrize(
        "state",
        ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL"],
    )
    def test_terminal_states_stop_polling(self, state: str) -> None:
        assert state in TERMINAL_JOB_STATES

    @pytest.mark.parametrize(
        "state",
        ["PENDING", "RUNNING", "COMPLETING", "CONFIGURING", "SUSPENDED", "REQUEUED", "RESIZING"],
    )
    def test_transient_states_keep_polling(self, state: str) -> None:
        assert state not in TERMINAL_JOB_STATES

    @pytest.mark.parametrize("state", ["", "FUTURE_SLURM_STATE"])
    def test_empty_or_unknown_state_keeps_polling(self, state: str) -> None:
        # The submit_first call site is `if job_status and job_status not in TERMINAL_JOB_STATES`,
        # so an empty string short-circuits to "still running" via the truthiness guard.
        assert state not in TERMINAL_JOB_STATES


class TestEnsureCleanGitState:
    def test_prefers_branch_tip_in_github_actions_detached_head(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("GITHUB_HEAD_REF", "proper_integration_tests")

        def mock_subprocess_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            assert kwargs.get("capture_output") is True
            assert kwargs.get("text") is True
            if command == ["git", "status", "--porcelain"]:
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            if command == ["git", "rev-parse", "--verify", "origin/proper_integration_tests"]:
                return subprocess.CompletedProcess(
                    command, 0, stdout="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n", stderr=""
                )
            raise AssertionError(f"Unexpected subprocess.run call: {command}")

        def mock_subprocess_check_output(command: list[str], **kwargs) -> str:
            assert kwargs.get("text") is True
            if command == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
                return "HEAD\n"
            if command == ["git", "rev-parse", "HEAD"]:
                return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
            raise AssertionError(f"Unexpected subprocess.check_output call: {command}")

        monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
        monkeypatch.setattr(subprocess, "check_output", mock_subprocess_check_output)

        assert ensure_clean_git_state() == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    def test_falls_back_to_head_if_remote_branch_ref_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("GITHUB_HEAD_REF", "missing_branch")

        def mock_subprocess_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            assert kwargs.get("capture_output") is True
            assert kwargs.get("text") is True
            if command == ["git", "status", "--porcelain"]:
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            if command == ["git", "rev-parse", "--verify", "origin/missing_branch"]:
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="unknown revision")
            raise AssertionError(f"Unexpected subprocess.run call: {command}")

        def mock_subprocess_check_output(command: list[str], **kwargs) -> str:
            assert kwargs.get("text") is True
            if command == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
                return "HEAD\n"
            if command == ["git", "rev-parse", "HEAD"]:
                return "cccccccccccccccccccccccccccccccccccccccc\n"
            raise AssertionError(f"Unexpected subprocess.check_output call: {command}")

        monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
        monkeypatch.setattr(subprocess, "check_output", mock_subprocess_check_output)

        assert ensure_clean_git_state() == "cccccccccccccccccccccccccccccccccccccccc"
