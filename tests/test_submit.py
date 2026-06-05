import subprocess
import textwrap
from io import StringIO
from pathlib import Path

from rich.console import Console

import pytest

from cluv.cli.submit import _build_commands_table, _build_submission_table, ensure_clean_git_state, get_sbatch_command
from cluv.config import get_cluv_config


@pytest.fixture(autouse=True)
def clear_get_cluv_config_cache():
    # To avoid that a test reads the cached config of an other, we need to clear the cache between each test.
    get_cluv_config.cache_clear()


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    fake_home = tmp_path / "fake_home"
    fake_home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    return fake_home


@pytest.fixture
def project_dir(fake_home: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    project_dir = fake_home / "my_project"
    project_dir.mkdir()
    monkeypatch.chdir(project_dir)  # Set current working dir
    return project_dir


class TestGetSbatchCommand:
    def test_generate_command_for_selected_cluster_with_correct_args_and_vars(
        self, project_dir: Path, fake_home: Path
    ) -> None:
        p = project_dir / "pyproject.toml"
        results_path = "results"
        p.write_text(
            textwrap.dedent(
                f"""\
            [tool.cluv]
            results_path = "{results_path}"
            [tool.cluv.env]
            MY_VAR="1"
            [tool.cluv.clusters.mila.env]
            SPECIAL_MILA_VAR="xyz"
            [tool.cluv.clusters.vulcan.env]
            SPECIAL_VULCAN_VAR="kij"
            """
            )
        )
        sbatch_script = project_dir / "my_script.sh"
        sbatch_script.touch(0o755)
        cluster = "mila"
        sbatch_command = get_sbatch_command(
            cluster=cluster,
            job_script=sbatch_script,
            sbatch_args=["--account=my_account", "--mem=8G"],
            program_args=["program_arg_1", "program_arg_2"],
            git_commit="abecdef",
        )
        job_script_relative_path = sbatch_script.relative_to(fake_home)

        assert sbatch_command == (
            "bash --login -c 'MY_VAR=1 SPECIAL_MILA_VAR=xyz SBATCH_JOB_NAME=cluv-my_script "
            # Ugly, quite hard-coded.
            f"GIT_COMMIT=abecdef SBATCH_OUTPUT={results_path}/{cluster}_%j/slurm-%j.out "
            "sbatch --parsable --chdir=my_project --account=my_account "
            f"--mem=8G ~/{job_script_relative_path} program_arg_1 program_arg_2'"
        )

    def test_only_override_slurm_vars_with_selected_cluster_vars(self, project_dir: Path) -> None:
        p = project_dir / "pyproject.toml"
        results_path = "results"
        p.write_text(
            textwrap.dedent(
                f"""\
            [tool.cluv]
            results_path = "{results_path}"
            [tool.cluv.env]
            MY_VAR="1"
            [tool.cluv.clusters.mila.env]
            MY_VAR="2"
            [tool.cluv.clusters.vulcan.env]
            MY_VAR="3"
            """
            )
        )
        job_script = project_dir / "scripts" / "my_script.sh"
        job_script.parent.mkdir()
        job_script.touch(0o755)
        sbatch_command = get_sbatch_command(
            cluster="mila",
            job_script=job_script,
            sbatch_args=[],
            program_args=[],
            git_commit="abecdef",
        )

        assert sbatch_command == (
            "bash --login -c 'MY_VAR=2 SBATCH_JOB_NAME=cluv-my_script GIT_COMMIT=abecdef "
            f"SBATCH_OUTPUT={results_path}/mila_%j/slurm-%j.out "
            "sbatch --parsable --chdir=my_project  ~/my_project/scripts/my_script.sh '"
        )


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
                return subprocess.CompletedProcess(
                    command, 1, stdout="", stderr="unknown revision"
                )
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


class TestBuildSubmissionTable:
    def _make_ok(self, job_id: int) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, stdout=f"{job_id}\n", stderr="")

    def _make_err(self, msg: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 1, stdout="", stderr=msg)

    def test_successful_submissions_populate_cluster_to_jobid(self) -> None:
        cluster_to_jobid: dict[str, int] = {}
        table = _build_submission_table(
            ["mila", "narval"],
            [self._make_ok(12345), self._make_ok(67890)],
            cluster_to_jobid,
        )
        assert cluster_to_jobid == {"mila": 12345, "narval": 67890}
        # Two data rows expected
        assert table.row_count == 2

    def test_failed_submission_not_added_to_cluster_to_jobid(self) -> None:
        cluster_to_jobid: dict[str, int] = {}
        _build_submission_table(
            ["mila", "narval"],
            [self._make_ok(42), self._make_err("out of memory")],
            cluster_to_jobid,
        )
        assert "narval" not in cluster_to_jobid
        assert cluster_to_jobid == {"mila": 42}

    def test_exception_result_not_added_to_cluster_to_jobid(self) -> None:
        cluster_to_jobid: dict[str, int] = {}
        _build_submission_table(
            ["mila"],
            [RuntimeError("connection refused")],
            cluster_to_jobid,
        )
        assert cluster_to_jobid == {}

    def test_table_cells_contain_expected_text(self) -> None:
        cluster_to_jobid: dict[str, int] = {}
        table = _build_submission_table(
            ["mila", "narval", "rorqual"],
            [
                self._make_ok(99),
                self._make_err("sbatch: error: ..."),
                RuntimeError("timeout"),
            ],
            cluster_to_jobid,
        )
        buf = StringIO()
        Console(file=buf, no_color=True, highlight=False).print(table)
        rendered = buf.getvalue()
        assert "99" in rendered
        assert "sbatch: error:" in rendered
        assert "timeout" in rendered


class TestBuildCommandsTable:
    def test_renders_all_clusters_and_commands(self) -> None:
        commands = {
            "mila": "bash --login -c 'GIT_COMMIT=abc sbatch --parsable --chdir=proj job.sh'",
            "narval": "bash --login -c 'GIT_COMMIT=abc sbatch --parsable --chdir=proj job.sh'",
        }
        table = _build_commands_table(commands)
        buf = StringIO()
        Console(file=buf, no_color=True, highlight=False).print(table)
        rendered = buf.getvalue()
        assert "mila" in rendered
        assert "narval" in rendered
        assert "GIT_COMMIT=abc" in rendered
        assert table.row_count == 2
