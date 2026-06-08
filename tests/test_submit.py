import shlex
import subprocess
import textwrap
import unittest
import unittest.mock
from pathlib import Path

import pytest

import cluv.cli.init
import cluv.cli.submit
import cluv.remote
import cluv.utils
from cluv.cli.submit import ensure_clean_git_state, get_sbatch_command, submit
from cluv.config import get_cluv_config
from cluv.utils import current_cluster


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


@pytest.fixture
def cluv_project_dir(project_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(project_dir)  # Set current working dir

    # def uv_init_without_git():
    #     subprocess.check_output(("uv", "init", "--package", "--vcs", "none"), text=True)

    # # from cluv.cli.init import run_uv_init

    # monkeypatch.setattr(
    #     "cluv.cli.init",
    #     run_uv_init.__name__,
    #     mock := unittest.mock.Mock(uv_init_without_git),
    # )
    # from cluv.cli import init

    cluv.cli.init()
    # mock.assert_called_once()
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


@pytest.fixture(params=["mila", "tamia", "rorqual"])
def mock_current_cluster(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    cluster = getattr(request, "param", "mila")
    mock = unittest.mock.Mock(spec=current_cluster, return_value=cluster)
    monkeypatch.setattr(cluv.utils, current_cluster.__name__, mock)
    monkeypatch.setattr(cluv.cli.submit, current_cluster.__name__, mock)
    yield cluster
    mock.assert_called()


async def test_can_submit_on_current_cluster(
    monkeypatch: pytest.MonkeyPatch, mock_current_cluster: str, cluv_project_dir: Path
) -> None:
    # This is a very basic test, just to check that we can call the function without error.
    # A more thorough test would require mocking the sync and sbatch functions, which is a bit more work.
    dummy_commit = "dummy_git_commit"
    monkeypatch.setattr(
        cluv.cli.submit,
        ensure_clean_git_state.__name__,
        mock_ensure_clean_git_state := unittest.mock.Mock(
            wraps=ensure_clean_git_state, side_effect=lambda: dummy_commit
        ),
    )
    here = mock_current_cluster
    monkeypatch.setenv("CC_CLUSTER", here)

    jobid = 123

    sbatch_args = ["--account=my_account", "--mem=8G"]
    program_args = ["program_arg_1", "program_arg_2"]

    async def fake_run(
        program_and_args: tuple[str, ...],
        input: str | None = None,
        warn: bool = False,
        hide: cluv.remote.Hide = False,
        **other_kwargs,
    ) -> subprocess.CompletedProcess[str]:
        full_command = shlex.join(program_and_args)
        assert (
            "ssh" not in full_command
        )  # Should not SSH since we're submitting to the current cluster.
        assert " ".join(program_args) in full_command
        assert " ".join(sbatch_args) in full_command
        assert "sbatch --parsable" in full_command
        return subprocess.CompletedProcess(
            program_and_args, returncode=0, stdout=f"{jobid}", stderr=""
        )

    monkeypatch.setattr(
        cluv.remote, cluv.remote.run.__name__, mock := unittest.mock.Mock(wraps=fake_run)
    )
    monkeypatch.setattr(
        cluv.cli.submit, cluv.cli.submit.run.__name__, mock := unittest.mock.Mock(wraps=fake_run)
    )

    job_script = cluv_project_dir / "my_script.sh"
    job_script.parent.mkdir(exist_ok=True)
    job_script.write_text("#!/bin/bash\necho Hello World\n")
    job_script.touch(0o755)

    returned_jobid = await submit(
        cluster=here,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
    )

    assert returned_jobid == jobid
    mock_ensure_clean_git_state.assert_called_once()
    mock.assert_called_once()
