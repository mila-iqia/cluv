import asyncio
import shlex
import subprocess
import textwrap
import unittest
import unittest.mock
from pathlib import Path
from unittest import mock

import pytest

import cluv.__main__ as cluv_main
import cluv.cli.init
import cluv.cli.submit
import cluv.cli.submit_utils.first
import cluv.remote
import cluv.slurm
import cluv.utils
from cluv.cli.submit import (
    ResolvedSbatchArgs,
    build_submit_command,
    ensure_clean_git_state,
    get_sbatch_command,
    sbatch_args_from_dict,
    submit,
    submit_first,
)
from cluv.cli.submit_utils.chunking import CHUNK_SIZE
from cluv.cli.sync import prepare_sync, sync_task_function
from cluv.config import get_cluv_config, load_cluv_config
from cluv.utils import current_cluster
from tests.test_integration import IN_GITHUB_CLOUD_CI


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

    cluv.cli.init()
    return project_dir


class TestSbatchArgsFromDict:
    def test_long_key_string_value(self) -> None:
        assert sbatch_args_from_dict({"time": "2:00:00"}) == ["--time=2:00:00"]

    def test_short_key_string_value(self) -> None:
        assert sbatch_args_from_dict({"N": "2"}) == ["-N", "2"]

    def test_true_long_key_is_bare_flag(self) -> None:
        assert sbatch_args_from_dict({"exclusive": True}) == ["--exclusive"]

    def test_true_short_key_is_bare_flag(self) -> None:
        assert sbatch_args_from_dict({"n": True}) == ["-n"]

    def test_empty_string_omitted(self) -> None:
        assert sbatch_args_from_dict({"gpus": ""}) == []

    def test_false_omitted(self) -> None:
        assert sbatch_args_from_dict({"requeue": False}) == []

    def test_multiple_flags_in_order(self) -> None:
        result = sbatch_args_from_dict({"time": "2:00:00", "gpus": "1", "exclusive": True})
        assert result == ["--time=2:00:00", "--gpus=1", "--exclusive"]


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
        sbatch_args = ["--account=my_account", "--mem=8G"]
        sbatch_command, submission_args = get_sbatch_command(
            cluster=cluster,
            job_script=sbatch_script,
            sbatch_args=sbatch_args,
            program_args=["program_arg_1", "program_arg_2"],
            git_commit="abecdef",
            chunking=None,
        )
        job_script_relative_path = sbatch_script.relative_to(fake_home)

        assert sbatch_command == (
            "bash --login -c 'MY_VAR=1 SPECIAL_MILA_VAR=xyz SBATCH_JOB_NAME=cluv-my_script "
            # Ugly, quite hard-coded.
            f"GIT_COMMIT=abecdef SBATCH_OUTPUT={results_path}/{cluster}_%j/slurm-%j.out "
            "sbatch --parsable --chdir=$HOME/my_project --account=my_account "
            f"--mem=8G $HOME/{job_script_relative_path} program_arg_1 program_arg_2'"
        )
        assert submission_args == ResolvedSbatchArgs(sbatch_args=sbatch_args)

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

        sbatch_args = []
        sbatch_command, submission_args = get_sbatch_command(
            cluster="mila",
            job_script=job_script,
            sbatch_args=sbatch_args,
            program_args=[],
            git_commit="abecdef",
            chunking=None,
        )

        assert sbatch_command == (
            "bash --login -c 'MY_VAR=2 SBATCH_JOB_NAME=cluv-my_script GIT_COMMIT=abecdef "
            f"SBATCH_OUTPUT={results_path}/mila_%j/slurm-%j.out "
            "sbatch --parsable --chdir=$HOME/my_project  $HOME/my_project/scripts/my_script.sh '"
        )
        assert submission_args == ResolvedSbatchArgs(sbatch_args=sbatch_args)

    def test_config_sbatch_args_prepended_to_cli_args(self, project_dir: Path) -> None:
        """Config-derived sbatch flags are prepended; CLI flags come last and can override."""
        p = project_dir / "pyproject.toml"
        results_path = "results"
        p.write_text(
            textwrap.dedent(
                f"""\
            [tool.cluv]
            results_path = "{results_path}"
            [tool.cluv.sbatch_args]
            time = "3:00:00"
            requeue = true

            [tool.cluv.clusters.mila]
            [tool.cluv.clusters.mila.sbatch_args]
            gpus = "a100:2"
            """
            )
        )
        job_script = project_dir / "job.sh"
        job_script.touch(0o755)
        config_sbatch_args = sbatch_args_from_dict(
            load_cluv_config(p).get_cluster_config("mila").sbatch_args[0]
        )
        sbatch_command, submission_args = get_sbatch_command(
            cluster="mila",
            job_script=job_script,
            sbatch_args=config_sbatch_args + ["--time=1:00:00"],  # CLI overrides the config time
            program_args=[],
            git_commit="abc123",
            chunking=None,
        )
        # Config flags come first (time, requeue, gpus), then CLI flag (--time=1:00:00).
        # sbatch uses last occurrence, so the CLI time wins.
        assert "--time=3:00:00" in sbatch_command
        assert "--requeue" in sbatch_command
        assert "--gpus=a100:2" in sbatch_command
        assert "--time=1:00:00" in sbatch_command
        # Config flags appear before CLI flags in the command string
        assert sbatch_command.index("--time=3:00:00") < sbatch_command.index("--time=1:00:00")
        assert submission_args == ResolvedSbatchArgs(
            sbatch_args=["--time=3:00:00", "--requeue", "--gpus=a100:2", "--time=1:00:00"]
        )

    def test_cluster_sbatch_args_override_global(self, project_dir: Path) -> None:
        """Cluster-level sbatch_args override global ones; empty string removes a flag."""
        p = project_dir / "pyproject.toml"
        results_path = "results"
        p.write_text(
            textwrap.dedent(
                f"""\
            [tool.cluv]
            results_path = "{results_path}"
            [tool.cluv.sbatch_args]
            gpus = "1"
            time = "2:00:00"
            [tool.cluv.clusters.cpu_cluster]
            [tool.cluv.clusters.cpu_cluster.sbatch_args]
            gpus = ""
            """
            )
        )
        job_script = project_dir / "job.sh"
        job_script.touch(0o755)
        config_sbatch_args = sbatch_args_from_dict(
            load_cluv_config(p).get_cluster_config("cpu_cluster").sbatch_args[0]
        )
        sbatch_command, submission_args = get_sbatch_command(
            cluster="cpu_cluster",
            job_script=job_script,
            sbatch_args=config_sbatch_args + [],
            program_args=[],
            git_commit="abc123",
            chunking=None,
        )
        # gpus removed by cluster override, time still present
        assert "--gpus" not in sbatch_command
        assert "--time=2:00:00" in sbatch_command
        assert submission_args == ResolvedSbatchArgs(sbatch_args=["--time=2:00:00"])

    def test_use_correct_time_value_when_chunking(self, project_dir: Path) -> None:
        p = project_dir / "pyproject.toml"
        results_path = "results"
        p.write_text(
            textwrap.dedent(
                f"""\
                [tool.cluv]
                results_path = "{results_path}"
                [tool.cluv.sbatch_args]
                time = "5:00:00"
                [tool.cluv.clusters.mila]
                """
            )
        )
        job_script = project_dir / "scripts" / "my_script.sh"
        job_script.parent.mkdir()
        job_script.write_text("#SBATCH --time=20:00:00")

        sbatch_command, submission_args = get_sbatch_command(
            cluster="mila",
            job_script=job_script,
            sbatch_args=["--time=10:00:00"],
            program_args=[],
            git_commit="abecdef",
            chunking=3,
        )

        expected_sbatch_args = ["--time=3:00:00", "--array=0-3%1"]

        assert " ".join(expected_sbatch_args) in sbatch_command
        assert submission_args == ResolvedSbatchArgs(sbatch_args=expected_sbatch_args, n_chunks=4)


class TestSubmitCliParsing:
    def test_job_script_can_be_omitted_when_using_separator(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            cluv_main, "submit", mock_submit := mock.AsyncMock(spec=cluv_main.submit)
        )

        cluv_main.main(["submit", "tamia", "--", "python", "main.py"])

        mock_submit.assert_called_once_with(
            **{
                "cluster": "tamia",
                "job_script": None,
                "sbatch_args": [],
                "program_args": ["python", "main.py"],
                "autocommit": False,
                "chunking": None,
            }
        )

    def test_sbatch_args_are_not_mistaken_for_job_script(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            cluv_main, "submit", mock_submit := mock.AsyncMock(spec=cluv_main.submit)
        )

        cluv_main.main(["submit", "tamia", "--mem=8G", "--", "python", "main.py"])

        mock_submit.assert_called_once_with(
            **{
                "cluster": "tamia",
                "job_script": None,
                "sbatch_args": ["--mem=8G"],
                "program_args": ["python", "main.py"],
                "autocommit": False,
                "chunking": None,
            }
        )

    def test_existing_hyphen_prefixed_path_is_kept_as_job_script(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            cluv_main, "submit", mock_submit := mock.AsyncMock(spec=cluv_main.submit)
        )
        job_script = tmp_path / "-job.sh"
        job_script.write_text("#!/bin/bash\n")
        monkeypatch.chdir(tmp_path)

        cluv_main.main(["submit", "tamia", str(job_script)])

        mock_submit.assert_awaited_once_with(
            **{
                "cluster": "tamia",
                "job_script": job_script,
                "sbatch_args": [],
                "program_args": [],
                "autocommit": False,
                "chunking": None,
            }
        )

    def test_chunking_with_value_is_recovered_from_sbatch_args(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--chunking=N` placed before `--` can get swallowed into the REMAINDER `sbatch_args`
        along with the other sbatch flags; it should still be parsed as `chunking=N` and not be
        forwarded to `sbatch`."""
        monkeypatch.setattr(
            cluv_main, "submit", mock_submit := mock.AsyncMock(spec=cluv_main.submit)
        )

        cluv_main.main(["submit", "tamia", "--chunking=6", "--time=24:00:00", "--", "sleep", "10"])

        mock_submit.assert_called_once_with(
            **{
                "cluster": "tamia",
                "job_script": None,
                "sbatch_args": ["--time=24:00:00"],
                "program_args": ["sleep", "10"],
                "autocommit": False,
                "chunking": 6,
            }
        )

    def test_bare_chunking_recovered_from_sbatch_args_uses_default_chunk_size(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A bare `--chunking` recovered from the REMAINDER `sbatch_args` should default to
        `CHUNK_SIZE`, not `True`."""
        monkeypatch.setattr(
            cluv_main, "submit", mock_submit := mock.AsyncMock(spec=cluv_main.submit)
        )

        cluv_main.main(["submit", "tamia", "--chunking", "--time=24:00:00", "--", "sleep", "10"])

        mock_submit.assert_called_once_with(
            **{
                "cluster": "tamia",
                "job_script": None,
                "sbatch_args": ["--time=24:00:00"],
                "program_args": ["sleep", "10"],
                "autocommit": False,
                "chunking": CHUNK_SIZE,
            }
        )


class TestBuildSubmitCommand:
    def test_build_submit_command_with_program_args(self) -> None:
        assert (
            build_submit_command(
                cluster="mila",
                job_script=Path("scripts/job.sh"),
                sbatch_args=[],
                program_args=["--flag"],
            )
            == "cluv submit mila scripts/job.sh -- --flag"
        )


class TestEnsureCleanGitState:
    def test_ensure_clean_git_state_exits_when_repo_dirty_without_autocommit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        messages: list[tuple[str, dict]] = []

        def mock_subprocess_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            assert kwargs.get("capture_output") is True
            assert kwargs.get("text") is True
            if command == ["git", "status", "--porcelain"]:
                return subprocess.CompletedProcess(
                    command, 0, stdout=" M cluv/cli/submit.py\n", stderr=""
                )
            raise AssertionError(f"Unexpected subprocess.run call: {command}")

        monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
        monkeypatch.setattr(
            cluv.cli.submit.console,
            "print",
            lambda message, **kwargs: messages.append((message, kwargs)),
        )

        with pytest.raises(SystemExit):
            ensure_clean_git_state()

        assert messages == [
            (
                "Working directory is dirty. Please commit your changes before submitting, or use "
                "`--autocommit` (`hydra.launcher.autocommit=True` when using Hydra).",
                {"style": "red"},
            )
        ]

    def test_ensure_clean_git_state_creates_commit_when_autocommit_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        launched_job_command = "cluv submit mila scripts/job.sh -- --flag"
        expected_commit_body = f"Launched job command:\n\n{launched_job_command}"
        command_calls: list[tuple[list[str], dict]] = []

        def mock_subprocess_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            command_calls.append((command, kwargs))
            if command == ["git", "status", "--porcelain"]:
                return subprocess.CompletedProcess(
                    command, 0, stdout=" M cluv/cli/submit.py\n?? notes.txt\n", stderr=""
                )
            if command == ["git", "add", "-u"]:
                assert kwargs.get("check") is True
                assert kwargs.get("capture_output") is True
                assert kwargs.get("text") is True
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            if command[:2] == ["git", "commit"]:
                assert kwargs.get("check") is True
                assert kwargs.get("capture_output") is True
                assert kwargs.get("text") is True
                assert command[2:4] == ["-m", "cluv submit: auto-commit tracked changes"]
                assert command[4] == "-m"
                assert command[5] == expected_commit_body
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            raise AssertionError(f"Unexpected subprocess.run call: {command}")

        def mock_subprocess_check_output(command: list[str], **kwargs) -> str:
            assert kwargs.get("text") is True
            if command == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
                return "main\n"
            if command == ["git", "rev-parse", "HEAD"]:
                return "dddddddddddddddddddddddddddddddddddddddd\n"
            raise AssertionError(f"Unexpected subprocess.check_output call: {command}")

        monkeypatch.setattr(subprocess, "run", mock_subprocess_run)
        monkeypatch.setattr(subprocess, "check_output", mock_subprocess_check_output)

        assert (
            ensure_clean_git_state(
                autocommit=True,
                submit_command=launched_job_command,
            )
            == "dddddddddddddddddddddddddddddddddddddddd"
        )
        assert [call[0] for call in command_calls[:3]] == [
            ["git", "status", "--porcelain"],
            ["git", "add", "-u"],
            [
                "git",
                "commit",
                "-m",
                "cluv submit: auto-commit tracked changes",
                "-m",
                expected_commit_body,
            ],
        ]

    def test_ensure_clean_git_state_raises_when_autocommit_without_builder(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def mock_subprocess_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
            assert kwargs.get("capture_output") is True
            assert kwargs.get("text") is True
            if command == ["git", "status", "--porcelain"]:
                return subprocess.CompletedProcess(
                    command, 0, stdout=" M cluv/cli/submit.py\n", stderr=""
                )
            raise AssertionError(f"Unexpected subprocess.run call: {command}")

        monkeypatch.setattr(subprocess, "run", mock_subprocess_run)

        with pytest.raises(ValueError, match="submit_command is required"):
            ensure_clean_git_state(autocommit=True)

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
    dummy_commit = "dummy_git_commit"
    monkeypatch.setattr(
        cluv.cli.submit,
        ensure_clean_git_state.__name__,
        mock_ensure_clean_git_state := unittest.mock.Mock(
            wraps=ensure_clean_git_state, side_effect=lambda *args, **kwargs: dummy_commit
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

    returned_job = await submit(
        cluster=here,
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        chunking=None,
    )

    assert returned_job
    assert returned_job.job_id == jobid
    mock_ensure_clean_git_state.assert_called_once()
    mock.assert_called_once()


async def test_submit_races_the_allocations_of_a_cluster(
    monkeypatch: pytest.MonkeyPatch, project_dir: Path
) -> None:
    """A cluster with two allocations gets one job per allocation, and the loser is cancelled."""
    cluster = "narval"
    (project_dir / "pyproject.toml").write_text(
        textwrap.dedent(
            f"""\
        [tool.cluv]
        results_path = "results"
        [tool.cluv.sbatch_args]
        time = "1:00:00"
        [tool.cluv.clusters.{cluster}]
        sbatch_args = [{{ account = "rrg-bengioy-ad" }}, {{ account = "def-bengioy" }}]
        """
        )
    )
    # Submit from the cluster itself, so that everything runs locally (no ssh, no sync).
    current_cluster_mock = unittest.mock.Mock(spec=current_cluster, return_value=cluster)
    monkeypatch.setattr(cluv.utils, current_cluster.__name__, current_cluster_mock)
    monkeypatch.setattr(cluv.cli.submit, current_cluster.__name__, current_cluster_mock)
    monkeypatch.setattr(
        cluv.cli.submit, ensure_clean_git_state.__name__, lambda **kwargs: "dummy_git_commit"
    )
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _: real_sleep(0))

    job_script = project_dir / "job.sh"
    job_script.write_text("#!/bin/bash\necho Hello World\n")

    # The job of the `def-` allocation starts right away; the `rrg-` one stays pending.
    rrg_job_id, def_job_id = 111, 222
    cancelled: list[int] = []

    async def fake_run(program_and_args: tuple[str, ...], **kwargs):
        full_command = shlex.join(program_and_args)

        def _result(stdout: str):
            return subprocess.CompletedProcess(
                program_and_args, returncode=0, stdout=stdout, stderr=""
            )

        if "sbatch --parsable" in full_command:
            assert "--time=1:00:00" in full_command  # global sbatch args are applied to both
            if "--account=rrg-bengioy-ad" in full_command:
                return _result(str(rrg_job_id))
            assert "--account=def-bengioy" in full_command
            return _result(str(def_job_id))
        if full_command.startswith(f"sacct -j {rrg_job_id} --format=State"):
            return _result("CANCELLED" if rrg_job_id in cancelled else "PENDING")
        if full_command.startswith(f"sacct -j {def_job_id} --format=State"):
            return _result("RUNNING")
        if full_command == f"scancel {rrg_job_id}":
            cancelled.append(rrg_job_id)
            return _result("")
        pytest.fail(f"Unexpected command: {full_command}")

    run_name = cluv.remote.run.__name__
    for module in (cluv.remote, cluv.slurm, cluv.cli.submit, cluv.cli.submit_utils.first):
        monkeypatch.setattr(module, run_name, unittest.mock.AsyncMock(wraps=fake_run))

    returned_job = await submit(
        cluster=cluster, job_script=job_script, sbatch_args=[], program_args=[]
    )

    assert returned_job
    assert returned_job.job_id == def_job_id
    # The allocation that was used is saved with the job.
    assert returned_job.sbatch_args == ["--time=1:00:00", "--account=def-bengioy"]
    assert cancelled == [rrg_job_id]


@pytest.mark.parametrize(
    "runs_first_on_current_cluster",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                IN_GITHUB_CLOUD_CI,
                reason=(
                    "This test is racy on the GitHub Cloud CI's shared runners, not sure why. "
                    "Not strict: it sometimes passes there too, and that's fine."
                ),
                strict=False,
            ),
        ),
    ],
    ids=["current_cluster_runs_first", "other_cluster_runs_first"],
)
async def test_submit_first_considers_current_cluster(
    monkeypatch: pytest.MonkeyPatch,
    mock_current_cluster: str,
    cluv_project_dir: Path,
    runs_first_on_current_cluster: bool,
) -> None:
    """Test that `submit first` also considers the current cluster as an option.

    Test that it submits a job locally, and also cancels the local job.
    """
    run_commands: list[tuple[str, ...]] = []
    this_cluster_jobid = 123
    other_cluster_jobid = 456
    this_cluster_wait_time = 1 if runs_first_on_current_cluster else 3
    other_cluster_wait_time = 3 if runs_first_on_current_cluster else 1
    scancel_received_on_this_cluster = False
    scancel_received_on_other_cluster = False
    real_sleep = asyncio.sleep
    # Speed up the test by patching sleep
    # (we're not doing real sacct / scancel / sbatch.)
    monkeypatch.setattr(asyncio, "sleep", lambda x: real_sleep(0.1 * x))

    async def fake_run(
        program_and_args: tuple[str, ...],
        input: str | None = None,
        warn: bool = False,
        hide: cluv.remote.Hide = False,
        **other_kwargs,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal this_cluster_wait_time, other_cluster_wait_time
        nonlocal scancel_received_on_this_cluster, scancel_received_on_other_cluster
        full_command = shlex.join(program_and_args)
        run_commands.append(program_and_args)

        def _result(stdout: str):
            return subprocess.CompletedProcess(
                program_and_args, returncode=0, stdout=stdout, stderr=""
            )

        # `ssh` to the other cluster: not just `("ssh", other_cluster, ...)`, since `Remote.run`
        # inserts `-oControlMaster=...`-style multiplexing flags before the hostname whenever the
        # local `~/.ssh/config` doesn't already set them (e.g. on a CI runner).
        runs_via_ssh_to_other_cluster = (
            program_and_args[0] == "ssh" and other_cluster in program_and_args
        )

        print(f"Running command: {full_command}")
        if not runs_via_ssh_to_other_cluster and "sbatch --parsable" in full_command:
            return _result(str(this_cluster_jobid))
        if runs_via_ssh_to_other_cluster and "sbatch --parsable" in full_command:
            return _result(str(other_cluster_jobid))

        # Querying for the job's state:
        if full_command.startswith(f"sacct -j {this_cluster_jobid} --format=State"):
            this_cluster_wait_time -= 1
            if scancel_received_on_this_cluster:
                return _result("CANCELLED")
            if this_cluster_wait_time > 0:
                return _result("PENDING")
            return _result("RUNNING")
        if (
            runs_via_ssh_to_other_cluster
            and f"sacct -j {other_cluster_jobid} --format=State" in full_command
        ):
            other_cluster_wait_time -= 1
            if scancel_received_on_other_cluster:
                return _result("CANCELLED")
            if other_cluster_wait_time > 0:
                return _result("PENDING")
            return _result("RUNNING")

        # Cancelling once the jobs are running.
        if (
            runs_first_on_current_cluster
            and runs_via_ssh_to_other_cluster
            and f"scancel {other_cluster_jobid}" in full_command
        ):
            scancel_received_on_other_cluster = True
            return _result("")
        if not runs_first_on_current_cluster and full_command == f"scancel {this_cluster_jobid}":
            scancel_received_on_this_cluster = True
            return _result("")
        print(*run_commands, sep="\n")
        pytest.fail(f"Unexpected command: {full_command}, {runs_first_on_current_cluster=}")

    monkeypatch.setattr(
        cluv.remote, cluv.remote.run.__name__, _mock := unittest.mock.AsyncMock(wraps=fake_run)
    )
    monkeypatch.setattr(
        cluv.slurm, cluv.slurm.run.__name__, _mock := unittest.mock.AsyncMock(wraps=fake_run)
    )
    monkeypatch.setattr(
        cluv.cli.submit,
        cluv.cli.submit.run.__name__,
        _mock := unittest.mock.AsyncMock(wraps=fake_run),
    )
    monkeypatch.setattr(
        cluv.cli.submit_utils.first,
        cluv.cli.submit_utils.first.run.__name__,
        _mock := unittest.mock.AsyncMock(wraps=fake_run),
    )

    # Patch the shared part of `cluv sync` so it returns a Remote that is not for the current
    # cluster, and make the per-cluster sync a no-op.
    other_cluster = "mila" if mock_current_cluster != "mila" else "tamia"
    # Should be fine to use a 'real' remote here, since we patch the `run` function that is used
    # everywhere. There shouldn't be an actual call to `ssh other_cluster` that goes though.
    other_cluster_remote = cluv.remote.Remote(hostname=other_cluster)
    monkeypatch.setattr(
        cluv.cli.submit,
        prepare_sync.__name__,
        mock_prepare_sync := unittest.mock.AsyncMock(return_value=[other_cluster_remote]),
    )
    monkeypatch.setattr(
        cluv.cli.submit,
        sync_task_function.__name__,
        mock_sync_task := unittest.mock.AsyncMock(return_value=[]),
    )

    job_script = cluv_project_dir / "my_script.sh"
    job_script.parent.mkdir(exist_ok=True)
    job_script.write_text("#!/bin/bash\necho Hello World\n")
    job_script.touch(0o755)

    sbatch_args = ["--account=my_account", "--mem=8G"]
    program_args = ["program_arg_1", "program_arg_2"]
    dummy_commit = "dummy_git_commit"
    returned_job = await submit_first(
        job_script=job_script,
        sbatch_args=sbatch_args,
        program_args=program_args,
        git_commit=dummy_commit,
        chunking=None,
    )
    assert returned_job
    mock_prepare_sync.assert_awaited_once()
    # Only the other cluster is synced (the current cluster doesn't need to be).
    mock_sync_task.assert_awaited_once()
    assert mock_sync_task.await_args.kwargs["remote"] is other_cluster_remote
    if runs_first_on_current_cluster:
        assert returned_job.job_id == this_cluster_jobid
    else:
        assert returned_job.job_id == other_cluster_jobid


async def test_submit_first_doesnt_wait_for_the_slowest_cluster_to_sync(
    monkeypatch: pytest.MonkeyPatch,
    cluv_project_dir: Path,
) -> None:
    """`submit first` submits on each cluster as soon as *that* cluster is done syncing.

    A cluster whose `uv sync` takes forever must not delay (or prevent) the submission of the job on
    the clusters that are already synced. See https://github.com/mila-iqia/cluv/issues/165.
    """
    fast_cluster, slow_cluster = "mila", "fir"
    fast_jobid, slow_jobid = 111, 222
    slow_sync_started = asyncio.Event()
    never_set = asyncio.Event()
    slow_sync_was_cancelled = False
    submitted_on: list[str] = []

    monkeypatch.setattr(cluv.utils, current_cluster.__name__, lambda: None)
    monkeypatch.setattr(cluv.cli.submit, current_cluster.__name__, lambda: None)

    async def fake_sync_task_function(report_progress, remote: cluv.remote.Remote) -> list[Path]:
        report_progress(progress=0, total=4, info="Running 'uv sync'")
        if remote.hostname == fast_cluster:
            return []
        slow_sync_started.set()
        nonlocal slow_sync_was_cancelled
        try:
            # This sync never finishes: the fast cluster has to win the race without it.
            await never_set.wait()
        except asyncio.CancelledError:
            slow_sync_was_cancelled = True
            raise
        return []

    monkeypatch.setattr(
        cluv.cli.submit,
        sync_task_function.__name__,
        mock_sync_task := unittest.mock.AsyncMock(wraps=fake_sync_task_function),
    )

    async def fake_run(
        program_and_args: tuple[str, ...],
        input: str | None = None,
        warn: bool = False,
        hide: cluv.remote.Hide = False,
        **other_kwargs,
    ) -> subprocess.CompletedProcess[str]:
        full_command = shlex.join(program_and_args)

        def _result(stdout: str):
            return subprocess.CompletedProcess(
                program_and_args, returncode=0, stdout=stdout, stderr=""
            )

        if "sbatch --parsable" in full_command:
            # `Remote.run` inserts `-oControlMaster=...`-style multiplexing flags before the
            # hostname whenever `~/.ssh/config` doesn't already set them (e.g. on a CI runner),
            # so check the hostname's presence in the argument tuple rather than a fixed prefix.
            cluster = fast_cluster if fast_cluster in program_and_args else slow_cluster
            submitted_on.append(cluster)
            return _result(str(fast_jobid if cluster == fast_cluster else slow_jobid))
        if f"sacct -j {fast_jobid}" in full_command:
            return _result("RUNNING")
        if f"sacct -j {slow_jobid}" in full_command:
            return _result("CANCELLED")
        if "scancel" in full_command:
            return _result("")
        pytest.fail(f"Unexpected command: {full_command}")

    # Both clusters go through SSH here (there's no "current" cluster to run locally on), so only
    # `cluv.remote.run` is ever exercised; patching `cluv.slurm.run` / `cluv.cli.submit.run` too
    # would be needless.
    monkeypatch.setattr(
        cluv.remote, cluv.remote.run.__name__, mock_run := unittest.mock.AsyncMock(wraps=fake_run)
    )

    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda x: real_sleep(0.01 * x))
    monkeypatch.setattr(
        cluv.cli.submit,
        prepare_sync.__name__,
        mock_prepare_sync := unittest.mock.AsyncMock(
            return_value=[cluv.remote.Remote(hostname=c) for c in (fast_cluster, slow_cluster)]
        ),
    )

    job_script = cluv_project_dir / "my_script.sh"
    job_script.write_text("#!/bin/bash\necho Hello World\n")

    # The timeout is what makes this a regression test: waiting for the slow cluster to be done
    # syncing before submitting anything would block here forever.
    returned_job = await asyncio.wait_for(
        submit_first(
            job_script=job_script, sbatch_args=[], program_args=[], git_commit="dummy_git_commit"
        ),
        timeout=30,
    )

    # The job was submitted on (and won by) the fast cluster while the slow one was still syncing.
    assert slow_sync_started.is_set()
    assert returned_job is not None
    assert returned_job.job_id == fast_jobid
    assert returned_job.cluster == fast_cluster
    # Nothing was ever submitted on the cluster that was still syncing when the race ended.
    assert submitted_on == [fast_cluster]
    assert slow_sync_was_cancelled
    mock_prepare_sync.assert_awaited_once()
    # Both clusters started syncing (the slow one just never got to finish).
    assert mock_sync_task.await_count == 2
    mock_run.assert_awaited()


async def test_submit_first_only_syncs_once_for_clusters_sharing_a_filesystem(
    monkeypatch: pytest.MonkeyPatch,
    cluv_project_dir: Path,
) -> None:
    """Two clusters racing in `submit first` that share a filesystem (see
    `CLUSTERS_SHARING_A_FILESYSTEM`) must not sync concurrently -- that races on the same on-disk
    checkout. Only one of them should actually run `sync_task_function`; the other should wait for
    it and then still be a candidate to win the race.
    """
    # Reuse two clusters already present in the default test config (see `cluv_project_dir`) so
    # `get_cluv_config().get_cluster_config(...)` resolves; only their "shares a filesystem"
    # membership is faked here.
    shared_a, shared_b = "rorqual", "vulcan"
    monkeypatch.setattr(
        cluv.cli.submit, "CLUSTERS_SHARING_A_FILESYSTEM", [frozenset({shared_a, shared_b})]
    )
    monkeypatch.setattr(cluv.utils, current_cluster.__name__, lambda: None)
    monkeypatch.setattr(cluv.cli.submit, current_cluster.__name__, lambda: None)

    async def fake_sync_task_function(report_progress, remote: cluv.remote.Remote) -> list[Path]:
        report_progress(progress=0, total=4, info="Running 'uv sync'")
        return []

    monkeypatch.setattr(
        cluv.cli.submit,
        sync_task_function.__name__,
        mock_sync_task := unittest.mock.AsyncMock(wraps=fake_sync_task_function),
    )

    jobid_by_cluster = {shared_a: 111, shared_b: 222}

    async def fake_run(
        program_and_args: tuple[str, ...],
        input: str | None = None,
        warn: bool = False,
        hide: cluv.remote.Hide = False,
        **other_kwargs,
    ) -> subprocess.CompletedProcess[str]:
        full_command = shlex.join(program_and_args)

        def _result(stdout: str):
            return subprocess.CompletedProcess(
                program_and_args, returncode=0, stdout=stdout, stderr=""
            )

        if "sbatch --parsable" in full_command:
            cluster = shared_a if shared_a in program_and_args else shared_b
            return _result(str(jobid_by_cluster[cluster]))
        for cluster, jobid in jobid_by_cluster.items():
            if f"sacct -j {jobid}" in full_command:
                return _result("RUNNING" if cluster == shared_a else "CANCELLED")
        if "scancel" in full_command:
            return _result("")
        pytest.fail(f"Unexpected command: {full_command}")

    monkeypatch.setattr(
        cluv.remote, cluv.remote.run.__name__, unittest.mock.AsyncMock(wraps=fake_run)
    )

    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda x: real_sleep(0.01 * x))
    monkeypatch.setattr(
        cluv.cli.submit,
        prepare_sync.__name__,
        unittest.mock.AsyncMock(
            return_value=[cluv.remote.Remote(hostname=c) for c in (shared_a, shared_b)]
        ),
    )

    job_script = cluv_project_dir / "my_script.sh"
    job_script.write_text("#!/bin/bash\necho Hello World\n")

    returned_job = await asyncio.wait_for(
        submit_first(
            job_script=job_script, sbatch_args=[], program_args=[], git_commit="dummy_git_commit"
        ),
        timeout=30,
    )

    assert returned_job is not None
    assert returned_job.cluster == shared_a
    # Only one of the two clusters actually ran the sync step; the other waited for it instead.
    mock_sync_task.assert_awaited_once()
    assert mock_sync_task.await_args.kwargs["remote"].hostname == shared_a


async def test_submit_first_returns_none_when_no_cluster_can_submit(
    monkeypatch: pytest.MonkeyPatch,
    cluv_project_dir: Path,
) -> None:
    """`submit first` gives up (instead of waiting for a job that will never come)."""
    clusters = ["mila", "fir"]

    monkeypatch.setattr(cluv.utils, current_cluster.__name__, lambda: None)
    monkeypatch.setattr(cluv.cli.submit, current_cluster.__name__, lambda: None)

    async def fake_run(
        program_and_args: tuple[str, ...], **other_kwargs
    ) -> subprocess.CompletedProcess[str]:
        full_command = shlex.join(program_and_args)
        assert "sbatch --parsable" in full_command, f"Unexpected command: {full_command}"
        # The sync worked, but `sbatch` is rejecting the job (bad account, invalid flag, ...).
        return subprocess.CompletedProcess(
            program_and_args, returncode=1, stdout="", stderr="sbatch: error: Invalid account"
        )

    # Both clusters go through SSH here (there's no "current" cluster to run locally on), so only
    # `cluv.remote.run` is ever exercised; patching `cluv.slurm.run` / `cluv.cli.submit.run` too
    # would be needless.
    monkeypatch.setattr(
        cluv.remote, cluv.remote.run.__name__, mock_run := unittest.mock.AsyncMock(wraps=fake_run)
    )
    monkeypatch.setattr(
        cluv.cli.submit,
        sync_task_function.__name__,
        mock_sync_task := unittest.mock.AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        cluv.cli.submit,
        prepare_sync.__name__,
        mock_prepare_sync := unittest.mock.AsyncMock(
            return_value=[cluv.remote.Remote(hostname=cluster) for cluster in clusters]
        ),
    )

    job_script = cluv_project_dir / "my_script.sh"
    job_script.write_text("#!/bin/bash\necho Hello World\n")

    returned_job = await asyncio.wait_for(
        submit_first(
            job_script=job_script, sbatch_args=[], program_args=[], git_commit="dummy_git_commit"
        ),
        timeout=30,
    )
    assert returned_job is None
    mock_prepare_sync.assert_awaited_once()
    assert mock_sync_task.await_count == len(clusters)
    mock_run.assert_awaited()
