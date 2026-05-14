"""Unit tests for cluv/cli/init.py check functions."""

import importlib
import shutil
import textwrap
from pathlib import Path

import pytest

from cluv.cli.init import (
    DEFAULT_RESULTS_PATH,
    JOB_SCRIPT_PATH,
    check_cluv_config,
    check_git,
    check_home_dir,
    check_job_script,
    check_symlink_to_scratch,
    init,
)
from cluv.config import load_cluv_config

REPO_ROOT = Path(__file__).resolve().parents[1]
CLUV_INIT_MODULE = importlib.import_module("cluv.cli.init")


class TestCheckHomeDir:
    def test_not_under_home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_home_dir() should raise an error if the current directory is not under the user's home directory"""
        monkeypatch.setattr(
            Path, "home", lambda: str(tmp_path)
        )  # Set the home directory to tmp_path
        monkeypatch.chdir(
            tmp_path.parent
        )  # Set the current work dir to the parent of tmp_path, which is not under the "home" directory

        with pytest.raises(
            RuntimeError, match="cluv init should be run in a directory under your home directory."
        ):
            check_home_dir()


class TestGitCheck:
    def test_not_in_git_repo(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_git() should raise an error if the current directory is not a git repository"""
        monkeypatch.chdir(tmp_path)  # Set the working dir to tmp_path

        with pytest.raises(RuntimeError, match="Error when checking git remote: "):
            check_git()


class TestCheckCluvConfig:
    def test_add_missing_cluv_config(self, tmp_path: Path) -> None:
        """check_cluv_config() should add a cluv config section if the toml doesn't have it"""
        p = tmp_path / "pyproject.toml"
        p.touch()

        check_cluv_config(p)
        config = load_cluv_config(p)
        expected_config = load_cluv_config(REPO_ROOT / "pyproject.toml")

        assert config.results_path == expected_config.results_path
        assert config.env == expected_config.env
        assert config.clusters_names == expected_config.clusters_names
        assert config.clusters == expected_config.clusters

    def test_keep_existing_cluv_config(self, tmp_path: Path) -> None:
        """check_cluv_config() should not overwrite an existing cluv config"""
        p = tmp_path / "pyproject.toml"
        p.write_text(textwrap.dedent(
            """\
            [tool.cluv]
            clusters = {"mila" = {}}
            results_path = "results"
            """
        ))

        check_cluv_config(p)
        config = load_cluv_config(p)

        assert config.clusters_names == ["mila"]
        assert config.results_path == "results"


# TODO : fixture to set environment variables ?
class TestSymlinkCheck:
    def test_no_symlink_if_results_path_is_none(self, tmp_path) -> None:
        """check_symlink_to_scratch() should not create a symlink if the results_path is None"""
        check_symlink_to_scratch(tmp_path, None)

        assert not (tmp_path / DEFAULT_RESULTS_PATH).exists()

    def test_no_symlink_if_scratch_not_set(self, tmp_path, monkeypatch) -> None:
        """check_symlink_to_scratch() should not create a symlink if the $SCRATCH env var is not set"""
        monkeypatch.delenv("SCRATCH", raising=False)

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH)

        assert not (tmp_path / DEFAULT_RESULTS_PATH).exists()

    def test_create_symlink(self, tmp_path, monkeypatch) -> None:
        """check_symlink_to_scratch() should create a symlink from results_path to scratch"""
        scratch_path = tmp_path / "scratch"
        monkeypatch.setenv("SCRATCH", str(scratch_path))
        expected_results_path = tmp_path / DEFAULT_RESULTS_PATH
        expected_results_scratch_path = scratch_path / DEFAULT_RESULTS_PATH / tmp_path.name

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH)

        assert expected_results_path.exists()
        assert expected_results_path.is_symlink()
        assert expected_results_scratch_path.exists()
        assert expected_results_path.resolve() == expected_results_scratch_path.resolve()


    def test_keep_existing_symlink(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """check_symlink_to_scratch() should not overwrite an existing symlink not pointing to scratch"""
        scratch_path = tmp_path / "scratch"
        monkeypatch.setenv("SCRATCH", str(scratch_path))
        expected_results_path = tmp_path / DEFAULT_RESULTS_PATH
        expected_results_scratch_path = scratch_path / DEFAULT_RESULTS_PATH / tmp_path.name
        expected_results_path.symlink_to(
            tmp_path / "some_other_folder"
        )  # Create a symlink pointing to a new location

        check_symlink_to_scratch(tmp_path, DEFAULT_RESULTS_PATH)

        # The original symlink should be kept, and not changed to point to scratch
        assert expected_results_path.is_symlink()
        assert expected_results_path.resolve() == (tmp_path / "some_other_folder").resolve()
        assert not expected_results_scratch_path.exists()


class TestJobScriptCheck:
    def test_no_job_script_if_results_path_is_none(self, tmp_path: Path) -> None:
        """check_job_script() should not create a job script if the results_path is None"""

        check_job_script(tmp_path, None)

        assert not (tmp_path / JOB_SCRIPT_PATH).exists()

    def test_keep_existing_job_script(self, tmp_path: Path) -> None:
        """check_job_script() should not overwrite an existing job script"""
        job_script_path = tmp_path / JOB_SCRIPT_PATH
        job_script_path.parent.mkdir(exist_ok=True)
        job_script_path.write_text("#!/bin/bash\necho 'Hello world!'")

        check_job_script(tmp_path, DEFAULT_RESULTS_PATH)

        assert job_script_path.exists()
        assert job_script_path.read_text() == "#!/bin/bash\necho 'Hello world!'"

    def test_create_missing_job_scripts_from_templates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_home = tmp_path / "home"
        project_root = fake_home / "my_project"
        project_root.mkdir(parents=True)
        monkeypatch.setattr(Path, "home", lambda: fake_home)

        check_job_script(project_root, "outputs")

        job_script = project_root / "scripts" / "job.sh"
        safe_job_script = project_root / "scripts" / "safe_job.sh"

        assert job_script.exists()
        assert safe_job_script.exists()
        assert "#SBATCH --output=outputs/%j/slurm-%j.out" in job_script.read_text()

        safe_job_script_content = safe_job_script.read_text()
        assert 'project_name="my_project"' in safe_job_script_content
        assert 'project_root="$HOME/my_project"' in safe_job_script_content
        assert 'results_path="outputs"' in safe_job_script_content
        assert "results_dir" not in safe_job_script_content
        assert "mkdir -p $project_root_in_tmpdir/$results_path" in safe_job_script_content
        assert (
            "rsync --update --recursive $project_root/$results_path/$SLURM_JOB_ID "
            "$project_root_in_tmpdir/$results_path/"
        ) in safe_job_script_content
        assert (
            "rsync --update --recursive $project_root_in_tmpdir/$results_path/$SLURM_JOB_ID "
            "$project_root/$results_path/"
        ) in safe_job_script_content

    def test_replace_results_dir_from_legacy_template(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_home = tmp_path / "home"
        project_root = fake_home / "my_project"
        project_root.mkdir(parents=True)
        monkeypatch.setattr(Path, "home", lambda: fake_home)

        templates_dir = tmp_path / "templates"
        templates_dir.mkdir()
        legacy_script = templates_dir / "legacy_job.sh"
        legacy_script.write_text(
            textwrap.dedent(
                """\
                #!/bin/bash
                #SBATCH --output=logs/%j/slurm-%j.out
                results_dir="logs"
                echo "Using $results_dir"
                """
            )
        )
        monkeypatch.setattr(CLUV_INIT_MODULE, "_get_script_templates_path", lambda: templates_dir)

        check_job_script(project_root, "outputs")

        generated_legacy_script = project_root / "scripts" / "legacy_job.sh"
        assert generated_legacy_script.exists()
        generated_legacy_script_content = generated_legacy_script.read_text()
        assert '#SBATCH --output=outputs/%j/slurm-%j.out' in generated_legacy_script_content
        assert 'results_path="outputs"' in generated_legacy_script_content
        assert "Using $results_path" in generated_legacy_script_content
        assert "results_dir" not in generated_legacy_script_content


class TestInitIntegration:
    """Integration tests for the init() function that run the full init flow locally."""

    @pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")
    def test_init_with_path_creates_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """init(path=<name>) creates and initializes a project directory end-to-end."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        new_project = fake_home / "my_project"

        monkeypatch.setattr(Path, "home", lambda: fake_home)
        monkeypatch.delenv("SCRATCH", raising=False)
        monkeypatch.chdir(tmp_path)  # ensures cwd is restored after the test

        init(path=new_project)

        assert new_project.is_dir()
        pyproject_path = new_project / "pyproject.toml"
        assert pyproject_path.exists()

        config = load_cluv_config(pyproject_path)
        assert config.results_path is not None

        assert (new_project / "scripts").is_dir()
        assert (new_project / JOB_SCRIPT_PATH).exists()

    @pytest.mark.skipif(shutil.which("uv") is None, reason="uv is not installed")
    def test_init_without_path_uses_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """init() without a path argument runs in the current directory."""
        fake_home = tmp_path / "home"
        project_dir = fake_home / "my_project"
        project_dir.mkdir(parents=True)

        monkeypatch.setattr(Path, "home", lambda: fake_home)
        monkeypatch.delenv("SCRATCH", raising=False)
        monkeypatch.chdir(project_dir)

        init()

        pyproject_path = project_dir / "pyproject.toml"
        assert pyproject_path.exists()

        config = load_cluv_config(pyproject_path)
        assert config.results_path is not None

