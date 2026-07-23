from pathlib import Path

import pytest

from cluv.cli.submit_utils.chunking import (
    chunking_update_sbatch_args,
    get_n_chunks,
    get_time_from_sbatch_args,
)


class TestGetTimeFromSbatchArgs:
    @pytest.mark.parametrize("time_arg", ["--time=01:00:00", "-t=01:00:00"])
    def test_should_use_time_arg(self, time_arg: str) -> None:
        "Should return the value of --time arg"
        sbatch_args = ["--abc=123", time_arg, "--def=456"]
        assert get_time_from_sbatch_args(sbatch_args) == "01:00:00"

    def test_multiple_time_values(self) -> None:
        "Should only return the value of the last time argument"
        sbatch_args = ["--abc=123", "--time=01:00:00", "-t=1-03:00:00", "--def=456"]
        assert get_time_from_sbatch_args(sbatch_args) == "1-03:00:00"


class TestGetNumberOfChunks:
    @pytest.mark.parametrize("time_arg", ["--time=12:00:00", "-t=12:00:00"])
    def test_should_get_correct_number_of_chunks_with_sbatch_args(self, time_arg: str) -> None:
        sbatch_args = ["--abc=123", time_arg, "--def=456"]
        env_vars = {}
        job_script = Path("my_script.sh")
        assert get_n_chunks(sbatch_args, env_vars, job_script) == 4

    @pytest.mark.parametrize("time_arg", ["--time=12:00:00", "-t=12:00:00"])
    def test_should_get_correct_number_of_chunks_with_script_header(
        self, tmp_path: Path, time_arg: str
    ) -> None:
        sbatch_args = ["--abc=123", "--def=456"]
        env_vars = {}
        job_script = tmp_path / "my_script"
        job_script.write_text(f"#SBATCH {time_arg}")

        assert get_n_chunks(sbatch_args, env_vars, job_script) == 4

    def test_should_get_correct_number_of_chunks_with_env_vars(self) -> None:
        sbatch_args = ["--abc=123", "--def=456"]
        env_vars = {"SBATCH_TIMELIMIT": "12:00:00"}
        job_script = Path("my_script.sh")
        assert get_n_chunks(sbatch_args, env_vars, job_script) == 4


class TestChunkingUpdateSbatchArgs:
    def test_should_replace_sbatch_time_args(self) -> None:
        sbatch_args = ["--abc=123", "--time=01:00:00", "-t=20:30:00", "--def=456"]
        env_vars = {}
        job_script = Path("my_script.sh")

        expected_sbatch_args = ["--abc=123", "--def=456", "--time=3:00:00", "--array=0-6%1"]

        assert (
            chunking_update_sbatch_args(sbatch_args, env_vars, job_script) == expected_sbatch_args
        )

    def test_should_return_one_chunk_if_time_inferior_to_a_chunk(self) -> None:
        sbatch_args = ["--abc=123", "-t=02:00:00", "--def=456"]
        env_vars = {}
        job_script = Path("my_script.sh")

        expected_sbatch_args = ["--abc=123", "--def=456", "--time=3:00:00", "--array=0-0%1"]

        assert (
            chunking_update_sbatch_args(sbatch_args, env_vars, job_script) == expected_sbatch_args
        )

    def test_should_return_one_chunk_if_null_time(self) -> None:
        sbatch_args = ["--abc=123", "--time=00:00:00", "--def=456"]
        env_vars = {}
        job_script = Path("my_script.sh")

        expected_sbatch_args = ["--abc=123", "--def=456", "--time=3:00:00", "--array=0-0%1"]

        assert (
            chunking_update_sbatch_args(sbatch_args, env_vars, job_script) == expected_sbatch_args
        )
