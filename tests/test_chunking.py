from pathlib import Path

import pytest

from cluv.cli.submit_utils.chunking import (
    chunking_update_sbatch_args,
    get_n_chunks,
    get_time_from_sbatch_args,
)


class TestGetTimeFromSbatchArgs:
    @pytest.mark.parametrize(
        ("sbatch_args", "expected"),
        [
            (["--abc=123", "--time=01:00:00", "--def=456"], "01:00:00"),
            (["--abc=123", "-t=01:00:00", "--def=456"], "01:00:00"),
            (["--abc=123", "--time=01:00:00", "-t=1-03:00:00", "--def=456"], "1-03:00:00"),
        ],
    )
    def test_should_use_time_arg(self, sbatch_args: list[str], expected: str) -> None:
        assert get_time_from_sbatch_args(sbatch_args) == expected


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

    def test_should_use_custom_chunk_size(self) -> None:
        sbatch_args = ["--time=12:00:00"]
        env_vars = {}
        job_script = Path("my_script.sh")
        assert get_n_chunks(sbatch_args, env_vars, job_script, chunk_size=6) == 2


class TestChunkingUpdateSbatchArgs:
    @pytest.mark.parametrize(
        ("sbatch_args", "expected_sbatch_args"),
        [
            (
                ["--abc=123", "--time=01:00:00", "-t=20:30:00", "--def=456"],
                ["--abc=123", "--def=456", "--time=3:00:00", "--array=0-6%1"],
            ),
            (
                ["--abc=123", "-t=02:00:00", "--def=456"],
                ["--abc=123", "--def=456", "--time=3:00:00", "--array=0-0%1"],
            ),
            (
                ["--abc=123", "--time=00:00:00", "--def=456"],
                ["--abc=123", "--def=456", "--time=3:00:00", "--array=0-0%1"],
            ),
        ],
    )
    def test_update_sbatch_args(self, sbatch_args: list[str], expected_sbatch_args: str) -> None:
        n_chunks = get_n_chunks(sbatch_args, {}, Path("script.sh"))
        assert chunking_update_sbatch_args(n_chunks, sbatch_args) == expected_sbatch_args

    def test_update_sbatch_args_with_custom_chunk_size(self) -> None:
        sbatch_args = ["--time=12:00:00"]
        chunk_size = 6
        n_chunks = get_n_chunks(sbatch_args, {}, Path("script.sh"), chunk_size=chunk_size)
        result = chunking_update_sbatch_args(n_chunks, sbatch_args, chunk_size=chunk_size)
        assert result == ["--time=6:00:00", "--array=0-1%1"]
