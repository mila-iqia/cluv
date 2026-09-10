from pathlib import Path

import pytest

from cluv.cli.submit_utils.chunking import apply_chunking
from cluv.config import SbatchArgs


class TestApplyChunking:
    def test_chunking_disabled_is_a_passthrough(self) -> None:
        sbatch_args: SbatchArgs = {"time": "12:00:00"}
        n_chunks, result = apply_chunking(sbatch_args, job_script=None, chunking=None, env_vars={})
        assert n_chunks is None
        assert result == sbatch_args

    @pytest.mark.parametrize("time_key", ["time", "t"])
    def test_uses_time_from_sbatch_args(self, time_key: str) -> None:
        n_chunks, result = apply_chunking(
            {"abc": "123", time_key: "12:00:00"}, job_script=None, chunking=3, env_vars={}
        )
        assert n_chunks == 4
        assert result["time"] == "03:00:00"
        assert result["array"] == "0-3%1"
        assert "t" not in result

    def test_last_time_key_in_dict_is_used(self) -> None:
        """`merge_sbatch_args` normalizes `-t` to `time` before it ever reaches here, so in
        practice `sbatch_args` shouldn't carry both keys at once -- but `apply_chunking` is a
        public function, so its own tie-break (documented in its docstring: `time` before `t`)
        stays covered directly, independent of that upstream normalization."""
        n_chunks, result = apply_chunking(
            {"time": "12:00:00", "t": "6:00:00"}, job_script=None, chunking=3, env_vars={}
        )
        assert n_chunks == 2
        assert result["time"] == "03:00:00"
        n_chunks, result = apply_chunking(
            {"t": "6:00:00", "time": "12:00:00"}, job_script=None, chunking=3, env_vars={}
        )
        assert n_chunks == 4
        assert result["time"] == "03:00:00"

    def test_uses_time_from_job_script_header(self, tmp_path: Path) -> None:
        job_script = tmp_path / "my_script"
        job_script.write_text("#SBATCH --time=12:00:00")

        n_chunks, result = apply_chunking(
            {"abc": "123"}, job_script=job_script, chunking=3, env_vars={}
        )
        assert n_chunks == 4
        assert result["time"] == "03:00:00"

    def test_uses_time_from_env_vars(self) -> None:
        n_chunks, result = apply_chunking(
            {"abc": "123"},
            job_script=None,
            chunking=3,
            env_vars={"SBATCH_TIMELIMIT": "12:00:00"},
        )
        assert n_chunks == 4
        assert result["time"] == "03:00:00"

    @pytest.mark.parametrize(
        "header",
        [
            "#SBATCH -t=12:00:00",
            "#SBATCH --time=12:00:00 # with a trailing comment",
            "#SBATCH -n 2\n#SBATCH --time=12:00:00",
            "#!/bin/bash\n#SBATCH --time=12:00:00",
        ],
    )
    def test_various_job_script_header_formats(self, tmp_path: Path, header: str) -> None:
        job_script = tmp_path / "my_script"
        job_script.write_text(header)

        n_chunks, result = apply_chunking({}, job_script=job_script, chunking=3, env_vars={})
        assert n_chunks == 4
        assert result["time"] == "03:00:00"

    def test_env_vars_take_precedence_over_job_script_header(self, tmp_path: Path) -> None:
        job_script = tmp_path / "my_script"
        job_script.write_text("#SBATCH --time=99:00:00")

        n_chunks, _ = apply_chunking(
            {},
            job_script=job_script,
            chunking=3,
            env_vars={"SBATCH_TIMELIMIT": "12:00:00"},
        )
        assert n_chunks == 4

    def test_empty_env_var_falls_back_to_job_script_header(self, tmp_path: Path) -> None:
        job_script = tmp_path / "my_script"
        job_script.write_text("#SBATCH --time=12:00:00")

        n_chunks, _ = apply_chunking(
            {},
            job_script=job_script,
            chunking=3,
            env_vars={"SBATCH_TIMELIMIT": ""},
        )
        assert n_chunks == 4

    def test_sbatch_args_take_precedence_over_env_vars_and_header(self, tmp_path: Path) -> None:
        job_script = tmp_path / "my_script"
        job_script.write_text("#SBATCH --time=99:00:00")

        n_chunks, _ = apply_chunking(
            {"time": "12:00:00"},
            job_script=job_script,
            chunking=3,
            env_vars={"SBATCH_TIMELIMIT": "50:00:00"},
        )
        assert n_chunks == 4

    def test_raises_when_no_time_value_found(self) -> None:
        with pytest.raises(ValueError, match="Could not find a time value"):
            apply_chunking({}, job_script=None, chunking=3, env_vars={})

    def test_uses_custom_chunk_size(self) -> None:
        n_chunks, result = apply_chunking(
            {"time": "12:00:00"}, job_script=None, chunking=6, env_vars={}
        )
        assert n_chunks == 2
        assert result["time"] == "06:00:00"
        assert result["array"] == "0-1%1"

    @pytest.mark.parametrize(
        ("time_value", "expected_array"),
        [
            ("20:30:00", "0-6%1"),  # 20.5h / 3h -> 7 chunks
            ("02:00:00", "0-0%1"),  # under one chunk -> still 1 chunk
            ("00:00:00", "0-0%1"),  # zero -> still at least 1 chunk
        ],
    )
    def test_number_of_chunks_rounds_up(self, time_value: str, expected_array: str) -> None:
        _, result = apply_chunking({"time": time_value}, job_script=None, chunking=3, env_vars={})
        assert result["array"] == expected_array
        assert result["time"] == "03:00:00"
