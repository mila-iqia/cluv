from cluv.sbatch_args import sbatch_args_to_list


class TestSbatchArgsFromDict:
    def test_long_key_string_value(self) -> None:
        assert sbatch_args_to_list({"time": "2:00:00"}) == ["--time=2:00:00"]

    def test_short_key_string_value(self) -> None:
        assert sbatch_args_to_list({"N": "2"}) == ["-N", "2"]

    def test_true_long_key_is_bare_flag(self) -> None:
        assert sbatch_args_to_list({"exclusive": True}) == ["--exclusive"]

    def test_true_short_key_is_bare_flag(self) -> None:
        assert sbatch_args_to_list({"n": True}) == ["-n"]

    def test_empty_string_omitted(self) -> None:
        assert sbatch_args_to_list({"gpus": ""}) == []

    def test_false_omitted(self) -> None:
        assert sbatch_args_to_list({"requeue": False}) == []

    def test_multiple_flags_in_order(self) -> None:
        result = sbatch_args_to_list({"time": "2:00:00", "gpus": "1", "exclusive": True})
        assert result == ["--time=2:00:00", "--gpus=1", "--exclusive"]
