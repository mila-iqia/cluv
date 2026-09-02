from cluv.sbatch_args import sbatch_args_from_args_list, sbatch_args_to_list


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


class TestSbatchArgsFromArgsList:
    def test_long_flag_with_equals(self) -> None:
        assert sbatch_args_from_args_list(["--gpus=1"]) == {"gpus": "1"}

    def test_short_alias_expands_to_long_name(self) -> None:
        assert sbatch_args_from_args_list(["-N", "2"]) == {"nodes": "2"}

    def test_short_alias_with_equals_expands_to_long_name(self) -> None:
        assert sbatch_args_from_args_list(["-t=00:00:30"]) == {"time": "00:00:30"}

    def test_unknown_short_flag_kept_as_is(self) -> None:
        assert sbatch_args_from_args_list(["-f", "2"]) == {"f": "2"}

    def test_bare_long_flag_is_true(self) -> None:
        assert sbatch_args_from_args_list(["--exclusive"]) == {"exclusive": True}

    def test_bare_short_flag_is_true(self) -> None:
        assert sbatch_args_from_args_list(["-n"]) == {"n": True}

    def test_bare_unknown_alias_flag_is_true(self) -> None:
        assert sbatch_args_from_args_list(["--gpus"]) == {"gpus": True}

    def test_string_value_that_looks_falsy_stays_a_string(self) -> None:
        assert sbatch_args_from_args_list(["--requeue=False"]) == {"requeue": "False"}

    def test_last_duplicate_known_alias_wins(self) -> None:
        assert sbatch_args_from_args_list(["--time=2:00:00", "-t=00:00:30"]) == {"time": "00:00:30"}

    def test_last_duplicate_unknown_flag_wins(self) -> None:
        assert sbatch_args_from_args_list(["--array=0-3%2", "--array=0-1%1"]) == {"array": "0-1%1"}

    def test_duplicate_unknown_flag_preserves_original_position(self) -> None:
        result = sbatch_args_from_args_list(["--array=0-3%2", "--mem=4G", "--array=0-1%1"])
        assert list(result.items()) == [("mem", "4G"), ("array", "0-1%1")]

    def test_short_and_long_alias_are_distinct_keys(self) -> None:
        # `-a` is not treated as an alias of `--array`; both are kept as unknown flags.
        assert sbatch_args_from_args_list(["--array=0-3%2", "-a=0-1%1"]) == {
            "array": "0-3%2",
            "a": "0-1%1",
        }

    def test_multiple_known_and_unknown_flags_combined(self) -> None:
        result = sbatch_args_from_args_list(["--time=2:00:00", "--gpus=1", "--exclusive"])
        assert result == {"time": "2:00:00", "gpus": "1", "exclusive": True}

    def test_empty_list_returns_empty_dict(self) -> None:
        assert sbatch_args_from_args_list([]) == {}

    def test_two_consecutive_bare_unknown_flags(self) -> None:
        # Neither flag has a following value, so both stay bare (True).
        assert sbatch_args_from_args_list(["-f", "-g"]) == {"f": True, "g": True}

    def test_known_flag_default_is_suppressed_when_absent(self) -> None:
        result = sbatch_args_from_args_list(["--gpus=1"])
        assert "time" not in result
        assert "nodes" not in result
        assert "account" not in result
        assert "cpus-per-task" not in result

    def test_known_alias_account(self) -> None:
        assert sbatch_args_from_args_list(["-A", "my-account"]) == {"account": "my-account"}

    def test_known_alias_cpus_per_task(self) -> None:
        assert sbatch_args_from_args_list(["-c", "4"]) == {"cpus-per-task": "4"}
