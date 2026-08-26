"""Tests for `cluv.sweep` — the SSH-free runtime module used by `cluv sweep` job scripts.

Pure, no I/O. See `design/cluv-sweep.md` for the design these tests are derived from.
"""

import sys

import pytest

from cluv.sweep import (
    CLUV_SWEEP_NAME_ENV_VAR,
    CLUV_SWEEP_TASK_OFFSET_ENV_VAR,
    _current_sweep_context,
    _slugify_combo,
    expand_sweep_args,
    patch_argv,
)

# ---------------------------------------------------------------------------
# expand_sweep_args
# ---------------------------------------------------------------------------


def test_expand_sweep_args_passthrough_when_nothing_swept():
    args = ["--foo=1", "--bar=baz", "positional"]
    assert expand_sweep_args(args) == [args]


def test_expand_sweep_args_empty_list_returns_single_empty_combo():
    assert expand_sweep_args([]) == [[]]


def test_expand_sweep_args_single_swept_flag():
    combos = expand_sweep_args(["--foo=1,2,3"])
    assert combos == [["--foo=1"], ["--foo=2"], ["--foo=3"]]


def test_expand_sweep_args_multiple_swept_flags_cartesian_product_in_appearance_order():
    combos = expand_sweep_args(["--foo=1,2", "--baz=eee,fff"])
    assert combos == [
        ["--foo=1", "--baz=eee"],
        ["--foo=1", "--baz=fff"],
        ["--foo=2", "--baz=eee"],
        ["--foo=2", "--baz=fff"],
    ]


def test_expand_sweep_args_fixed_tokens_copied_unchanged_into_every_combo():
    combos = expand_sweep_args(["python", "main.py", "--foo=1,2", "--lr=0.1"])
    assert combos == [
        ["python", "main.py", "--foo=1", "--lr=0.1"],
        ["python", "main.py", "--foo=2", "--lr=0.1"],
    ]


def test_expand_sweep_args_ignores_non_equals_and_positional_tokens():
    # A bare flag (no '=') or a positional arg is never treated as swept, even if it
    # happens to contain a comma.
    combos = expand_sweep_args(["--verbose", "a,b,c", "--foo=1,2"])
    assert combos == [
        ["--verbose", "a,b,c", "--foo=1"],
        ["--verbose", "a,b,c", "--foo=2"],
    ]


def test_expand_sweep_args_single_value_flag_is_not_treated_as_swept():
    # A `--key=value` with no comma is fixed, same as any other flag.
    assert expand_sweep_args(["--foo=1"]) == [["--foo=1"]]


# ---------------------------------------------------------------------------
# _slugify_combo
# ---------------------------------------------------------------------------


def test_slugify_combo_is_deterministic():
    combo = ["--foo=1", "--bar=baz"]
    assert _slugify_combo(combo) == _slugify_combo(list(combo))


def test_slugify_combo_differs_for_different_combos():
    assert _slugify_combo(["--foo=1"]) != _slugify_combo(["--foo=2"])


def test_slugify_combo_is_filesystem_safe():
    slug = _slugify_combo(["--path=/some/weird path!.txt"])
    assert "/" not in slug
    assert all(c.isalnum() or c in "-_." for c in slug)


def test_slugify_combo_truncates_long_combos_with_stable_hash_suffix():
    combo = [f"--flag_{i}=value_{i}" for i in range(50)]
    slug = _slugify_combo(combo)
    assert len(slug) < len("-".join(combo))
    # Truncation must stay collision-resistant: a combo differing only in a part that
    # would get truncated away must still produce a different slug.
    other = [*combo[:-1], "--flag_49=something_else_entirely"]
    assert _slugify_combo(other) != slug


# ---------------------------------------------------------------------------
# patch_argv
# ---------------------------------------------------------------------------


def test_patch_argv_is_noop_when_sweep_name_env_var_unset(monkeypatch):
    monkeypatch.delenv(CLUV_SWEEP_NAME_ENV_VAR, raising=False)
    monkeypatch.setenv("SLURM_PROCID", "0")
    monkeypatch.setattr(sys, "argv", ["main.py", "--foo=1,2"])
    patch_argv()
    assert sys.argv == ["main.py", "--foo=1,2"]


def test_patch_argv_is_noop_when_slurm_procid_unset(monkeypatch):
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.delenv("SLURM_PROCID", raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", "--foo=1,2"])
    patch_argv()
    assert sys.argv == ["main.py", "--foo=1,2"]


def test_patch_argv_patches_sys_argv_to_resolved_combo(monkeypatch):
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.setenv("SLURM_PROCID", "1")
    monkeypatch.delenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", "--foo=1,2,3"])
    patch_argv()
    assert sys.argv == ["main.py", "--foo=2"]


def test_patch_argv_computes_global_index_from_offset_plus_procid(monkeypatch):
    # SLURM_PROCID=2 + CLUV_SWEEP_TASK_OFFSET=4 -> global index 6.
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.setenv("SLURM_PROCID", "2")
    monkeypatch.setenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "4")
    monkeypatch.setattr(sys, "argv", ["main.py", "--x=" + ",".join(str(i) for i in range(10))])
    patch_argv()
    assert sys.argv == ["main.py", "--x=6"]


def test_patch_argv_idle_exits_when_global_index_out_of_range(monkeypatch):
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.setenv("SLURM_PROCID", "5")
    monkeypatch.setenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "0")
    monkeypatch.setattr(sys, "argv", ["main.py", "--x=1,2,3"])  # only 3 combos, index 5 is out
    with pytest.raises(SystemExit) as exc_info:
        patch_argv()
    assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# _current_sweep_context
# ---------------------------------------------------------------------------


def test_current_sweep_context_returns_none_before_patch_argv_called(monkeypatch):
    monkeypatch.delenv(CLUV_SWEEP_NAME_ENV_VAR, raising=False)
    assert _current_sweep_context() is None


def test_current_sweep_context_returns_none_when_patch_argv_was_noop(monkeypatch):
    monkeypatch.delenv(CLUV_SWEEP_NAME_ENV_VAR, raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", "--foo=1,2"])
    patch_argv()
    assert _current_sweep_context() is None


def test_current_sweep_context_returns_none_after_idle_exit(monkeypatch):
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.setenv("SLURM_PROCID", "5")
    monkeypatch.setenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "0")
    monkeypatch.setattr(sys, "argv", ["main.py", "--x=1,2,3"])
    with pytest.raises(SystemExit):
        patch_argv()
    assert _current_sweep_context() is None


def test_current_sweep_context_returns_sweep_name_slug_and_combo_after_patch(monkeypatch):
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.setenv("SLURM_PROCID", "1")
    monkeypatch.delenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, raising=False)
    monkeypatch.setattr(sys, "argv", ["main.py", "--foo=1,2,3"])
    patch_argv()

    sweep_name, slug, combo = _current_sweep_context()

    assert sweep_name == "my-sweep"
    assert combo == ["--foo=2"]
    assert slug == _slugify_combo(["--foo=2"])


def test_current_sweep_context_same_combo_same_slug_under_different_offset_procid_pairs(
    monkeypatch,
):
    # Simulates a resubmission: the same global combo (index 6) landing on a different
    # job/task the second time around must still resolve to the same slug, since resumability
    # is keyed on the combo's argument values, not on which job/task it happened to land on.
    monkeypatch.setenv(CLUV_SWEEP_NAME_ENV_VAR, "my-sweep")
    monkeypatch.setattr(sys, "argv", ["main.py", "--x=" + ",".join(str(i) for i in range(10))])

    monkeypatch.setenv("SLURM_PROCID", "6")
    monkeypatch.setenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "0")
    patch_argv()
    _, slug_first, combo_first = _current_sweep_context()

    monkeypatch.setattr(sys, "argv", ["main.py", "--x=" + ",".join(str(i) for i in range(10))])
    monkeypatch.setenv("SLURM_PROCID", "2")
    monkeypatch.setenv(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "4")
    patch_argv()
    _, slug_second, combo_second = _current_sweep_context()

    assert combo_first == combo_second
    assert slug_first == slug_second
