"""SSH-free runtime module for `cluv sweep` job scripts.

Imported directly by users' training scripts, so it must never import `cluv.remote`,
`cluv.cli.*`, or `milatools` (only stdlib + `cluv.config`/`cluv.utils`).

See `design/cluv-sweep.md` for the full design. This module computes and stashes the raw
ingredients (sweep name, slug, resolved combo) for the current task; `cluv.job.current_run_info()`
reads them via `_current_sweep_context()` to build a `RunInfo` for sweep tasks the same way it
already does for plain `cluv submit` jobs.

Stub: every function below raises `NotImplementedError`. Implemented one at a time via TDD,
following the test order in `tests/test_sweep.py`.
"""

from __future__ import annotations

import hashlib
import itertools
import os
import re
import sys

CLUV_SWEEP_NAME_ENV_VAR = "CLUV_SWEEP_NAME"
CLUV_SWEEP_TASK_OFFSET_ENV_VAR = "CLUV_SWEEP_TASK_OFFSET"
"""Set per-job by `cluv sweep` (`job_index * job_capacity`); defaults to "0" when absent."""

_current_sweep_state: tuple[str, str, list[str]] | None = None
"""Stashed (sweep_name, slug, resolved_combo) for this task, set by `patch_argv()`."""


def expand_sweep_args(args: list[str]) -> list[list[str]]:
    """Expands any `--key=v1,v2,...` token (>=2 comma-separated values) into one combo per
    value; every other token is fixed and copied unchanged into every combo. Cartesian
    product over all swept flags, in the order they appear. Returns `[list(args)]` if
    nothing is swept.
    """
    swept: list[tuple[int, str, list[str]]] = []
    for i, arg in enumerate(args):
        if arg.startswith("--") and "=" in arg:
            key, _, value = arg.partition("=")
            values = value.split(",")
            if len(values) >= 2:
                swept.append((i, key, values))
    if not swept:
        return [list(args)]

    combos = []
    for combo_values in itertools.product(*(values for _, _, values in swept)):
        combo = list(args)
        for (i, key, _), value in zip(swept, combo_values, strict=True):
            combo[i] = f"{key}={value}"
        combos.append(combo)
    return combos


def patch_argv() -> None:
    """Patches `sys.argv` in place to this task's resolved combo.

    No-op if `$CLUV_SWEEP_NAME` or `$SLURM_PROCID` is unset (always safe to call, including
    when running locally or under a plain `cluv submit`). Otherwise computes this task's
    global combo index as `$CLUV_SWEEP_TASK_OFFSET` (default "0") + `$SLURM_PROCID`; if that
    index is out of range for the expanded combos, idle-exits (prints a message and
    `sys.exit(0)`) rather than raising.
    """
    global _current_sweep_state
    _current_sweep_state = None

    sweep_name = os.environ.get(CLUV_SWEEP_NAME_ENV_VAR)
    procid = os.environ.get("SLURM_PROCID")
    if not sweep_name or procid is None:
        return

    combos = expand_sweep_args(sys.argv[1:])
    offset = int(os.environ.get(CLUV_SWEEP_TASK_OFFSET_ENV_VAR, "0"))
    global_index = offset + int(procid)
    if global_index >= len(combos):
        print(
            f"[cluv sweep {sweep_name!r}] task index {global_index} has no combo to run "
            f"({len(combos)} combo(s) total) - exiting idle."
        )
        sys.exit(0)

    combo = combos[global_index]
    sys.argv[1:] = combo
    _current_sweep_state = (sweep_name, _slugify_combo(combo), combo)


_SLUG_UNSAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_SLUG_MAX_LEN = 80
_SLUG_HASH_LEN = 8


def _slugify_combo(combo: list[str]) -> str:
    """Filesystem-safe, readable slug for a resolved combo: sanitized `--key=value` tokens
    joined by `-` (order preserved), truncated to a sane length, with a short content-hash
    suffix so truncation can't collide two different combos onto the same slug.
    """
    raw = "-".join(combo)
    safe = _SLUG_UNSAFE_CHARS_RE.sub("-", raw).strip("-") or "combo"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:_SLUG_HASH_LEN]
    truncated = safe[: _SLUG_MAX_LEN - _SLUG_HASH_LEN - 1]
    return f"{truncated}-{digest}"


def _current_sweep_context() -> tuple[str, str, list[str]] | None:
    """Returns `(sweep_name, slug, resolved_combo)` for the current task if `patch_argv()`
    has run in this process and resolved a combo (i.e. it wasn't a no-op and didn't
    idle-exit); `None` otherwise. Private — used only by `cluv.job.current_run_info()`.
    """
    return _current_sweep_state
