# `cluv sh` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `cluv sh <command...>` subcommand that runs a raw shell command across every currently-connected cluster via `clush` (from `clustershell`, fetched on demand with `uvx`), plus locally when invoked from a cluster, with live colored output streamed straight to the terminal.

**Architecture:** A new `cluv/cli/sh.py` module exposes an async `sh(command: list[str]) -> None`. It reuses the existing `get_active_remotes()` helper (from `cluv.cli.sync`) to find connected, non-disabled clusters, and `current_cluster()` (from `cluv.utils`) to detect whether it's running on a cluster itself. If on a cluster, it runs `command` locally first (no SSH). Then, if there are any other connected clusters, it writes their hostnames to a temp file and invokes `uvx --from=clustershell clush --hostfile <tmpfile> <command...>`. Both subprocess calls inherit stdio directly (no capturing) so colors and live streaming work exactly as they would from a real terminal. `__main__.py` gets a new `add_sh_args()` parser wired up the same way as the existing `run` subcommand.

**Tech Stack:** Python 3.11+, `asyncio`, `argparse`/`simple_parsing`, `pytest` + `pytest-asyncio` (`asyncio_mode = "auto"`, no `@pytest.mark.asyncio` needed), `unittest.mock`.

## Global Constraints

- Do not add `clustershell` to `pyproject.toml` dependencies — it's invoked on demand via `uvx --from=clustershell clush`, matching how `sbatch`/`git`/`rsync`/`ssh` are treated as external tools.
- `cluv sh` takes **no** cluster-selection argument — it always targets every currently-connected cluster (plus the current cluster, if any). Only the command itself (`nargs=argparse.REMAINDER`) is a positional argument.
- Reuse `cluv.cli.sync.get_active_remotes()` for connected-cluster resolution. Do not reimplement the connected/disabled/current-cluster filtering logic.
- Both the local run and the `clush` subprocess must inherit stdout/stderr from the parent process (do not pass `stdout=`/`stderr=` to `asyncio.create_subprocess_exec` — the default `None` already means "inherit"), so colors and live streaming work.
- When both a local run and a `clush` run apply, they run **sequentially** — local first, then `clush` — never concurrently, to avoid interleaving their live output.
- If either the local run or `clush` exits non-zero, call `sys.exit(returncode)` directly from `sh()`, using the first non-zero code encountered in run order (local's code takes precedence if both fail). Do not raise `subprocess.CalledProcessError` (the shared handler in `__main__.py` would print misleading "No standard output/error" messages since output was never captured).
- If there are no connected clusters *and* `current_cluster()` is `None` (nothing to run anything on), print a message pointing at `cluv login` and return — do not raise (unlike `sync`/`clean`, there's no cluster-selection arg here to make a raised error actionable, and `__main__.py` has no handler for plain `RuntimeError`, which would otherwise dump an unhandled traceback).

---

### Task 1: `sh()` core — connected-cluster resolution, local-run detection, hostfile, and dispatch

**Files:**
- Create: `cluv/cli/sh.py`
- Test: `tests/test_sh.py`

**Interfaces:**
- Consumes: `cluv.cli.sync.get_active_remotes() -> list[Remote]` (existing), `cluv.remote.Remote` (has `.hostname: str` field, existing), `cluv.utils.console` (existing `rich.console.Console` instance), `cluv.utils.current_cluster() -> str | None` (existing).
- Produces: `async def sh(command: list[str]) -> None` (consumed by Task 3's CLI wiring). Also produces two stubs that Task 1's tests mock out and Task 2 implements for real:
  - `async def _invoke_clush(hostfile: Path, command: list[str]) -> int`
  - `async def _run_locally(command: list[str]) -> int`

- [ ] **Step 1: Write the failing tests for `sh()`**

Create `tests/test_sh.py`:

```python
"""Tests for `cluv sh`."""

from pathlib import Path
from unittest import mock

import pytest

import cluv.cli.sh as sh_module
from cluv.cli.sh import sh
from cluv.remote import Remote


@pytest.fixture(autouse=True)
def no_disabled_clusters(monkeypatch: pytest.MonkeyPatch):
    """By default, pretend no clusters are disabled, so tests don't depend on the real cache."""
    monkeypatch.setattr(sh_module, "get_disabled_clusters", lambda: {})


@pytest.fixture(autouse=True)
def not_on_a_cluster(monkeypatch: pytest.MonkeyPatch):
    """By default, pretend we're not running from a cluster (e.g. a dev laptop)."""
    monkeypatch.setattr(sh_module, "current_cluster", lambda: None)


async def test_sh_prints_message_and_returns_when_nothing_to_run_on(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=[]))
    invoke_clush = mock.AsyncMock()
    run_locally = mock.AsyncMock()
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush)
    monkeypatch.setattr(sh_module, "_run_locally", run_locally)

    await sh(["echo", "hi"])

    invoke_clush.assert_not_called()
    run_locally.assert_not_called()
    assert "cluv login" in capsys.readouterr().out


async def test_sh_writes_hostfile_with_connected_hostnames_and_invokes_clush(
    monkeypatch: pytest.MonkeyPatch,
):
    remotes = [Remote(hostname="mila"), Remote(hostname="narval")]
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=remotes))

    captured: dict = {}

    async def fake_invoke_clush(hostfile: Path, command: list[str]) -> int:
        captured["hostfile_contents"] = Path(hostfile).read_text()
        captured["command"] = command
        return 0

    monkeypatch.setattr(sh_module, "_invoke_clush", fake_invoke_clush)

    await sh(["squeue", "--me"])

    assert captured["hostfile_contents"].splitlines() == ["mila", "narval"]
    assert captured["command"] == ["squeue", "--me"]


async def test_sh_exits_with_clush_return_code_on_failure(monkeypatch: pytest.MonkeyPatch):
    remotes = [Remote(hostname="mila")]
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=remotes))
    monkeypatch.setattr(sh_module, "_invoke_clush", mock.AsyncMock(return_value=17))

    with pytest.raises(SystemExit) as exc_info:
        await sh(["false"])

    assert exc_info.value.code == 17


async def test_sh_runs_locally_when_on_a_cluster_even_with_no_other_connections(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=[]))
    invoke_clush = mock.AsyncMock()
    run_locally = mock.AsyncMock(return_value=0)
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush)
    monkeypatch.setattr(sh_module, "_run_locally", run_locally)

    await sh(["squeue", "--me"])

    run_locally.assert_called_once_with(["squeue", "--me"])
    invoke_clush.assert_not_called()  # nothing to hostfile — no other clusters connected.


async def test_sh_runs_locally_then_clush_in_order_when_both_apply(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(
        sh_module, "get_active_remotes", mock.AsyncMock(return_value=[Remote(hostname="narval")])
    )
    call_order: list[str] = []

    async def fake_run_locally(command: list[str]) -> int:
        call_order.append("local")
        return 0

    async def fake_invoke_clush(hostfile: Path, command: list[str]) -> int:
        call_order.append("clush")
        return 0

    monkeypatch.setattr(sh_module, "_run_locally", fake_run_locally)
    monkeypatch.setattr(sh_module, "_invoke_clush", fake_invoke_clush)

    await sh(["squeue", "--me"])

    assert call_order == ["local", "clush"]


async def test_sh_exits_with_local_return_code_when_local_fails_before_clush_runs(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(
        sh_module, "get_active_remotes", mock.AsyncMock(return_value=[Remote(hostname="narval")])
    )
    monkeypatch.setattr(sh_module, "_run_locally", mock.AsyncMock(return_value=3))
    invoke_clush = mock.AsyncMock(return_value=0)
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush)

    with pytest.raises(SystemExit) as exc_info:
        await sh(["false"])

    invoke_clush.assert_called_once()  # clush still runs even though the local run failed.
    assert exc_info.value.code == 3  # but the local (first) failure's code wins.
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sh.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cluv.cli.sh'` (the file doesn't exist yet).

- [ ] **Step 3: Write the implementation**

Create `cluv/cli/sh.py`:

```python
"""`cluv sh`: run a raw shell command across every currently-connected cluster via clush."""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

from cluv.cache import get_disabled_clusters
from cluv.cli.disable import print_disabled_clusters
from cluv.cli.sync import get_active_remotes
from cluv.utils import console, current_cluster

__all__ = ["sh"]

logger = logging.getLogger(__name__)


async def sh(command: list[str]) -> None:
    """Runs `command` via `clush` on every cluster we currently have an active SSH connection to.

    If invoked from a cluster (i.e. `current_cluster()` is set), also runs `command` locally
    first, since `get_active_remotes()` never includes the current cluster.

    Does not sync the project or wrap the command in `uv run` (see `cluv run` for that). Does not
    attempt to connect to clusters we aren't already connected to, so this never triggers a 2FA
    prompt. If either run exits with a non-zero return code, this exits the process with the first
    non-zero code encountered (the local run's code takes precedence if both fail).

    Parameters:
        command: The command (and its arguments) to run.
    """
    disabled = get_disabled_clusters()
    print_disabled_clusters(disabled)

    remotes = await get_active_remotes()
    here = current_cluster()

    if not remotes and not here:
        console.print(
            "[yellow]Not currently connected to any cluster.[/yellow] "
            "Use `cluv login` to connect first."
        )
        return

    returncode = 0

    if here:
        returncode = await _run_locally(command)

    if remotes:
        hostnames = [remote.hostname for remote in remotes]
        with tempfile.NamedTemporaryFile("w", suffix=".txt") as hostfile:
            hostfile.write("\n".join(hostnames) + "\n")
            hostfile.flush()
            clush_returncode = await _invoke_clush(Path(hostfile.name), command)
        returncode = returncode or clush_returncode

    if returncode != 0:
        sys.exit(returncode)


async def _invoke_clush(hostfile: Path, command: list[str]) -> int:
    raise NotImplementedError  # implemented in Task 2


async def _run_locally(command: list[str]) -> int:
    raise NotImplementedError  # implemented in Task 2
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sh.py -v`
Expected: PASS (all six tests pass — Task 1's tests all monkeypatch `_invoke_clush`/`_run_locally`, so their `NotImplementedError` bodies are never reached).

- [ ] **Step 5: Commit**

```bash
git add cluv/cli/sh.py tests/test_sh.py
git commit -m "feat: add cluv sh core logic (connected-cluster resolution + local run + hostfile)"
```

---

### Task 2: `_invoke_clush` and `_run_locally` — real subprocess execution with inherited stdio

**Files:**
- Modify: `cluv/cli/sh.py`
- Test: `tests/test_sh.py`

**Interfaces:**
- Consumes: nothing new from other tasks.
- Produces: the real bodies of `_invoke_clush(hostfile: Path, command: list[str]) -> int` and `_run_locally(command: list[str]) -> int`, replacing Task 1's `NotImplementedError` stubs. Both are already wired up by `sh()` from Task 1.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sh.py`:

```python
def _fake_proc(returncode: int) -> mock.Mock:
    proc = mock.Mock()
    proc.wait = mock.AsyncMock(return_value=returncode)
    return proc


async def test_invoke_clush_builds_expected_argv_and_returns_exit_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    create_subprocess_exec = mock.AsyncMock(return_value=_fake_proc(0))
    monkeypatch.setattr(sh_module.asyncio, "create_subprocess_exec", create_subprocess_exec)

    hostfile = tmp_path / "hosts.txt"
    hostfile.write_text("mila\nnarval\n")

    returncode = await sh_module._invoke_clush(hostfile, ["squeue", "--me"])

    assert returncode == 0
    create_subprocess_exec.assert_called_once_with(
        "uvx", "--from=clustershell", "clush", "--hostfile", str(hostfile), "squeue", "--me"
    )


async def test_invoke_clush_returns_nonzero_exit_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(
        sh_module.asyncio, "create_subprocess_exec", mock.AsyncMock(return_value=_fake_proc(42))
    )

    hostfile = tmp_path / "hosts.txt"
    hostfile.write_text("mila\n")

    returncode = await sh_module._invoke_clush(hostfile, ["false"])

    assert returncode == 42


async def test_run_locally_builds_expected_argv_and_returns_exit_code(
    monkeypatch: pytest.MonkeyPatch,
):
    create_subprocess_exec = mock.AsyncMock(return_value=_fake_proc(0))
    monkeypatch.setattr(sh_module.asyncio, "create_subprocess_exec", create_subprocess_exec)

    returncode = await sh_module._run_locally(["squeue", "--me"])

    assert returncode == 0
    create_subprocess_exec.assert_called_once_with("squeue", "--me")


async def test_run_locally_returns_nonzero_exit_code(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        sh_module.asyncio, "create_subprocess_exec", mock.AsyncMock(return_value=_fake_proc(7))
    )

    returncode = await sh_module._run_locally(["false"])

    assert returncode == 7
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sh.py -v -k "test_invoke_clush or test_run_locally"`
Expected: FAIL — `NotImplementedError` raised by the Task 1 stubs.

- [ ] **Step 3: Write the implementation**

In `cluv/cli/sh.py`, add `import asyncio` and `import shlex` to the imports, and replace both stubs:

```python
async def _invoke_clush(hostfile: Path, command: list[str]) -> int:
    """Runs `clush` (via `uvx --from=clustershell`) against `hostfile`, running `command` on each host.

    Inherits stdout/stderr from the current process (instead of capturing them) so that `clush`'s
    own TTY detection and per-node colored output pass straight through to the user's terminal.
    """
    argv = ["uvx", "--from=clustershell", "clush", "--hostfile", str(hostfile), *command]
    console.log(f"$ {shlex.join(argv)}", style="green")
    proc = await asyncio.create_subprocess_exec(*argv)
    return await proc.wait()


async def _run_locally(command: list[str]) -> int:
    """Runs `command` directly as a local subprocess (no SSH), inheriting stdout/stderr."""
    console.log(f"({current_cluster()}) $ {shlex.join(command)}", style="green")
    proc = await asyncio.create_subprocess_exec(*command)
    return await proc.wait()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sh.py -v`
Expected: PASS (all ten tests in the file pass).

- [ ] **Step 5: Commit**

```bash
git add cluv/cli/sh.py tests/test_sh.py
git commit -m "feat: invoke clush and local commands via subprocess with inherited stdio"
```

---

### Task 3: Wire up the `cluv sh` CLI subcommand

**Files:**
- Modify: `cluv/__main__.py`
- Test: `tests/test_sh.py`

**Interfaces:**
- Consumes: `cluv.cli.sh.sh(command: list[str]) -> None` (from Tasks 1–2).
- Produces: `cluv sh <command...>` as a working CLI entry point. Nothing later depends on new names here.

- [ ] **Step 1: Write the failing CLI-parsing tests**

Append to `tests/test_sh.py`:

```python
import cluv.__main__ as cluv_main


def test_sh_cli_parses_command_args(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cluv_main, cluv_main.sh.__name__, mock_sh := mock.AsyncMock(spec=cluv_main.sh)
    )

    cluv_main.main(["sh", "squeue", "--me"])

    mock_sh.assert_called_once_with(command=["squeue", "--me"])


def test_sh_cli_parses_no_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cluv_main, cluv_main.sh.__name__, mock_sh := mock.AsyncMock(spec=cluv_main.sh)
    )

    cluv_main.main(["sh"])

    mock_sh.assert_called_once_with(command=[])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sh.py -v -k test_sh_cli`
Expected: FAIL — `AttributeError: module 'cluv.__main__' has no attribute 'sh'`.

- [ ] **Step 3: Write the implementation**

In `cluv/__main__.py`:

1. Add the import, alphabetically between the `run` and `status` imports:

```python
from .cli.run import run
from .cli.sh import sh
from .cli.status import status
```

2. In `main()`, register the subparser right after the `run` subparser is registered:

```python
    run_parser = add_run_args(subparsers)
    _add_v_arg(run_parser)

    sh_parser = add_sh_args(subparsers)
    _add_v_arg(sh_parser)

    login_parser = add_login_args(subparsers)
```

3. Add the parser-builder function, placed after `add_run_args`:

```python
def add_sh_args(subparsers: Subparsers):
    sh_parser = subparsers.add_parser(
        "sh",
        help="Run a raw command on every currently-connected cluster (and locally, if applicable) via clush.",
        formatter_class=rich_argparse.RichHelpFormatter,
        usage="cluv sh <command...>",
    )
    sh_parser.add_argument(
        "command",
        type=str,
        metavar="<command>",
        help="The command to run on every currently-connected cluster.",
        nargs=argparse.REMAINDER,
    )
    sh_parser.set_defaults(func=sh)
    return sh_parser
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sh.py -v`
Expected: PASS (all twelve tests in the file pass).

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest -v`
Expected: PASS (no regressions in the rest of the suite; pre-existing `integration`/`slow`-marked tests that need real cluster connections may skip/xfail as usual on a dev machine — that's expected, not a regression).

- [ ] **Step 6: Manual smoke test**

Run: `uv run cluv sh --help` and confirm the help text renders correctly.
Run: `uv run cluv sh echo hello` (with no active cluster connections, and not on a cluster) and confirm it prints the "Not currently connected to any cluster" message and exits 0 without attempting to invoke `uvx`/`clush`.
If you have an active connection to at least one cluster (check with `cluv login <cluster>` first), run `uv run cluv sh squeue --me` and confirm real, colored `clush` output streams to the terminal live.

- [ ] **Step 7: Commit**

```bash
git add cluv/__main__.py tests/test_sh.py
git commit -m "feat: wire up cluv sh subcommand in the CLI"
```

---

### Task 4: Open a draft PR

**Files:** none (git/GitHub operations only).

- [ ] **Step 1: Push the branch**

```bash
git push -u origin cluv-sh-command
```

- [ ] **Step 2: Open the draft PR**

```bash
gh pr create --draft --title "Add cluv sh: run a command on every connected cluster via clush" --body "$(cat <<'EOF'
## Summary
- Adds `cluv sh <command...>`, which runs a raw shell command across every currently-connected cluster via `clush` (clustershell, fetched on demand with `uvx --from=clustershell`), with live colored output.
- If invoked from a cluster, also runs the command locally first (since the current cluster is never part of the SSH-connected set), then runs `clush` for whichever other clusters are connected.
- Unlike `cluv run`, this does not sync the project or wrap the command in `uv run`, and it always targets every connected cluster (no cluster-selection argument).
- Reuses the existing `get_active_remotes()` helper for connected-cluster resolution; never triggers a 2FA prompt.

See `docs/superpowers/specs/2026-07-31-cluv-sh-command-design.md` and `docs/superpowers/plans/2026-07-31-cluv-sh-command.md` for the full design and implementation plan.

## Test plan
- [x] `uv run pytest tests/test_sh.py -v`
- [x] `uv run pytest -v` (full suite, no regressions)
- [x] Manual: `cluv sh --help`
- [x] Manual: `cluv sh echo hello` with no active connections (prints the "not connected" message, exits cleanly)
- [ ] Manual: `cluv sh squeue --me` against a real connected cluster
- [ ] Manual: `cluv sh squeue --me` run from on a cluster itself, with another cluster connected

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report the PR URL to the user**
