"""Tests for `cluv sh`."""

import textwrap
from pathlib import Path, PurePosixPath
from unittest import mock

import pytest

import cluv.__main__ as cluv_main
import cluv.cli.sh as sh_module
from cluv.cli.sh import sh
from cluv.config import find_pyproject, load_cluv_config
from cluv.remote import Remote
from cluv.utils import console


@pytest.fixture(autouse=True)
def no_disabled_clusters(monkeypatch: pytest.MonkeyPatch):
    """By default, pretend no clusters are disabled, so tests don't depend on the real cache."""
    monkeypatch.setattr(sh_module, "get_disabled_clusters", lambda: {})


@pytest.fixture(autouse=True)
def not_on_a_cluster(monkeypatch: pytest.MonkeyPatch):
    """By default, pretend we're not running from a cluster (e.g. a dev laptop)."""
    monkeypatch.setattr(sh_module, "current_cluster", lambda: None)


async def test_sh_prints_message_and_returns_when_nothing_to_run_on(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        sh_module, "get_active_remotes", get_active_remotes := mock.AsyncMock(return_value=[])
    )
    monkeypatch.setattr(sh_module, "_invoke_clush", invoke_clush := mock.AsyncMock())
    monkeypatch.setattr(sh_module, "_run_locally", run_locally := mock.AsyncMock())

    with console.capture() as cap:
        await sh(["echo", "hi"])

    get_active_remotes.assert_awaited_once()
    invoke_clush.assert_not_called()
    run_locally.assert_not_called()
    assert "cluv login" in cap.get()


async def test_sh_writes_hostfile_with_connected_hostnames_and_invokes_clush(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        sh_module,
        "get_active_remotes",
        mock.AsyncMock(return_value=[Remote(hostname="mila"), Remote(hostname="narval")]),
    )

    captured: dict = {}

    async def fake_invoke_clush(
        hostfile: Path, command: list[str], project_dir: PurePosixPath | None
    ) -> int:
        captured["hostfile_contents"] = Path(hostfile).read_text()
        captured["command"] = command
        return 0

    monkeypatch.setattr(
        sh_module, "_invoke_clush", invoke_clush := mock.AsyncMock(side_effect=fake_invoke_clush)
    )

    await sh(["squeue", "--me"])

    invoke_clush.assert_awaited_once()
    assert captured["hostfile_contents"].splitlines() == ["mila", "narval"]
    assert captured["command"] == ["squeue", "--me"]


async def test_sh_groups_remotes_by_project_dir_into_separate_clush_calls(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    """Clusters with different (or no) project dirs are split into separate `clush` invocations."""
    monkeypatch.setattr(
        sh_module,
        "get_active_remotes",
        mock.AsyncMock(
            return_value=[
                Remote(hostname="mila"),
                Remote(hostname="tamia"),
                Remote(hostname="killarney"),
            ]
        ),
    )
    project_dirs = {
        "mila": PurePosixPath("$HOME/proj"),
        "tamia": PurePosixPath("$HOME/proj"),
        "killarney": PurePosixPath("$SCRATCH/proj"),
    }
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        textwrap.dedent(f"""\
        [tool.cluv]
        results_path = "logs"

        [tool.cluv.clusters.mila]
        project_dir = "{project_dirs["mila"]}"

        [tool.cluv.clusters.killarney]
        project_dir = "{project_dirs["killarney"]}"

        [tool.cluv.clusters.tamia]
        project_dir = "{project_dirs["tamia"]}"
        """)
    )
    config = load_cluv_config(pyproject)
    monkeypatch.setattr(sh_module, "get_cluv_config", lambda: config)

    calls: list[tuple[list[str], PurePosixPath | None]] = []

    async def fake_invoke_clush(
        hostfile: Path, command: list[str], project_dir: PurePosixPath | None
    ) -> int:
        calls.append((Path(hostfile).read_text().splitlines(), project_dir))
        return 0

    monkeypatch.setattr(
        sh_module, "_invoke_clush", invoke_clush := mock.AsyncMock(side_effect=fake_invoke_clush)
    )

    await sh(["squeue", "--me"])

    assert invoke_clush.await_count == 2  # one per distinct project dir, not one per cluster.
    assert sorted(calls, key=str) == sorted(
        [
            (["mila", "tamia"], PurePosixPath("$HOME/proj")),
            (["killarney"], PurePosixPath("$SCRATCH/proj")),
        ],
        key=str,
    )


async def test_sh_exits_with_clush_return_code_on_failure(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        sh_module, "get_active_remotes", mock.AsyncMock(return_value=[Remote(hostname="mila")])
    )
    monkeypatch.setattr(
        sh_module, "_invoke_clush", invoke_clush := mock.AsyncMock(return_value=17)
    )

    with pytest.raises(SystemExit) as exc_info:
        await sh(["false"])

    invoke_clush.assert_awaited_once()
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

    run_locally.assert_awaited_once_with(["squeue", "--me"])
    invoke_clush.assert_not_called()  # nothing to hostfile — no other clusters connected.


async def test_sh_runs_locally_then_clush_in_order_when_both_apply(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(
        sh_module, "get_active_remotes", mock.AsyncMock(return_value=[Remote(hostname="narval")])
    )
    call_order: list[str] = []
    received_commands: list[list[str]] = []

    async def fake_run_locally(command: list[str]) -> int:
        call_order.append("local")
        received_commands.append(command)
        return 0

    async def fake_invoke_clush(
        hostfile: Path, command: list[str], project_dir: PurePosixPath | None
    ) -> int:
        call_order.append("clush")
        received_commands.append(command)
        return 0

    monkeypatch.setattr(sh_module, "_run_locally", fake_run_locally)
    monkeypatch.setattr(sh_module, "_invoke_clush", fake_invoke_clush)

    await sh(["squeue", "--me"])

    assert call_order == ["local", "clush"]
    assert received_commands == [["squeue", "--me"], ["squeue", "--me"]]


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

    invoke_clush.assert_awaited_once()  # clush still runs even though the local run failed.
    assert exc_info.value.code == 3  # but the local (first) failure's code wins.


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

    returncode = await sh_module._invoke_clush(hostfile, ["squeue", "--me"], None)

    assert returncode == 0
    create_subprocess_exec.assert_called_once_with(
        "uvx", "--from=clustershell", "clush", "-S", "--hostfile", str(hostfile), "squeue --me"
    )


async def test_invoke_clush_cds_into_project_dir_first_when_given(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    create_subprocess_exec = mock.AsyncMock(return_value=_fake_proc(0))
    monkeypatch.setattr(sh_module.asyncio, "create_subprocess_exec", create_subprocess_exec)

    hostfile = tmp_path / "hosts.txt"
    hostfile.write_text("mila\n")

    await sh_module._invoke_clush(hostfile, ["squeue", "--me"], PurePosixPath("$HOME/proj"))

    create_subprocess_exec.assert_called_once_with(
        "uvx",
        "--from=clustershell",
        "clush",
        "-S",
        "--hostfile",
        str(hostfile),
        'cd "$HOME/proj" 2>/dev/null; squeue --me',
    )


async def test_invoke_clush_returns_nonzero_exit_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.setattr(
        sh_module.asyncio, "create_subprocess_exec", mock.AsyncMock(return_value=_fake_proc(42))
    )

    hostfile = tmp_path / "hosts.txt"
    hostfile.write_text("mila\n")

    returncode = await sh_module._invoke_clush(hostfile, ["false"], None)

    assert returncode == 42


def test_with_cd_prefix_returns_plain_command_without_a_project_dir():
    assert sh_module._with_cd_prefix(["echo", "hi there"], None) == "echo 'hi there'"


def test_with_cd_prefix_prepends_a_best_effort_cd_when_given_a_project_dir():
    result = sh_module._with_cd_prefix(["squeue", "--me"], PurePosixPath("$HOME/proj"))
    assert result == 'cd "$HOME/proj" 2>/dev/null; squeue --me'


async def test_sh_pwd_runs_for_real_from_the_local_project_root(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
):
    """An end-to-end (unmocked) run of `cluv sh pwd`'s local-execution path.

    No SSH/`clush` is involved here (that would need a live cluster connection — see
    `tests/test_integration.py`), so this is real enough to exercise the actual `subprocess`
    call, unlike `test_invoke_clush_*`/the old `_run_locally` argv tests: it proves `cluv sh`
    really does `cd` into the project root, rather than just asserting a mock received the right
    `cwd=` kwarg.
    """
    monkeypatch.setattr(sh_module, "current_cluster", lambda: "mila")
    monkeypatch.setattr(sh_module, "get_active_remotes", mock.AsyncMock(return_value=[]))

    await sh(["pwd"])

    printed_lines = [line for line in capfd.readouterr().out.splitlines() if line.strip()]
    assert printed_lines[-1] == str(find_pyproject().parent)


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
