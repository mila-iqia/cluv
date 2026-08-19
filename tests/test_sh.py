"""Tests for `cluv sh`."""

from unittest import mock

import pytest

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
