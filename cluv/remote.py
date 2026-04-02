from __future__ import annotations

import asyncio
import dataclasses
import functools
import subprocess
import sys
from logging import getLogger as get_logger
from typing import Callable, Literal, Self, TypeVar

from cluv.utils import console

logger = get_logger(__name__)

Hide = Literal[True, False, "out", "stdout", "err", "stderr"]

C = TypeVar("C", bound=Callable)


@dataclasses.dataclass(frozen=True)
class Remote:
    """Used to run commands over SSH asynchronously in subprocesses while sharing an SSH connection.

    This doesn't work on Windows, as it assumes that the SSH client has SSH multiplexing
    support (ControlMaster, ControlPath and ControlPersist).
    """

    hostname: str

    @classmethod
    async def connect(cls, hostname: str) -> Self:
        """Async 'constructor'.

        Using this once explicitly before running lots of commands in parallel can be useful, since
        otherwise the 2FA prompt might happen for each individual command.

        TODO: cache this function's result using an async-compatible version of functools.cache maybe?
        """
        remote = cls(hostname)
        if not (await control_socket_is_running(hostname)):
            result = await remote.run("echo OK", display=False, hide="out")
            if "OK" not in result.stdout:
                raise RuntimeError(
                    f"Some strange error occurred when connecting to hostname {hostname}: {result.stderr}"
                )
        return remote

    async def run(
        self,
        command: str,
        *,
        input: str | None = None,
        display: bool = True,
        warn: bool = False,
        hide: Hide = False,
    ) -> subprocess.CompletedProcess[str]:
        if sys.platform == "win32":
            raise NotImplementedError(
                "This feature isn't supported on Windows, as it requires an SSH client "
                "with SSH multiplexing support (ControlMaster, ControlPath and "
                "ControlPersist).\n"
                "Please consider switching to the Windows Subsystem for Linux (WSL).\n"
                "See https://learn.microsoft.com/en-us/windows/wsl/install for a guide on "
                "setting up WSL."
            )
        ssh_command = (
            "ssh",
            *get_multiplexing_options_to_use(self.hostname),
            self.hostname,
            command,
        )

        if display:
            console.log(
                (
                    f"({self.hostname}) $ {command}"
                    if input is None
                    else f"({self.hostname}) $ {command=}\n{input=}"
                ),
                style="green",
                _stack_offset=2,  # to show a link to the code calling this, instead of here.
            )
        return await run(ssh_command, input=input, warn=warn, hide=hide, _stacklevel=3)

    async def get_output(
        self,
        command: str,
        *,
        display: bool = False,
        warn: bool = False,
        hide: Hide = True,
    ) -> str:
        """Runs the command asynchronously and returns the stripped output string."""
        return (await self.run(command, display=display, warn=warn, hide=hide)).stdout.strip()


async def run(
    program_and_args: tuple[str, ...],
    input: str | None = None,
    warn: bool = False,
    hide: Hide = False,
    _stacklevel: int = 2,
) -> subprocess.CompletedProcess[str]:
    """Runs the command *asynchronously* in a subprocess and returns the result.

    Parameters
    ----------
    program_and_args: The program and arguments to pass to it. This is a tuple of \
        strings, same as in `subprocess.Popen`.
    input: The optional 'input' argument to `subprocess.Popen.communicate()`.
    warn: When `True` and an exception occurs, warn instead of raising the exception.
    hide: Controls the printing of the subprocess' stdout and stderr.

    Returns
    -------
    A `subprocess.CompletedProcess` object with the result of the asyncio.Process.

    Raises
    ------
    subprocess.CalledProcessError
        If an error occurs when running the command and `warn` is `False`.
    """

    logger.debug(f"Calling `asyncio.create_subprocess_exec` with {program_and_args=}")
    proc = await asyncio.create_subprocess_exec(
        *program_and_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
        shell=False,
    )

    if input:
        logger.debug(f"Sending {input=!r} to the subprocess' stdin.")
    try:
        stdout, stderr = await proc.communicate(input.encode() if input else None)
    except asyncio.CancelledError:
        logger.debug(f"Got interrupted while calling {proc}.communicate({input=}).")
        # This is a fix for ugly error trace on interrupt: https://bugs.python.org/issue43884
        if transport := getattr(proc, "_transport", None):
            transport.close()
        raise

    assert proc.returncode is not None
    if proc.returncode != 0:
        message = (
            f"{program_and_args!r}"
            + (f" with input {input!r}" if input else "")
            + f" exited with {proc.returncode}"
            + (f": {stderr}" if stderr else "")
        )
        logger.debug(message, stacklevel=_stacklevel)
        if not warn:
            if stderr and hide not in [True, "err", "stderr"]:
                logger.error(stderr.decode(), stacklevel=_stacklevel)
            raise subprocess.CalledProcessError(
                returncode=proc.returncode,
                cmd=program_and_args,
                output=stdout,
                stderr=stderr,
            )
        if hide is not True:  # don't warn if hide is True.
            logger.warning(RuntimeWarning(message), stacklevel=_stacklevel)
    result = subprocess.CompletedProcess(
        args=program_and_args,
        returncode=proc.returncode,
        stdout=stdout.decode(),
        stderr=stderr.decode(),
    )
    if result.stdout:
        if hide not in [True, "out", "stdout"]:
            print(result.stdout)
        logger.debug(result.stdout)
    if result.stderr:
        if hide not in [True, "err", "stderr"]:
            print(result.stderr, file=sys.stderr)
        logger.debug(result.stderr)
    return result


async def control_socket_is_running(host: str) -> bool:
    """Asynchronously checks whether the control socket at the given path is running."""
    result = await run(
        ("ssh", *get_multiplexing_options_to_use(host), "-O", "check", host),
        warn=True,
        hide=True,
    )
    if (
        result.returncode != 0
        or not result.stderr
        or not result.stderr.startswith("Master running")
    ):
        logger.debug("ControlMaster isn't running.")
        return False
    return True


def get_ssh_options_for_host(hostname: str) -> dict[str, str]:
    """Returns the dictionary of ssh options for a given host (taken from `ssh -G <hostname>`)."""
    return dict(_get_ssh_options_for_host(hostname))


# note: Could potentially cache the results of this function if we wanted to, assuming
# that the ssh config file doesn't change.
@functools.cache
def _get_ssh_options_for_host(hostname: str) -> tuple[tuple[str, str], ...]:
    output = subprocess.getoutput(f"ssh -G {hostname}")
    results = []
    for line in output.splitlines():
        key, val = line.split(maxsplit=1)
        results.append((key, val))
    return tuple(results)


def get_multiplexing_options_to_use(hostname: str):
    ssh_options = get_ssh_options_for_host(hostname)
    multiplexing_options: list[str] = []
    if ssh_options.get("controlmaster") not in ("yes", "auto"):
        multiplexing_options.append("-oControlMaster=auto")
    if "controlpersist" not in ssh_options:
        multiplexing_options.append("-oControlPersist=yes")
    if "controlpath" not in ssh_options:
        multiplexing_options.append("-oControlPath=~/.cache/ssh/%r@%h:%p")
    return tuple(multiplexing_options)
