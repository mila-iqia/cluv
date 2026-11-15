"""`cluv submit --test-only`: ask Slurm when each job configuration would start, submit nothing.

`sbatch --test-only` validates a job against the cluster (partition, account, QoS, limits) and
prints a forecast to *stderr*, without queueing anything. There is no job id on stdout, which is
why this can't go through `submit_job`. It runs where `cluv submit` would run `sbatch`: in the
project on the cluster, after the current commit is pushed and pulled there (only that part of
`cluv sync`, see `cluv.cli.submit.submit_test_only`).

Slurm prints the start time in the login node's local time, which isn't always the cluster's
geographic one (Fir, in Vancouver, runs on MST). So sbatch runs with `TZ=UTC`, and the table shows
every start time in the timezone of the machine running cluv.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import re
import shlex
import sys
from collections import Counter
from collections.abc import Sequence

import rich.box
import rich.table
import rich.text

from cluv.cache import Submission
from cluv.remote import run
from cluv.sbatch_args import SbatchArgs
from cluv.utils import console

# sbatch: Job 123 to start at 2026-11-03T17:00:00 using 4 processors on nodes cn-a001 in partition main
# Observed variants: Mila prints a stray token after the time ("...T17:06:26 a using 1 ..."), and
# for heterogeneous jobs there is neither "nodes" nor a partition ("using 2 processors on cn-c036").
_FORECAST_RE = re.compile(
    r"Job (?P<job_id>\d+) to start at (?P<start>\S+)"
    r"(?:.*?using (?P<cpus>\d+) processors?)?"
    r"(?: on (?:nodes? )?(?P<nodes>\S+))?"
    r"(?: in partition (?P<partition>\S+))?"
)
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


@dataclasses.dataclass(frozen=True)
class TestOnlyResult:
    """What `sbatch --test-only` said about one job configuration."""

    __test__ = False  # not a pytest test class

    cluster: str
    config: str
    """Short description of what distinguishes this configuration (account, GPU type, ...)."""
    start: datetime.datetime | None = None
    cpus: int | None = None
    nodes: str | None = None
    partition: str | None = None
    resources: Resources | None = None
    """What the job asks for, from the options `sbatch -v` says it parsed."""
    error: str | None = None
    """Slurm's error message if the configuration is rejected (or its output, if unparsable)."""

    @property
    def accepted(self) -> bool:
        return self.error is None


def parse_test_only_output(output: str) -> dict[str, object] | None:
    """Parse the forecast line printed by `sbatch --test-only`, or return None if there is none.

    The start time is read as UTC, since the test-only command runs sbatch with `TZ=UTC`.

    >>> parse_test_only_output(
    ...     "sbatch: Job 1 to start at 2026-11-03T17:00:00 using 4 processors on nodes cn-a001 "
    ...     "in partition main"
    ... )
    {'start': datetime.datetime(2026, 11, 3, 17, 0, tzinfo=datetime.timezone.utc), 'cpus': 4, 'nodes': 'cn-a001', 'partition': 'main'}
    >>> parse_test_only_output("sbatch: error: Batch job submission failed") is None
    True
    """
    for line in output.splitlines():
        if match := _FORECAST_RE.search(line):
            try:
                start = datetime.datetime.fromisoformat(match["start"])
            except ValueError:
                continue
            return {
                "start": start.replace(tzinfo=datetime.timezone.utc),
                "cpus": int(match["cpus"]) if match["cpus"] else None,
                "nodes": match["nodes"],
                "partition": match["partition"],
            }
    return None


@dataclasses.dataclass(frozen=True)
class Resources:
    """What a job configuration asks for, summed over the components of a heterogeneous job."""

    cpus: int
    gpus: dict[str, int]
    """GPU count per type ("" when the type isn't named)."""
    mem_gb: float | None
    """None if some component doesn't ask for memory (the cluster's default then applies)."""

    def __str__(self) -> str:
        gpus = ", ".join(f"{n}x {t or 'gpu'}" for t, n in sorted(self.gpus.items()))
        mem = f"{self.mem_gb:g}GB RAM" if self.mem_gb is not None else "default RAM"
        return ", ".join(x for x in (f"{self.cpus} cpus", gpus, mem) if x)


_OPTION_RE = re.compile(r"^sbatch: (?P<key>[a-z][a-z-]*)\s*: (?P<value>.*)$")


def _plain_sbatch_line(line: str) -> str:
    """The line without colours, with sbatch's prefix in lower case (Trillium's wrapper prints
    `SBATCH:`)."""
    return re.sub(r"^SBATCH:", "sbatch:", _ANSI_RE.sub("", line).rstrip())


def parse_defined_options(output: str) -> list[dict[str, str]]:
    """The options `sbatch -v` prints for each component of the job, in order.

    >>> parse_defined_options(
    ...     "sbatch: defined options\\n"
    ...     "sbatch: -------------------- --------------------\\n"
    ...     "sbatch: cpus-per-task       : 8\\n"
    ...     "sbatch: gpus-per-task       : l40s:1\\n"
    ...     "sbatch: end of defined options\\n"
    ... )
    [{'cpus-per-task': '8', 'gpus-per-task': 'l40s:1'}]
    """
    components: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in map(_plain_sbatch_line, output.splitlines()):
        if line == "sbatch: defined options":
            current = {}
        elif line == "sbatch: end of defined options" and current is not None:
            components.append(current)
            current = None
        elif current is not None and (match := _OPTION_RE.match(line)):
            current[match["key"]] = match["value"].strip()
    return components


def parse_mem_gb(value: str) -> float:
    """Slurm memory sizes: a K/M/G/T suffix, megabytes without one.

    >>> parse_mem_gb("48G"), parse_mem_gb("2048"), parse_mem_gb("512M"), parse_mem_gb("1T")
    (48.0, 2.0, 0.5, 1024.0)
    """
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMGT]?)B?", value.strip().upper())
    if match is None:
        raise ValueError(f"Unrecognized memory size: {value!r}")
    number, unit = float(match[1]), match[2] or "M"
    return number * {"K": 1 / 1024**2, "M": 1 / 1024, "G": 1, "T": 1024}[unit]


def parse_gpu_spec(value: str, gres: bool = False) -> Counter[str]:
    """GPUs in a `--gpus*` value ("l40s:1", "2"), or in a `--gres` one if `gres` ("gpu:h100:4",
    where only the `gpu:` items count).

    >>> parse_gpu_spec("l40s:2"), parse_gpu_spec("3")
    (Counter({'l40s': 2}), Counter({'': 3}))
    >>> parse_gpu_spec("gpu:h100:4,tmpfs:1", gres=True), parse_gpu_spec("gpu:1", gres=True)
    (Counter({'h100': 4}), Counter({'': 1}))
    """
    gpus: Counter[str] = Counter()
    for item in value.split(","):
        parts = item.strip().split(":")
        if gres:
            if parts[0] != "gpu":
                continue
            parts = parts[1:]
        count = int(parts.pop()) if parts and parts[-1].isdigit() else 1
        gpus[parts[0] if parts else ""] += count
    return gpus


def component_resources(options: dict[str, str]) -> Resources:
    """What one job component asks for, following Slurm's defaults (1 task, 1 cpu per task).

    >>> str(component_resources({"ntasks": "2", "cpus-per-task": "8",
    ...                          "gpus-per-task": "l40s:1", "mem-per-gpu": "48G"}))
    '16 cpus, 2x l40s, 96GB RAM'
    >>> str(component_resources({"nodes": "2", "gres": "gpu:h100:4", "mem": "0"}))
    '2 cpus, 8x h100, 0GB RAM'
    """
    nodes = int(options.get("nodes", "1").split("-")[0])
    ntasks = int(
        options.get("ntasks")
        or (int(options["ntasks-per-node"]) * nodes if "ntasks-per-node" in options else nodes)
    )
    cpus = ntasks * int(options.get("cpus-per-task", "1"))
    gpus: Counter[str] = Counter()
    if "gpus" in options:
        gpus += parse_gpu_spec(options["gpus"])
    for key, multiplier in (("gpus-per-task", ntasks), ("gpus-per-node", nodes), ("gres", nodes)):
        if key in options:
            spec = parse_gpu_spec(options[key], gres=key == "gres")
            gpus += Counter({t: n * multiplier for t, n in spec.items()})
    mem_gb = None
    if "mem" in options:
        mem_gb = parse_mem_gb(options["mem"]) * nodes
    elif "mem-per-cpu" in options:
        mem_gb = parse_mem_gb(options["mem-per-cpu"]) * cpus
    elif "mem-per-gpu" in options:
        mem_gb = parse_mem_gb(options["mem-per-gpu"]) * sum(gpus.values())
    return Resources(cpus=cpus, gpus=dict(gpus), mem_gb=mem_gb)


def total_resources(components: Sequence[dict[str, str]]) -> Resources | None:
    """The resources of all the job's components together, or None if sbatch printed none."""
    if not components:
        return None
    parts = [component_resources(c) for c in components]
    gpus: Counter[str] = Counter()
    for part in parts:
        gpus.update(part.gpus)
    mems = [p.mem_gb for p in parts]
    return Resources(
        cpus=sum(p.cpus for p in parts),
        gpus=dict(gpus),
        mem_gb=None if None in mems else sum(m for m in mems if m is not None),
    )


def strip_verbose_output(output: str) -> str:
    """Drop what `sbatch -v` adds (the option blocks, debug lines), keeping everything else."""
    kept, in_block = [], False
    for line in output.splitlines():
        plain = _plain_sbatch_line(line)
        if plain == "sbatch: defined options":
            in_block = True
        elif plain == "sbatch: end of defined options":
            in_block = False
        elif not in_block and not plain.startswith("sbatch: debug"):
            kept.append(line)
    return "\n".join(kept)


def clean_sbatch_error(output: str) -> str:
    """Strip the `sbatch: error:` prefixes from sbatch's error output.

    >>> clean_sbatch_error("sbatch: error: Batch job submission failed: Invalid account\\n")
    'Batch job submission failed: Invalid account'
    >>> clean_sbatch_error("sbatch: error: \x1b[00;36m-----\x1b[00m\\nallocation failure: Invalid account\\n")
    'allocation failure: Invalid account'
    """
    lines = [
        re.sub(r"^sbatch: (error: )?", "", _ANSI_RE.sub("", line)).strip()
        for line in strip_verbose_output(output).splitlines()
    ]
    # Drop the banner some clusters (Mila) print around the real message.
    lines = [line for line in lines if line and not re.fullmatch(r"-+|# .*", line)]
    return "; ".join(lines)


def describe_config(sbatch_args: SbatchArgs) -> str:
    """Short description of the flags that tell the configurations of one cluster apart."""
    keys = ("account", "gres", "gpus", "partition", "constraint", "qos", "time")
    return " ".join(f"{k}={sbatch_args[k]}" for k in keys if k in sbatch_args) or "-"


async def test_only_submission(submission: Submission) -> TestOnlyResult:
    """Run this submission's `sbatch --test-only` command (`get_submissions(test_only=True)`)."""
    command = submission.sbatch_command
    kwargs = dict(cluster=submission.cluster, config=describe_config(submission.sbatch_args))
    if submission.remote is not None:
        result = await submission.remote.run(command, display=False, warn=True, hide=True)
    else:
        result = await run(tuple(shlex.split(command)), warn=True, hide=True, _display=False)
    output = f"{result.stderr}\n{result.stdout}"
    if result.returncode == 0 and (forecast := parse_test_only_output(output)):
        resources = total_resources(parse_defined_options(output))
        if resources is not None and forecast["cpus"] is not None:
            # Slurm's count is what the job gets: more than asked for on clusters that allocate
            # whole nodes (Trillium: 192 cpus for 2 nodes, where the options add up to 120).
            resources = dataclasses.replace(resources, cpus=forecast["cpus"])  # type: ignore[arg-type]
        return TestOnlyResult(**kwargs, **forecast, resources=resources)  # type: ignore[arg-type]
    if result.returncode == 0:
        return TestOnlyResult(
            **kwargs, error=f"accepted, but no start time in sbatch's output: {output.strip()!r}"
        )
    return TestOnlyResult(
        **kwargs, error=clean_sbatch_error(output) or f"exit {result.returncode}"
    )


def format_delta(delta: datetime.timedelta) -> str:
    """
    >>> format_delta(datetime.timedelta(seconds=30))
    'now'
    >>> format_delta(datetime.timedelta(hours=2, minutes=5))
    'in 2h05m'
    >>> format_delta(datetime.timedelta(days=1, hours=3))
    'in 1d03h'
    """
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "now"
    minutes, _ = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"in {days}d{hours:02d}h"
    if hours:
        return f"in {hours}h{minutes:02d}m"
    return f"in {minutes}m"


def sort_by_start(results: Sequence[TestOnlyResult]) -> list[TestOnlyResult]:
    """Soonest start first; rejected configurations last, in their original order."""
    far_future = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
    return sorted(results, key=lambda r: r.start or far_future)


def render_test_only_table(
    results: list[TestOnlyResult], now: datetime.datetime | None = None
) -> rich.table.Table:
    """The forecasts, soonest start first, with start times in this machine's local timezone."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    table = rich.table.Table(
        "Cluster",
        "Configuration",
        f"Estimated start ({datetime.datetime.now().astimezone():%Z})",
        "Resources",
        title="sbatch --test-only (nothing was submitted)",
        box=rich.box.ROUNDED,
        show_lines=True,
        header_style="bold white on #1a1a2e",
        title_style="bold cyan",
    )
    for r in sort_by_start(results):
        if r.error is not None:
            table.add_row(
                r.cluster,
                r.config,
                rich.text.Text(f"REJECTED: {r.error}", style="red"),
                "",
            )
            continue
        assert r.start is not None
        local_start = r.start.astimezone()
        start = f"{local_start:%Y-%m-%d %H:%M:%S} ({format_delta(r.start - now)})"
        resources = str(r.resources) if r.resources else "-"
        table.add_row(r.cluster, r.config, start, resources)
    return table


async def test_only(
    submissions: list[Submission], not_tested: Sequence[TestOnlyResult] = ()
) -> list[TestOnlyResult]:
    """Run `sbatch --test-only` for every submission, in parallel, and print the table, with
    the rows of `not_tested` (configurations that couldn't be tested, and why) too.

    Exits with status 1 if any configuration is rejected, so this can gate a CI job.
    """
    results = [
        *not_tested,
        *await asyncio.gather(*map(test_only_submission, submissions)),
    ]
    console.print(render_test_only_table(results))
    if not all(r.accepted for r in results):
        sys.exit(1)
    return results
