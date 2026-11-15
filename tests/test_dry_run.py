import datetime
import shlex
import subprocess
import time
from pathlib import Path, PurePosixPath

import pytest

import cluv.cli.submit as submit_module
from cluv.cache import Submission
from cluv.cli.submit_utils import dry_run
from cluv.cli.submit_utils.dry_run import (
    TestOnlyResult,
    clean_sbatch_error,
    parse_test_only_output,
    render_test_only_table,
)
from cluv.config import get_cluv_config

UTC = datetime.timezone.utc

FORECAST = (
    "sbatch: Job 123 to start at 2026-11-03T17:00:00 using 4 processors on nodes cn-a001 "
    "in partition main\n"
)


def test_parse_forecast() -> None:
    assert parse_test_only_output(FORECAST) == {
        "start": datetime.datetime(2026, 11, 3, 17, 0, 0, tzinfo=UTC),
        "cpus": 4,
        "nodes": "cn-a001",
        "partition": "main",
    }


def test_parse_forecast_among_other_lines_and_minimal_form() -> None:
    out = (
        "sbatch: warning: something\n"
        "sbatch: Job 7 to start at 2026-11-03T17:00:00 using 1 processors on nodes cn[1-2] "
        "in partition gpu,long\n"
    )
    parsed = parse_test_only_output(out)
    assert parsed is not None
    assert parsed["nodes"] == "cn[1-2]" and parsed["partition"] == "gpu,long"
    minimal = parse_test_only_output("Job 7 to start at 2026-11-03T17:00:00")
    assert minimal == {
        "start": datetime.datetime(2026, 11, 3, 17, tzinfo=UTC),
        "cpus": None,
        "nodes": None,
        "partition": None,
    }


def test_parse_returns_none_without_forecast() -> None:
    assert parse_test_only_output("sbatch: error: Invalid account\n") is None
    assert parse_test_only_output("") is None


def test_clean_sbatch_error() -> None:
    out = "sbatch: error: Batch job submission failed: Invalid account\n"
    assert clean_sbatch_error(out) == "Batch job submission failed: Invalid account"


def test_test_only_command_is_the_submit_command_with_sbatch_test_only() -> None:
    """Same directory, script and environment as a real submission; only sbatch's flags differ."""
    kwargs = dict(
        job_script=PurePosixPath("scripts/job.sh"),
        sbatch_args={"account": "def-x", "time": "0:10:00"},
        program_args=["--lr", "0.1"],
        env_vars={"GIT_COMMIT": "abc"},
        project_dir_on_cluster=PurePosixPath("$SCRATCH/proj"),
    )
    real = submit_module.get_sbatch_command(**kwargs)
    test_only = submit_module.get_sbatch_command(**kwargs, test_only=True)
    assert test_only == real.replace("sbatch --parsable", "TZ=UTC sbatch --verbose --test-only")
    (inner,) = shlex.split(test_only)[3:]
    assert inner.startswith("export GIT_COMMIT=abc; cd $SCRATCH/proj && TZ=UTC sbatch ")
    assert "--parsable" not in test_only and "scripts/job.sh" in inner


def test_table_shows_accepted_and_rejected() -> None:
    now = datetime.datetime(2026, 11, 3, 15, 0, tzinfo=UTC)
    results = [
        TestOnlyResult(
            cluster="mila",
            config="account=a",
            start=datetime.datetime(2026, 11, 3, 17, 0, tzinfo=UTC),
            cpus=4,
            nodes="cn-a001",
            partition="main",
        ),
        TestOnlyResult(cluster="fir", config="account=b", error="Invalid account"),
    ]
    table = render_test_only_table(results, now=now)
    from rich.console import Console

    console = Console(width=140, record=True)
    console.print(table)
    text = console.export_text()
    assert "in 2h00m" in text
    assert "cn-a001" not in text and "Partition" not in text  # not worth the width
    assert "REJECTED: Invalid account" in text


def _render(results: list[TestOnlyResult], now: datetime.datetime) -> str:
    from rich.console import Console

    console = Console(width=160, record=True)
    console.print(render_test_only_table(results, now=now))
    return console.export_text()


@pytest.fixture
def montreal_local_time(monkeypatch: pytest.MonkeyPatch):
    """Make this process's local timezone America/Montreal (UTC-5 in November)."""
    monkeypatch.setenv("TZ", "America/Montreal")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def test_table_shows_start_times_in_local_time(montreal_local_time) -> None:
    """sbatch ran with TZ=UTC; the table converts its start times to this machine's timezone."""
    now = datetime.datetime(2026, 11, 3, 15, 0, tzinfo=UTC)
    start = datetime.datetime(2026, 11, 3, 17, 0, tzinfo=UTC)
    text = _render([TestOnlyResult(cluster="fir", config="-", start=start)], now)
    assert "2026-11-03 12:00:00 (in 2h00m)" in text
    assert "Estimated start (EST)" in text


def test_table_rows_go_from_soonest_to_latest_start() -> None:
    now = datetime.datetime(2026, 11, 3, 15, 0, tzinfo=UTC)
    results = [
        TestOnlyResult(cluster="late", config="-", start=now + datetime.timedelta(days=2)),
        TestOnlyResult(cluster="rejected", config="-", error="nope"),
        TestOnlyResult(cluster="soon", config="-", start=now + datetime.timedelta(minutes=5)),
        TestOnlyResult(cluster="middle", config="-", start=now + datetime.timedelta(hours=3)),
    ]
    text = _render(results, now)
    positions = [text.index(name) for name in ("soon", "middle", "late", "rejected")]
    assert positions == sorted(positions)


class _FakeRemote:
    def __init__(self, returncode: int, stderr: str, hostname: str = "mila") -> None:
        self.returncode, self.stderr, self.hostname = returncode, stderr, hostname
        self.calls: list[tuple[str, str | None]] = []

    async def run(self, command, *, input=None, **kwargs):
        import subprocess

        self.calls.append((command, input))
        return subprocess.CompletedProcess(command, self.returncode, "", self.stderr)


def _submission(tmp_path: Path, remote, cluster: str = "mila") -> Submission:
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\n#SBATCH --gres=gpu:1\necho hi\n")
    return Submission(
        cluster=cluster,
        remote=remote,
        job_script=script,
        sbatch_args={"account": "a", "time": "0:10:00"},
        program_args=["x"],
        sbatch_command="the test-only sbatch command",
        n_chunks=None,
        git_commit="c",
    )


async def test_submission_runs_its_command_on_the_cluster_and_parses(tmp_path: Path) -> None:
    remote = _FakeRemote(0, FORECAST)
    result = await dry_run.test_only_submission(_submission(tmp_path, remote))
    assert (
        result.accepted
        and result.partition == "main"
        and result.config == "account=a time=0:10:00"
    )
    # The job script is the one in the project on the cluster (pulled first), not sent on stdin.
    assert remote.calls == [("the test-only sbatch command", None)]


async def test_submission_reports_rejection(tmp_path: Path) -> None:
    remote = _FakeRemote(1, "sbatch: error: Batch job submission failed: Invalid account\n")
    result = await dry_run.test_only_submission(_submission(tmp_path, remote))
    assert not result.accepted
    assert result.error == "Batch job submission failed: Invalid account"


async def test_exit_status_is_1_when_a_config_is_rejected(tmp_path: Path) -> None:
    ok = _submission(tmp_path, _FakeRemote(0, FORECAST))
    bad = _submission(tmp_path, _FakeRemote(1, "sbatch: error: nope\n"))
    with pytest.raises(SystemExit) as exc:
        await dry_run.test_only([ok, bad])
    assert exc.value.code == 1
    assert len(await dry_run.test_only([ok])) == 1


@pytest.fixture
def fake_clusters(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Two clusters, mila and fir (one submission each), with `git push`, the pulls, `sbatch` and
    the table faked; records what happened. The steps a real submission adds (the rest of the
    sync, the clean-git check and autocommit, saving the job) fail the test if called."""
    subs = {
        name: _submission(tmp_path, _FakeRemote(0, FORECAST, hostname=name), cluster=name)
        for name in ("mila", "fir")
    }
    seen: dict = {"pushed": 0, "pulled": [], "get_submissions": [], "shown": None}
    failures: dict[str, Exception] = {}

    async def fake_get_cluster_to_remote(cluster):
        return {name: sub.remote for name, sub in subs.items()}

    async def fake_get_submissions(cluster, **kwargs):
        seen["get_submissions"].append(kwargs)
        if cluster in failures:
            raise failures[cluster]
        return [subs[cluster]]

    async def fake_push():
        seen["pushed"] += 1
        if "push" in failures:
            raise failures["push"]

    async def fake_pull(remote):
        seen["pulled"].append(remote.hostname)
        if f"pull {remote.hostname}" in failures:
            raise failures[f"pull {remote.hostname}"]

    async def fake_test_only(submissions, not_tested):
        seen["shown"] = (list(submissions), list(not_tested))
        return []

    async def fail(*a, **k):
        raise AssertionError("must not be called")

    monkeypatch.setattr(submit_module, "get_cluster_to_remote", fake_get_cluster_to_remote)
    monkeypatch.setattr(submit_module, "get_submissions", fake_get_submissions)
    monkeypatch.setattr(submit_module, "run_git_push_if_needed", fake_push)
    monkeypatch.setattr(submit_module, "update_project_checkout", fake_pull)
    monkeypatch.setattr(dry_run, "test_only", fake_test_only)
    for name in (
        "sync_common_part",
        "sync_per_cluster_part",
        "ensure_clean_git_state",
        "save_job",
    ):
        monkeypatch.setattr(submit_module, name, fail)
    get_cluv_config.cache_clear()
    return subs, seen, failures


async def test_submit_test_only_pushes_and_pulls_but_does_not_sync_commit_or_save(
    fake_clusters,
) -> None:
    subs, seen, _ = fake_clusters
    assert await submit_module.submit("first", None, [], ["x"], test_only=True) is None
    assert seen["pushed"] == 1 and sorted(seen["pulled"]) == ["fir", "mila"]
    assert all(kwargs["test_only"] for kwargs in seen["get_submissions"])
    tested, not_tested = seen["shown"]
    assert tested == list(subs.values()) and not_tested == []


async def test_a_failed_push_is_an_error_in_every_row(fake_clusters) -> None:
    _, seen, failures = fake_clusters
    failures["push"] = subprocess.CalledProcessError(
        1, "git push", stderr="error: failed to push some refs\n"
    )
    await submit_module.submit("first", None, [], [], test_only=True)
    tested, not_tested = seen["shown"]
    assert tested == [] and seen["pulled"] == []
    assert sorted(r.cluster for r in not_tested) == ["fir", "mila"]
    assert {r.error for r in not_tested} == {"git push failed: error: failed to push some refs"}


async def test_a_failed_pull_is_an_error_in_that_clusters_rows(fake_clusters) -> None:
    subs, seen, failures = fake_clusters
    failures["pull fir"] = subprocess.CalledProcessError(
        1, "git pull", stderr="fatal: Not possible to fast-forward, aborting.\n"
    )
    await submit_module.submit("first", None, [], [], test_only=True)
    tested, (row,) = seen["shown"]
    assert tested == [subs["mila"]]
    assert row.cluster == "fir" and row.config == "account=a time=0:10:00"
    assert row.error == "git pull failed: fatal: Not possible to fast-forward, aborting."


async def test_submit_test_only_reports_an_unconfigured_cluster_as_a_rejected_row(
    fake_clusters,
) -> None:
    """A cluster without a job script doesn't crash the run: the others are still tested."""
    subs, seen, failures = fake_clusters
    failures["fir"] = ValueError("No job script specified for cluster 'fir'.")
    await submit_module.submit("first", None, [], [], test_only=True)
    tested, (row,) = seen["shown"]
    assert tested == [subs["mila"]] and seen["pulled"] == ["mila"]
    assert row.cluster == "fir" and "No job script" in row.error


async def test_test_only_puts_unconfigured_clusters_in_the_table(tmp_path: Path) -> None:
    ok = _submission(tmp_path, _FakeRemote(0, FORECAST))
    missing = dry_run.TestOnlyResult(cluster="tamia", config="-", error="no job script")
    with pytest.raises(SystemExit):
        await dry_run.test_only([ok], [missing])


def test_parse_real_cluster_variants() -> None:
    """Outputs seen on Mila: a stray token after the time, and no nodes/partition for het jobs."""
    odd = parse_test_only_output(
        "sbatch: Job 1 to start at 2026-11-03T17:06:26 a using 1 processors on nodes cn-f002 "
        "in partition long-cpu\n"
    )
    assert odd is not None and odd["cpus"] == 1 and odd["nodes"] == "cn-f002"
    assert odd["partition"] == "long-cpu"
    het = parse_test_only_output(
        "sbatch: 0: # Using default long partition\n"
        "sbatch: Job 2 to start at 2026-11-03T17:24:27 using 2 processors on cn-c036\n"
    )
    assert het is not None and het["cpus"] == 2 and het["nodes"] == "cn-c036"
    assert het["partition"] is None


def test_clean_sbatch_error_drops_ansi_banner() -> None:
    out = (
        "sbatch: error: \x1b[00;36m-----------\n"
        "sbatch: error: # Using default long partition\n"
        "sbatch: error: -----------\x1b[00m\n"
        "allocation failure: Invalid account or account/partition combination specified\n"
    )
    assert clean_sbatch_error(out) == (
        "allocation failure: Invalid account or account/partition combination specified"
    )


MILA_VERBOSE = """\
sbatch: defined options
sbatch: -------------------- --------------------
sbatch: cpus-per-task       : 8
sbatch: gpus-per-task       : l40s:1
sbatch: mem-per-gpu         : 48G
sbatch: ntasks              : 2
sbatch: test-only           : set
sbatch: time                : 03:00:00
sbatch: verbose             : 1
sbatch: -------------------- --------------------
sbatch: end of defined options
sbatch: defined options
sbatch: -------------------- --------------------
sbatch: cpus-per-task       : 12
sbatch: gpus-per-task       : l40s:1
sbatch: mem-per-gpu         : 64G
sbatch: ntasks              : 1
sbatch: -------------------- --------------------
sbatch: end of defined options
sbatch: 0: # Using default long partition
sbatch: Job 11533793 to start at 2026-11-04T23:33:57 using 28 processors on cn-l017
"""
"""Real `sbatch -v --test-only` output on Mila for a heterogeneous job (2 + 1 tasks)."""


def test_resources_of_a_heterogeneous_job() -> None:
    resources = dry_run.total_resources(dry_run.parse_defined_options(MILA_VERBOSE))
    assert resources == dry_run.Resources(cpus=28, gpus={"l40s": 3}, mem_gb=160.0)
    assert str(resources) == "28 cpus, 3x l40s, 160GB RAM"


def test_memory_is_unknown_if_a_component_leaves_it_to_the_cluster() -> None:
    out = (
        "sbatch: defined options\nsbatch: mem                 : 32G\n"
        "sbatch: end of defined options\n"
        "sbatch: defined options\nsbatch: gpus                : 2\n"
        "sbatch: end of defined options\n"
    )
    resources = dry_run.total_resources(dry_run.parse_defined_options(out))
    assert str(resources) == "2 cpus, 2x gpu, default RAM"


async def test_verbose_submission_reports_resources(tmp_path: Path) -> None:
    result = await dry_run.test_only_submission(
        _submission(tmp_path, _FakeRemote(0, MILA_VERBOSE))
    )
    assert result.resources is not None and result.resources.cpus == result.cpus == 28


def test_verbose_option_blocks_are_not_part_of_the_error() -> None:
    out = (
        "sbatch: defined options\nsbatch: account             : nope\n"
        "sbatch: end of defined options\n"
        "sbatch: debug:  propagating UMASK=0027\n"
        "sbatch: error: Batch job submission failed: Invalid account\n"
    )
    assert clean_sbatch_error(out) == "Batch job submission failed: Invalid account"


async def test_resources_take_slurm_cpu_count_and_uppercase_prefix(tmp_path: Path) -> None:
    """Trillium's sbatch wrapper prints `SBATCH:`, and allocates whole nodes (more cpus)."""
    out = (
        "SBATCH: defined options\n"
        "SBATCH: cpus-per-task       : 24\n"
        "SBATCH: gpus-per-node       : h100:4\n"
        "SBATCH: nodes               : 1\n"
        "SBATCH: ntasks-per-node     : 4\n"
        "SBATCH: end of defined options\n"
        "SBATCH: Job 1 to start at 2026-11-05T00:59:25 using 192 processors on trig0008\n"
    )
    result = await dry_run.test_only_submission(_submission(tmp_path, _FakeRemote(0, out)))
    assert str(result.resources) == "192 cpus, 4x h100, default RAM"
