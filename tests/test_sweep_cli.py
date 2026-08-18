"""Tests for `cluv.cli.sweep` — CLI orchestration / multi-job fan-out for `cluv sweep`.

See `design/cluv-sweep.md` §4 for the design these tests are derived from.
"""

from pathlib import Path

import pytest

import cluv.cli.sweep
from cluv.cli.sweep import compute_job_capacity, default_sweep_name, sweep


class TestComputeJobCapacity:
    def test_no_sizing_flags_defaults_to_one(self):
        assert compute_job_capacity([]) == 1

    def test_explicit_ntasks(self):
        assert compute_job_capacity(["--ntasks=5"]) == 5

    def test_ntasks_per_gpu_alone_defaults_to_one_gpu(self):
        assert compute_job_capacity(["--ntasks-per-gpu=3"]) == 3

    def test_ntasks_per_gpu_with_explicit_gpu_count(self):
        assert compute_job_capacity(["--ntasks-per-gpu=2", "--gres=gpu:h100:3"]) == 6

    def test_last_match_wins_for_ntasks(self):
        # Same convention as chunking.py's time parsing: last occurrence wins.
        assert compute_job_capacity(["--ntasks=2", "--ntasks=5"]) == 5


class TestDefaultSweepName:
    def test_uses_job_script_stem(self):
        assert default_sweep_name(Path("scripts/job.sh")) == "job"


class TestSweep:
    @pytest.fixture(autouse=True)
    def _mock_current_cluster(self, monkeypatch: pytest.MonkeyPatch):
        # Same cluster as submitted to, so the sync-before-fan-out branch is skipped —
        # `sync()` itself is exercised by tests/test_sync.py, not here.
        monkeypatch.setattr(cluv.cli.sweep, "current_cluster", lambda: "mila")

    @pytest.fixture
    def recording_submit(self, monkeypatch: pytest.MonkeyPatch):
        calls = []

        async def _fake_submit(**kwargs):
            calls.append(kwargs)
            return object()  # stand-in for a `Job`

        monkeypatch.setattr(cluv.cli.sweep, "submit", _fake_submit)
        return calls

    @pytest.fixture
    def job_script(self, tmp_path: Path) -> Path:
        job_script = tmp_path / "job.sh"
        job_script.touch()
        return job_script

    async def test_number_of_submit_calls_matches_combos_and_capacity(
        self, job_script, recording_submit
    ):
        jobs = await sweep(
            cluster="mila",
            job_script=job_script,
            name="my-sweep",
            sbatch_args=["--ntasks-per-gpu=2"],
            program_args=["python", "main.py", "--x=1,2,3,4,5"],  # 5 combos, capacity 2
        )

        assert len(recording_submit) == 3  # ceil(5 / 2)
        assert len(jobs) == 3

    async def test_offsets_are_job_index_times_capacity(self, job_script, recording_submit):
        await sweep(
            cluster="mila",
            job_script=job_script,
            name="my-sweep",
            sbatch_args=["--ntasks-per-gpu=2"],
            program_args=["python", "main.py", "--x=1,2,3,4,5"],
        )

        offsets = [int(c["extra_env"]["CLUV_SWEEP_TASK_OFFSET"]) for c in recording_submit]
        assert offsets == [0, 2, 4]
        assert all(c["extra_env"]["CLUV_SWEEP_NAME"] == "my-sweep" for c in recording_submit)

    async def test_sbatch_args_and_program_args_identical_across_jobs(
        self, job_script, recording_submit
    ):
        sbatch_args = ["--ntasks-per-gpu=2"]
        program_args = ["python", "main.py", "--x=1,2,3,4,5"]

        await sweep(
            cluster="mila",
            job_script=job_script,
            name="my-sweep",
            sbatch_args=sbatch_args,
            program_args=program_args,
        )

        assert all(c["sbatch_args"] == sbatch_args for c in recording_submit)
        assert all(c["program_args"] == program_args for c in recording_submit)
        assert all(c["in_job_packing"] is True for c in recording_submit)
        assert all(c["job_script"] == job_script for c in recording_submit)

    async def test_no_sizing_flags_submits_one_job_per_combo(self, job_script, recording_submit):
        await sweep(
            cluster="mila",
            job_script=job_script,
            name=None,
            sbatch_args=[],
            program_args=["python", "main.py", "--x=1,2,3"],
        )

        assert len(recording_submit) == 3

    async def test_default_name_from_job_script_when_not_given(self, job_script, recording_submit):
        await sweep(
            cluster="mila",
            job_script=job_script,
            name=None,
            sbatch_args=[],
            program_args=["python", "main.py"],
        )

        assert recording_submit[0]["extra_env"]["CLUV_SWEEP_NAME"] == "job"

    async def test_cluster_first_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            await sweep(
                cluster="first", job_script=None, name=None, sbatch_args=[], program_args=[]
            )
