"""Unit tests for the `--vram` support of `cluv submit`.

All tests are pure (no I/O, no SSH). The fixture strings are real
`sinfo --noheader --format='%f|%G' | sort -u` output captured on the clusters.
"""

from pathlib import Path
from unittest import mock

import pytest

import cluv.cli.submit_utils.vram
from cluv.cli.submit import expand_for_vram
from cluv.cli.submit_utils.vram import (
    GpuRequest,
    compatible_gpu_types,
    get_gpu_request,
    gpu_vram_gb,
    parse_gpu_types,
    parse_vram,
    sbatch_args_for_gpu_type,
)
from cluv.sbatch_args import SbatchArgs

pytestmark = pytest.mark.timeout(10)

RORQUAL_SINFO = """\
h100|gpu:h100:4(S:0-1)
h100mig|gpu:nvidia_h100_80gb_hbm3_3g.40gb:3(S:0-1),gpu:nvidia_h100_80gb_hbm3_2g.20gb:3(S:0-1),gpu:nvidia_h100_80gb_hbm3_1g.10gb:6(S:0-1)
h100mig|gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-1),gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-1),gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-1)
genoa|(null)
"""

NARVAL_SINFO = """\
genoa|(null)
milan,a100mig|gpu:a100_3g.20gb:3(S:1,3,7),gpu:a100_4g.20gb:1(S:3),gpu:a100_2g.10gb:4(S:1,5,7),gpu:a100_1g.5gb:7(S:1,5,7)
milan,a100,nvlink|gpu:a100:4(S:0-1)
milan|(null)
"""

MILA_SINFO = """\
x86_64,ampere,48gb|gpu:a6000:8(S:0-7)
x86_64,ampere,nvlink,40gb|gpu:a100:4(S:0-7)
x86_64,ampere,nvlink,dgx,80gb|gpu:a100l:8
x86_64,hopper,nvlink,80gb|gpu:h100:8(S:0-23)
x86_64,lovelace,48gb|gpu:l40s:4(S:0)
x86_64,turing,48gb|gpu:rtx8000:8(S:0-15)
x86_64,volta,nvlink,dgx,32gb|gpu:v100:8
x86_64,milan,archive|(null)
"""

TAMIA_SINFO = """\
x86_64,h100,hopper,nvlink,80gb|gpu:h100:4(S:0-1)
x86_64,h200,hopper,nvlink,150gb|gpu:h200:8(S:0-1)
x86_64,sapphire|(null)
"""


class TestParseVram:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("10GB", 10.0),
            ("10G", 10.0),
            ("10gb", 10.0),
            ("10GiB", 10.0),
            ("10", 10.0),
            ("10.5GB", 10.5),
            ("20480MB", 20.0),
            ("1T", 1024.0),
        ],
    )
    def test_parse_vram(self, value: str, expected: float):
        assert parse_vram(value) == expected

    @pytest.mark.parametrize("value", ["", "lots", "10 gigs", "GB"])
    def test_invalid_values_raise(self, value: str):
        with pytest.raises(ValueError):
            parse_vram(value)


class TestGpuVramGb:
    @pytest.mark.parametrize(
        ("gpu_type", "expected"),
        [
            # MIG slices: the VRAM is the second number of the profile.
            ("nvidia_h100_80gb_hbm3_1g.10gb", 10.0),
            ("nvidia_h100_80gb_hbm3_3g.40gb", 40.0),
            ("a100_1g.5gb", 5.0),
            ("a100_4g.20gb", 20.0),
            # Full GPUs whose name says how much VRAM they have.
            ("nvidia_h100_80gb_hbm3", 80.0),
            # Full GPUs from the hard-coded fallback.
            ("h100", 80.0),
            ("a100", 40.0),
            ("l40s", 48.0),
        ],
    )
    def test_vram_from_name(self, gpu_type: str, expected: float):
        assert gpu_vram_gb(gpu_type) == expected

    def test_vram_from_node_features(self):
        # Mila's V100s are 32GB ones, unlike the 16GB default of the fallback table.
        assert gpu_vram_gb("v100", "x86_64,volta,nvlink,dgx,32gb") == 32.0
        assert gpu_vram_gb("v100") == 16.0

    def test_unknown_gpu_type(self):
        assert gpu_vram_gb("b200") is None


class TestParseGpuTypes:
    def test_rorqual(self):
        assert parse_gpu_types(RORQUAL_SINFO) == {
            "h100": 80.0,
            "nvidia_h100_80gb_hbm3_1g.10gb": 10.0,
            "nvidia_h100_80gb_hbm3_2g.20gb": 20.0,
            "nvidia_h100_80gb_hbm3_3g.40gb": 40.0,
        }

    def test_narval(self):
        assert parse_gpu_types(NARVAL_SINFO) == {
            "a100": 40.0,
            "a100_1g.5gb": 5.0,
            "a100_2g.10gb": 10.0,
            "a100_3g.20gb": 20.0,
            "a100_4g.20gb": 20.0,
        }

    def test_mila_uses_the_node_features(self):
        assert parse_gpu_types(MILA_SINFO) == {
            "a100": 40.0,
            "a100l": 80.0,
            "a6000": 48.0,
            "h100": 80.0,
            "l40s": 48.0,
            "rtx8000": 48.0,
            "v100": 32.0,
        }

    def test_tamia_has_a_gpu_that_isnt_in_the_fallback_table(self):
        assert parse_gpu_types(TAMIA_SINFO) == {"h100": 80.0, "h200": 150.0}

    def test_empty_output(self):
        assert parse_gpu_types("genoa|(null)\n") == {}

    def test_smallest_vram_wins(self):
        output = "volta,16gb|gpu:v100:8\nvolta,32gb|gpu:v100:8\n"
        assert parse_gpu_types(output) == {"v100": 16.0}


class TestCompatibleGpuTypes:
    def test_races_between_mig_slices_and_full_gpus(self):
        # The example from the issue: 10GB on Rorqual should race the 10G, 20G and 40G slices
        # against the full H100s, easiest (smallest) first.
        assert compatible_gpu_types(parse_gpu_types(RORQUAL_SINFO), vram="10GB") == [
            "nvidia_h100_80gb_hbm3_1g.10gb",
            "nvidia_h100_80gb_hbm3_2g.20gb",
            "nvidia_h100_80gb_hbm3_3g.40gb",
            "h100",
        ]

    def test_slices_that_are_too_small_are_excluded(self):
        assert compatible_gpu_types(parse_gpu_types(RORQUAL_SINFO), vram="10GB") == [
            "nvidia_h100_80gb_hbm3_3g.40gb",
            "h100",
        ]

    def test_requested_model_restricts_the_race(self):
        gpu_types = parse_gpu_types(MILA_SINFO)
        assert compatible_gpu_types(gpu_types, vram="10GB") == [
            "v100",
            "a100",
            "a6000",
            "l40s",
            "rtx8000",
            "a100l",
            "h100",
        ]
        assert compatible_gpu_types(gpu_types, vram="10GB", model="a100") == ["a100"]
        assert compatible_gpu_types(gpu_types, vram="10GB", model="a100l") == ["a100l"]

    def test_a_mig_slice_counts_as_its_base_model(self):
        gpu_types = parse_gpu_types(NARVAL_SINFO)
        assert compatible_gpu_types(gpu_types, vram="10GB", model="a100") == [
            "a100_2g.10gb",
            "a100_3g.20gb",
            "a100_4g.20gb",
            "a100",
        ]

    def test_no_compatible_gpu_type(self):
        assert compatible_gpu_types(parse_gpu_types(RORQUAL_SINFO), vram="200GB") == []


class TestFindGpuRequest:
    @pytest.mark.parametrize(
        ("sbatch_args", "expected"),
        [
            ({"gpus": "1"}, GpuRequest("gpus", None, 1)),
            ({"gpus": "h100:1"}, GpuRequest("gpus", "h100", 1)),
            ({"time": "1:00:00", "gpus": "h100:2"}, GpuRequest("gpus", "h100", 2)),
            ({"gres": "gpu:1"}, GpuRequest("gres", None, 1)),
            ({"gres": "gpu:a100:4"}, GpuRequest("gres", "a100", 4)),
            ({"gres": "tmpfs:10G,gpu:a100:1"}, GpuRequest("gres", "a100", 1)),
            ({"gpus-per-node": "h100:1"}, GpuRequest("gpus-per-node", "h100", 1)),
            ({"G": "2"}, GpuRequest("G", None, 2)),
        ],
    )
    def test_from_sbatch_args(self, sbatch_args: dict, expected: GpuRequest):
        assert get_gpu_request(sbatch_args) == expected

    @pytest.mark.parametrize(
        "sbatch_args",
        [{}, {"time": "1:00:00"}, {"gres": "tmpfs:10G"}, {"cpus-per-task": "4"}],
    )
    def test_no_gpu_request(self, sbatch_args: dict):
        assert get_gpu_request(sbatch_args) is None

    def test_from_the_job_script_header(self, tmp_path: Path):
        job_script = tmp_path / "job.sh"
        job_script.write_text(
            "#!/bin/bash\n"
            "#SBATCH --time=1:00:00\n"
            "#SBATCH --gpus-per-node=h100:1\n"
            "\n"
            "echo --gpus=2\n"  # Not part of the header: must be ignored.
        )
        assert get_gpu_request({}, job_script) == GpuRequest("gpus-per-node", "h100", 1)

    def test_sbatch_args_take_precedence_over_the_job_script(self, tmp_path: Path):
        job_script = tmp_path / "job.sh"
        job_script.write_text("#!/bin/bash\n#SBATCH --gpus=a100:1\n")
        assert get_gpu_request({"gpus": "h100:1"}, job_script) == GpuRequest("gpus", "h100", 1)


class TestSbatchArgsForGpuType:
    def test_replaces_the_gpu_flag_in_place(self):
        sbatch_args: SbatchArgs = {"time": "1:00:00", "gpus": "h100:1", "cpus-per-task": "4"}
        gpu_request = get_gpu_request(sbatch_args)
        assert gpu_request
        assert sbatch_args_for_gpu_type(sbatch_args, gpu_request, "h100_1g.10gb") == {
            "time": "1:00:00",
            "gpus": "h100_1g.10gb:1",
            "cpus-per-task": "4",
        }

    def test_keeps_the_short_flag_key(self):
        sbatch_args: SbatchArgs = {"G": "1", "time": "1:00:00"}
        gpu_request = get_gpu_request(sbatch_args)
        assert gpu_request
        assert sbatch_args_for_gpu_type(sbatch_args, gpu_request, "a100_1g.5gb") == {
            "G": "a100_1g.5gb:1",
            "time": "1:00:00",
        }

    def test_keeps_the_gres_form(self):
        sbatch_args: SbatchArgs = {"gres": "gpu:1"}
        gpu_request = get_gpu_request(sbatch_args)
        assert gpu_request
        assert sbatch_args_for_gpu_type(sbatch_args, gpu_request, "a100_1g.5gb") == {
            "gres": "gpu:a100_1g.5gb:1"
        }

    def test_appends_the_flag_when_the_request_is_in_the_job_script(self):
        # sbatch flags override the `#SBATCH` directives of the job script.
        gpu_request = GpuRequest("gpus-per-node", "h100", 1)
        assert sbatch_args_for_gpu_type({"time": "1:00:00"}, gpu_request, "h100_3g.40gb") == {
            "time": "1:00:00",
            "gpus-per-node": "h100_3g.40gb:1",
        }

    def test_appends_the_flag_when_no_gpu_was_requested(self):
        assert sbatch_args_for_gpu_type({"time": "1:00:00"}, GpuRequest(), "h100_3g.40gb") == {
            "time": "1:00:00",
            "gpus": "h100_3g.40gb:1",
        }


class TestExpandForVram:
    @pytest.fixture
    def job_script(self, tmp_path: Path) -> Path:
        job_script = tmp_path / "job.sh"
        job_script.write_text("#!/bin/bash\n#SBATCH --time=1:00:00\n")
        return job_script

    @pytest.fixture(autouse=True)
    def gpu_types(self, monkeypatch: pytest.MonkeyPatch):
        """Don't reach out to the cluster: use the GPU types of Rorqual."""
        gpu_types = parse_gpu_types(RORQUAL_SINFO)
        monkeypatch.setattr(
            cluv.cli.submit_utils.vram, "get_gpu_types", mock.AsyncMock(return_value=gpu_types)
        )
        return gpu_types

    async def test_one_expansion_per_compatible_gpu_type(self, job_script: Path):
        sbatch_args: SbatchArgs = {"account": "rrg-bengioy-ad", "gpus": "1"}
        expanded = await expand_for_vram(
            "rorqual",
            mock.Mock(hostname="rorqual"),
            sbatch_args,
            job_script=job_script,
            vram="10GB",
        )
        assert expanded == [
            {"account": "rrg-bengioy-ad", "gpus": f"{gpu_type}:1"}
            for gpu_type in [
                "nvidia_h100_80gb_hbm3_1g.10gb",
                "nvidia_h100_80gb_hbm3_2g.20gb",
                "nvidia_h100_80gb_hbm3_3g.40gb",
                "h100",
            ]
        ]

    async def test_gpu_type_is_added_when_the_job_doesnt_ask_for_a_gpu(self, job_script: Path):
        sbatch_args: SbatchArgs = {"account": "rrg-bengioy-ad"}
        expanded = await expand_for_vram(
            "rorqual",
            mock.Mock(hostname="rorqual"),
            sbatch_args,
            job_script=job_script,
            vram="40GB",
        )
        assert expanded == [
            {"account": "rrg-bengioy-ad", "gpus": "nvidia_h100_80gb_hbm3_3g.40gb:1"},
            {"account": "rrg-bengioy-ad", "gpus": "h100:1"},
        ]

    async def test_multi_gpu_jobs_are_left_alone(self, job_script: Path):
        sbatch_args: SbatchArgs = {"gpus": "2"}
        assert await expand_for_vram(
            "rorqual",
            mock.Mock(hostname="rorqual"),
            sbatch_args,
            job_script=job_script,
            vram="10GB",
        ) == [sbatch_args]

    async def test_left_alone_when_no_gpu_type_is_big_enough(self, job_script: Path):
        sbatch_args: SbatchArgs = {"account": "rrg-bengioy-ad", "gpus": "1"}
        assert await expand_for_vram(
            "rorqual",
            mock.Mock(hostname="rorqual"),
            sbatch_args,
            job_script=job_script,
            vram="200GB",
        ) == [sbatch_args]

    async def test_left_alone_when_vram_not_set(self, job_script: Path):
        sbatch_args: SbatchArgs = {"account": "rrg-bengioy-ad", "gpus": "1"}
        assert await expand_for_vram(
            "rorqual",
            mock.Mock(hostname="rorqual"),
            sbatch_args,
            job_script=job_script,
            vram=None,
        ) == [sbatch_args]
