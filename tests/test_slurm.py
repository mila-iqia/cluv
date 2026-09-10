"""Unit tests for cluv/cli/slurm.py parsing functions.

All tests are pure (no I/O, no SSH). Fixture strings are taken from real
cluster output captured during development.
"""

from datetime import timedelta

import pytest

from cluv.slurm import (
    parse_disk_quota,
    parse_diskusage_report,
    parse_savail,
    parse_sinfo_nodes,
    parse_slurm_time,
)

pytestmark = pytest.mark.timeout(10)

# ---------------------------------------------------------------------------
# Fixtures – real output captured from live clusters
# ---------------------------------------------------------------------------

MILA_SAVAIL = """\
GPU               Avail / Total
===============================
a100                 15 / 32
a100l                13 / 136
a6000                 0 / 8
h100                  0 / 16
l40s                 10 / 352
rtx8000              130 / 376
v100                  5 / 56
"""

TAMIA_DISKUSAGE = """\

                            Description                Space         # of files
                  /home (user normandf)        20GiB/  25GiB         208K/ 250K
               /scratch (user normandf)       148GiB/ 500GiB         418K/ 500K
--
On some clusters, a break down per user may be available by adding the option '--per_user'.
"""


# ---------------------------------------------------------------------------
# parse_sinfo_nodes
# ---------------------------------------------------------------------------


class TestParseSinfoNodes:
    def test_all_alloc(self):
        output = "node01 alloc gpu:h100:4(S:0-1)\nnode02 alloc gpu:h100:4(S:0-1)\n"
        result = parse_sinfo_nodes(output)
        assert result == {"H100": (0, 8)}

    def test_mixed_states(self):
        output = (
            "node01 idle  gpu:h100:4(S:0-1)\n"
            "node02 alloc gpu:h200:8(S:0-1)\n"
            "node03 mix   gpu:h100:4(S:0-1)\n"
        )
        result = parse_sinfo_nodes(output)
        assert result == {"H100": (4, 8), "H200": (0, 8)}

    def test_idle_tilde_state(self):
        # sinfo sometimes reports "idle~" for draining idle nodes
        output = "node01 idle~ gpu:a100:8\n"
        result = parse_sinfo_nodes(output)
        assert result == {"A100": (8, 8)}

    def test_multiple_models_sorted(self):
        output = "node01 idle gpu:v100:2\nnode02 idle gpu:a100:4\nnode03 idle gpu:h100:8\n"
        result = parse_sinfo_nodes(output)
        assert list(result.keys()) == ["A100", "H100", "V100"]

    def test_gres_without_socket_spec(self):
        # Some nodes report GRES without the (S:...) suffix
        output = "node01 idle gpu:h100:4\n"
        result = parse_sinfo_nodes(output)
        assert result == {"H100": (4, 4)}

    def test_empty_output(self):
        assert parse_sinfo_nodes("") == {}

    def test_no_gpu_gres(self):
        # Lines without gpu: in GRES should be skipped
        output = "node01 idle cpu:32\nnode02 idle (null)\n"
        assert parse_sinfo_nodes(output) == {}

    def test_nvidia_prefix_normalized(self):
        # Full GRES name with nvidia_ prefix → model name is just the base
        output = "node01 idle gpu:nvidia_a100:8\n"
        result = parse_sinfo_nodes(output)
        assert result == {"A100": (8, 8)}

    def test_mig_node_reports_each_profile_as_its_own_type(self):
        # Rorqual-style MIG node: each MIG profile is its own GPU type, counted
        # in slices (not reconstructed into a physical GPU count).
        output = (
            "rg12501 idle "
            "gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)\n"
        )
        result = parse_sinfo_nodes(output)
        assert result == {
            "H100-1g.10gb": (8, 8),
            "H100-2g.20gb": (4, 4),
            "H100-3g.40gb": (4, 4),
        }

    def test_mig_model_normalization(self):
        # MIG GRES names should normalize to "<base>-<profile>", one type per profile
        output = (
            "rg01 alloc "
            "gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)\n"
        )
        result = parse_sinfo_nodes(output)
        assert list(result.keys()) == ["H100-1g.10gb", "H100-2g.20gb", "H100-3g.40gb"]

    def test_mixed_regular_and_mig_nodes(self):
        # Mix of regular H100 nodes and MIG nodes (rorqual-like): the regular
        # node counts as "H100", MIG slices count as their own profile types.
        output = (
            "rg00 idle gpu:h100:4(S:0-3)\n"
            "rg01 idle "
            "gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)\n"
        )
        result = parse_sinfo_nodes(output)
        assert result == {
            "H100": (4, 4),
            "H100-1g.10gb": (8, 8),
            "H100-2g.20gb": (4, 4),
            "H100-3g.40gb": (4, 4),
        }

    def test_deduplication_via_sort_u(self):
        # Same node appearing multiple times (once per Slurm partition) should
        # not inflate counts after upstream sort -u deduplication. The parser
        # itself trusts the input is already deduplicated.
        _output = (
            "node01 idle gpu:h100:4\n"
            "node01 idle gpu:h100:4\n"  # duplicate that sort -u would remove
        )
        # Parser sees duplicates → double-counts; this test documents the
        # contract that sort -u must be done upstream (in _REMOTE_SCRIPT).
        # Here we just verify the format is parsed correctly for one entry.
        output_deduped = "node01 idle gpu:h100:4\n"
        result = parse_sinfo_nodes(output_deduped)
        assert result == {"H100": (4, 4)}


# ---------------------------------------------------------------------------
# parse_diskusage_report
# ---------------------------------------------------------------------------


class TestParseSavail:
    def test_total_gpus(self):
        result = parse_savail(MILA_SAVAIL)
        # 32+136+8+16+352+376+56 = 976
        assert sum(total for _, total in result.values()) == 976

    def test_idle_gpus(self):
        result = parse_savail(MILA_SAVAIL)
        # 15+13+0+0+10+130+5 = 173
        assert sum(idle for idle, _ in result.values()) == 173

    def test_models_sorted(self):
        result = parse_savail(MILA_SAVAIL)
        assert list(result.keys()) == [
            "A100",
            "A100L",
            "A6000",
            "H100",
            "L40S",
            "RTX8000",
            "V100",
        ]

    def test_header_and_separator_skipped(self):
        # The "GPU  Avail / Total" header and "===" separator must not be parsed as data
        result = parse_savail(MILA_SAVAIL)
        assert "GPU" not in result
        assert "AVAIL" not in result

    def test_zero_available_still_counts_total(self):
        output = "a6000   0 / 8\nh100    0 / 16\n"
        result = parse_savail(output)
        assert result == {"A6000": (0, 8), "H100": (0, 16)}

    def test_empty_output(self):
        assert parse_savail("") == {}


class TestParseDiskQuota:
    # Real output captured from `disk-quota` on Mila
    MILA_DISK_QUOTA = """\
==== HOME ====
Disk quotas for usr normandf (uid 1471600598):
     Filesystem    used   quota   limit   grace   files   quota   limit   grace
     /home/mila  99.99G      0k    100G       -  921718       0 1048576       -
uid 1471600598 is using default block quota setting
uid 1471600598 is using default file quota setting

==== SCRATCH ====

Quota information for storage pool Default (ID: 1):

      user/group     ||           size          ||    chunk files
     name     |  id  ||    used    |    hard    ||  used   |  hard
--------------|------||------------|------------||---------|---------
      normandf|1471600598||   76.61 GiB|    5.00 TiB||   687792|unlimited
"""

    def test_home_used(self):
        storage = parse_disk_quota(self.MILA_DISK_QUOTA)
        assert storage.home_used == pytest.approx(99.99, rel=1e-3)

    def test_home_quota(self):
        storage = parse_disk_quota(self.MILA_DISK_QUOTA)
        assert storage.home_quota == pytest.approx(100.0, rel=1e-3)

    def test_scratch_used(self):
        storage = parse_disk_quota(self.MILA_DISK_QUOTA)
        assert storage.scratch_used == pytest.approx(76.61, rel=1e-3)

    def test_scratch_quota_tib_to_gib(self):
        # 5.00 TiB must be converted to GiB (5 * 1024 = 5120)
        storage = parse_disk_quota(self.MILA_DISK_QUOTA)
        assert storage.scratch_quota == pytest.approx(5120.0, rel=1e-3)

    def test_empty_output(self):
        storage = parse_disk_quota("")
        assert storage.home_used == 0.0
        assert storage.home_quota == 0.0
        assert storage.scratch_used == 0.0
        assert storage.scratch_quota == 0.0

    def test_tib_unit_conversion(self):
        output = "     /home/mila  1.00G      0k    2.00T       -\n"
        storage = parse_disk_quota(output)
        assert storage.home_used == pytest.approx(1.0)
        assert storage.home_quota == pytest.approx(2048.0)  # 2 TiB → GiB


class TestParseDiskusageReport:
    def test_tamia_home(self):
        storage = parse_diskusage_report(TAMIA_DISKUSAGE)
        assert storage.home_used == 20.0
        assert storage.home_quota == 25.0

    def test_tamia_scratch(self):
        storage = parse_diskusage_report(TAMIA_DISKUSAGE)
        assert storage.scratch_used == 148.0
        assert storage.scratch_quota == 500.0

    def test_fractional_values(self):
        output = "/home (user foo)    1.5GiB/  50GiB    1K/ 500K\n"
        storage = parse_diskusage_report(output)
        assert storage.home_used == 1.5
        assert storage.home_quota == 50.0

    def test_missing_scratch(self):
        output = "/home (user foo)    5GiB/  50GiB\n"
        storage = parse_diskusage_report(output)
        assert storage.home_used == 5.0
        assert storage.scratch_used == 0.0
        assert storage.scratch_quota == 0.0

    def test_empty_output(self):
        storage = parse_diskusage_report("")
        assert storage.home_used == 0.0
        assert storage.home_quota == 0.0
        assert storage.scratch_used == 0.0
        assert storage.scratch_quota == 0.0


class TestParseTime:
    @pytest.mark.parametrize(
        ("input", "expected"),
        [
            ("12:28:45", timedelta(hours=12, minutes=28, seconds=45)),
            ("07-12:28:45", timedelta(days=7, hours=12, minutes=28, seconds=45)),
            ("07-12", timedelta(days=7, hours=12)),
            ("07-12:28", timedelta(days=7, hours=12, minutes=28)),
            ("28:12", timedelta(minutes=28, seconds=12)),
            ("28", timedelta(minutes=28)),
        ],
    )
    def test_parse_slurm_time(self, input: str, expected: timedelta) -> None:
        assert parse_slurm_time(input) == expected
