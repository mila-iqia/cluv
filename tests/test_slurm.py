"""Unit tests for cluv/cli/slurm.py parsing functions.

All tests are pure (no I/O, no SSH). Fixture strings are taken from real
cluster output captured during development.
"""

import pytest

from cluv.cli.slurm import (
    parse_disk_quota,
    parse_diskusage_report,
    parse_partition_stats,
    parse_savail,
    parse_sinfo_nodes,
)

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

TAMIA_PARTITION_STATS = """\

Node type |                     Max walltime
          |     3 hr   |   12 hr   |   24 hr   |   72 hr   |   168 hr  |
----------|-------------------------------------------------------------
       Number of Queued Jobs by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |     0:0    |    0:1    |    0:0    |    0:0    |    0:0    |
GPU       |     8:-    |    9:-    |    2:-    |    0:-    |    0:-    |
----------|-------------------------------------------------------------
      Number of Running Jobs by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |     0:0    |    0:2    |    0:0    |    0:0    |    0:0    |
GPU       |     8:-    |   14:-    |   35:-    |    0:-    |    0:-    |
----------|-------------------------------------------------------------
        Number of Idle nodes by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |     2:2    |    0:0    |    0:0    |    0:0    |    0:0    |
GPU       |     1:-    |    1:-    |    1:-    |    0:-    |    0:-    |
----------|-------------------------------------------------------------
       Total Number of nodes by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |     8:8    |    6:6    |    4:4    |    0:0    |    0:0    |
GPU       |    65:-    |   59:-    |   49:-    |    0:-    |    0:-    |
----------|-------------------------------------------------------------
"""

# Example with a Large Mem row (from Alliance wiki docs)
WIKI_PARTITION_STATS = """\
Node type |                     Max walltime
          |     3 hr   |   12 hr   |   24 hr   |   72 hr   |   168 hr  |
----------|-------------------------------------------------------------
       Number of Queued Jobs by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |    12:170  |   69:7066 |   70:7335 |  386:961  |   59:509  |
Large Mem |     0:0    |    0:0    |    0:0    |    0:15   |    0:1    |
GPU       |     5:14   |    3:8    |   21:1    |  177:110  |    1:5    |
----------|-------------------------------------------------------------
      Number of Running Jobs by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |     8:32   |   10:854  |   84:10   |   15:65   |    0:674  |
Large Mem |     0:0    |    0:0    |    0:0    |    0:1    |    0:0    |
GPU       |     5:0    |    2:13   |   47:20   |   19:18   |    0:3    |
----------|-------------------------------------------------------------
        Number of Idle nodes by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |    16:9    |   15:8    |   15:8    |    7:0    |    2:0    |
Large Mem |     3:1    |    3:1    |    0:0    |    0:0    |    0:0    |
GPU       |     0:0    |    0:0    |    0:0    |    0:0    |    0:0    |
----------|-------------------------------------------------------------
       Total Number of nodes by partition Type (by node:by core)
----------|-------------------------------------------------------------
Regular   |   871:431  |  851:411  |  821:391  |  636:276  |  281:164  |
Large Mem |    27:12   |   27:12   |   24:11   |   20:3    |    4:3    |
GPU       |   156:78   |  156:78   |  144:72   |  104:52   |   13:12   |
"""

TAMIA_DISKUSAGE = """\

                            Description                Space         # of files
                  /home (user normandf)        20GiB/  25GiB         208K/ 250K
               /scratch (user normandf)       148GiB/ 500GiB         418K/ 500K
--
On some clusters, a break down per user may be available by adding the option '--per_user'.
"""


# ---------------------------------------------------------------------------
# parse_partition_stats
# ---------------------------------------------------------------------------


class TestParsePartitionStats:
    def test_tamia_running_jobs(self):
        result = parse_partition_stats(TAMIA_PARTITION_STATS)
        # GPU running: 8 + 14 + 35 + 0 + 0 = 57
        assert result["jobs_running"] == 57

    def test_tamia_pending_jobs(self):
        result = parse_partition_stats(TAMIA_PARTITION_STATS)
        # GPU queued: 8 + 9 + 2 + 0 + 0 = 19
        assert result["jobs_pending"] == 19

    def test_tamia_idle_nodes(self):
        result = parse_partition_stats(TAMIA_PARTITION_STATS)
        # GPU idle: max(1, 1, 1, 0, 0) = 1 (same physical node in multiple partitions)
        assert result["gpu_idle_nodes"] == 1

    def test_tamia_total_nodes(self):
        result = parse_partition_stats(TAMIA_PARTITION_STATS)
        # GPU total: max(65, 59, 49, 0, 0) = 65
        assert result["gpu_total_nodes"] == 65

    def test_wiki_running_jobs(self):
        result = parse_partition_stats(WIKI_PARTITION_STATS)
        # GPU running node+core per column: (5+0)+(2+13)+(47+20)+(19+18)+(0+3) = 127
        assert result["jobs_running"] == 127

    def test_wiki_pending_jobs(self):
        result = parse_partition_stats(WIKI_PARTITION_STATS)
        # GPU queued node+core per column: (5+14)+(3+8)+(21+1)+(177+110)+(1+5) = 345
        assert result["jobs_pending"] == 345

    def test_wiki_idle_nodes(self):
        result = parse_partition_stats(WIKI_PARTITION_STATS)
        # GPU idle: max(0,0,0,0,0) = 0
        assert result["gpu_idle_nodes"] == 0

    def test_wiki_total_nodes(self):
        result = parse_partition_stats(WIKI_PARTITION_STATS)
        # GPU total: max(156,156,144,104,13) = 156
        assert result["gpu_total_nodes"] == 156

    def test_large_mem_row_ignored(self):
        # Large Mem row must not affect GPU counts
        result = parse_partition_stats(WIKI_PARTITION_STATS)
        assert result["gpu_total_nodes"] == 156  # not 27

    def test_empty_output(self):
        result = parse_partition_stats("")
        assert result == {
            "jobs_running": 0,
            "jobs_pending": 0,
            "gpu_idle_nodes": 0,
            "gpu_total_nodes": 0,
        }

    def test_no_gpu_row(self):
        no_gpu = TAMIA_PARTITION_STATS.replace("GPU", "NOGPU")
        result = parse_partition_stats(no_gpu)
        assert result["jobs_running"] == 0
        assert result["gpu_total_nodes"] == 0


# ---------------------------------------------------------------------------
# parse_sinfo_nodes
# ---------------------------------------------------------------------------


class TestParseSinfoNodes:
    def test_all_alloc(self):
        output = "node01 alloc gpu:h100:4(S:0-1)\nnode02 alloc gpu:h100:4(S:0-1)\n"
        idle, total, models = parse_sinfo_nodes(output)
        assert idle == 0
        assert total == 8
        assert models == ["H100"]

    def test_mixed_states(self):
        output = (
            "node01 idle  gpu:h100:4(S:0-1)\n"
            "node02 alloc gpu:h200:8(S:0-1)\n"
            "node03 mix   gpu:h100:4(S:0-1)\n"
        )
        idle, total, models = parse_sinfo_nodes(output)
        assert idle == 4          # only the idle node
        assert total == 4 + 8 + 4  # all three nodes
        assert models == ["H100", "H200"]

    def test_idle_tilde_state(self):
        # sinfo sometimes reports "idle~" for draining idle nodes
        output = "node01 idle~ gpu:a100:8\n"
        idle, total, models = parse_sinfo_nodes(output)
        assert idle == 8
        assert total == 8

    def test_multiple_models_sorted(self):
        output = (
            "node01 idle gpu:v100:2\n"
            "node02 idle gpu:a100:4\n"
            "node03 idle gpu:h100:8\n"
        )
        _, _, models = parse_sinfo_nodes(output)
        assert models == ["A100", "H100", "V100"]

    def test_gres_without_socket_spec(self):
        # Some nodes report GRES without the (S:...) suffix
        output = "node01 idle gpu:h100:4\n"
        idle, total, models = parse_sinfo_nodes(output)
        assert idle == 4
        assert total == 4
        assert models == ["H100"]

    def test_empty_output(self):
        idle, total, models = parse_sinfo_nodes("")
        assert idle == 0
        assert total == 0
        assert models == []

    def test_no_gpu_gres(self):
        # Lines without gpu: in GRES should be skipped
        output = "node01 idle cpu:32\nnode02 idle (null)\n"
        idle, total, models = parse_sinfo_nodes(output)
        assert idle == 0
        assert total == 0
        assert models == []

    def test_nvidia_prefix_normalized(self):
        # Full GRES name with nvidia_ prefix → model name is just the base
        output = "node01 idle gpu:nvidia_a100:8\n"
        idle, total, models = parse_sinfo_nodes(output)
        assert models == ["A100"]
        assert total == 8

    def test_mig_node_physical_gpu_count(self):
        # Rorqual-style MIG node: 3 MIG profiles from 4 physical H100s
        # sum(g_val * count) = 3*4 + 2*4 + 1*8 = 28; 28 // 7 = 4 physical GPUs
        output = (
            "rg12501 idle "
            "gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)\n"
        )
        idle, total, models = parse_sinfo_nodes(output)
        assert total == 4, f"Expected 4 physical GPUs, got {total}"
        assert idle == 4
        assert models == ["H100"]

    def test_mig_model_normalization(self):
        # MIG GRES name should normalize to the base model (H100)
        output = (
            "rg01 alloc "
            "gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)\n"
        )
        _, _, models = parse_sinfo_nodes(output)
        assert models == ["H100"]

    def test_mixed_regular_and_mig_nodes(self):
        # Mix of regular H100 nodes and MIG nodes (rorqual-like)
        # Regular node: 4 GPUs; MIG node: 4 physical GPUs
        output = (
            "rg00 idle gpu:h100:4(S:0-3)\n"
            "rg01 idle "
            "gpu:nvidia_h100_80gb_hbm3_3g.40gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_2g.20gb:4(S:0-3),"
            "gpu:nvidia_h100_80gb_hbm3_1g.10gb:8(S:0-3)\n"
        )
        idle, total, models = parse_sinfo_nodes(output)
        assert total == 8   # 4 + 4
        assert idle == 8
        assert models == ["H100"]

    def test_deduplication_via_sort_u(self):
        # Same node appearing multiple times (once per Slurm partition) should
        # not inflate counts after upstream sort -u deduplication. The parser
        # itself trusts the input is already deduplicated.
        output = (
            "node01 idle gpu:h100:4\n"
            "node01 idle gpu:h100:4\n"  # duplicate that sort -u would remove
        )
        # Parser sees duplicates → double-counts; this test documents the
        # contract that sort -u must be done upstream (in _REMOTE_SCRIPT).
        # Here we just verify the format is parsed correctly for one entry.
        output_deduped = "node01 idle gpu:h100:4\n"
        idle, total, models = parse_sinfo_nodes(output_deduped)
        assert total == 4
        assert idle == 4


# ---------------------------------------------------------------------------
# parse_diskusage_report
# ---------------------------------------------------------------------------


class TestParseSavail:
    def test_total_gpus(self):
        _, total, _ = parse_savail(MILA_SAVAIL)
        # 32+136+8+16+352+376+56 = 976
        assert total == 976

    def test_idle_gpus(self):
        idle, _, _ = parse_savail(MILA_SAVAIL)
        # 15+13+0+0+10+130+5 = 173
        assert idle == 173

    def test_models_sorted(self):
        _, _, models = parse_savail(MILA_SAVAIL)
        assert models == ["A100", "A100L", "A6000", "H100", "L40S", "RTX8000", "V100"]

    def test_header_and_separator_skipped(self):
        # The "GPU  Avail / Total" header and "===" separator must not be parsed as data
        _, _, models = parse_savail(MILA_SAVAIL)
        assert "GPU" not in models
        assert "AVAIL" not in models

    def test_zero_available_still_counts_total(self):
        output = "a6000   0 / 8\nh100    0 / 16\n"
        idle, total, models = parse_savail(output)
        assert idle == 0
        assert total == 24
        assert models == ["A6000", "H100"]

    def test_empty_output(self):
        idle, total, models = parse_savail("")
        assert idle == 0
        assert total == 0
        assert models == []


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
