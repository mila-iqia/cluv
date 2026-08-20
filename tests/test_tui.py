import asyncio
import io

import pytest
import rich.console
import rich.table

from cluv.tui import JobWaitProgress, LiveRegistry, shorten_labels


class TestShortenLabels:
    def test_labels_under_threshold_are_unchanged(self) -> None:
        labels = ["python main.py lr=0.1", "python main.py lr=0.2"]
        assert shorten_labels(labels, max_len=40) == labels

    def test_single_label_is_unaffected_by_common_prefix_logic(self) -> None:
        assert shorten_labels(["onlyone_is_a_very_long_single_label_here"], max_len=10) == [
            "onlyone_i…"
        ]

    def test_shared_prefix_is_shortened_keeping_differentiating_suffix(self) -> None:
        labels = ["python main.py lr=0.1", "python main.py lr=0.2"]
        result = shorten_labels(labels, max_len=20)
        assert result == ["python (...) lr=0.1", "python (...) lr=0.2"]
        assert all(len(r) <= 20 for r in result)

    def test_no_common_prefix_falls_back_to_plain_truncation(self) -> None:
        labels = [
            "completely unrelated first one is long",
            "second one is also quite long indeed",
        ]
        result = shorten_labels(labels, max_len=10)
        assert result == ["completel…", "second on…"]

    def test_never_collapses_distinct_labels_into_duplicates(self) -> None:
        """Regression test: an earlier version of the truncation step could chop off the
        differentiating suffix, making two distinct labels render identically."""
        labels = ["totally different a", "totally different b"]
        result = shorten_labels(labels, max_len=5)
        assert result[0] != result[1]

    def test_extremely_tight_budget_still_prefers_suffix(self) -> None:
        labels = ["run job with prefix suffix_a", "run job with prefix suffix_b"]
        result = shorten_labels(labels, max_len=8)
        assert result[0] != result[1]


@pytest.fixture
def registry() -> LiveRegistry:
    console = rich.console.Console(file=io.StringIO(), force_terminal=True)
    return LiveRegistry(console)


class TestLiveRegistry:
    async def test_single_registrant_renders_plain_table_without_job_column(
        self, registry: LiveRegistry
    ) -> None:
        progress = JobWaitProgress(
            label="python main.py", cancelling=False, rows=[("mila", 123, "PENDING")]
        )
        async with registry.section(progress):
            table = registry._render()
            assert isinstance(table, rich.table.Table)
            assert [c.header for c in table.columns] == ["Cluster", "Job ID", "Status"]
            assert table.title == "Waiting for a job to start..."
        # Registry stops the shared Live once the last registrant exits.
        assert registry._live is None
        assert registry._entries == {}

    async def test_concurrent_registrants_are_fused_into_one_table_with_job_column(
        self, registry: LiveRegistry
    ) -> None:
        progress_a = JobWaitProgress(
            label="python main.py lr=0.1", cancelling=False, rows=[("mila", 1, "PENDING")]
        )
        progress_b = JobWaitProgress(
            label="python main.py lr=0.2", cancelling=False, rows=[("narval", 2, "RUNNING")]
        )
        async with registry.section(progress_a):
            async with registry.section(progress_b):
                table = registry._render()
                assert [c.header for c in table.columns] == [
                    "Job",
                    "Cluster",
                    "Job ID",
                    "Status",
                ]
            # Back down to a single registrant: the plain table shape returns.
            table = registry._render()
            assert [c.header for c in table.columns] == ["Cluster", "Job ID", "Status"]
        assert registry._live is None
        assert registry._entries == {}

    async def test_entry_is_removed_even_if_the_section_body_raises(
        self, registry: LiveRegistry
    ) -> None:
        progress = JobWaitProgress(label="doomed job", cancelling=False, rows=[])
        with pytest.raises(RuntimeError):
            async with registry.section(progress):
                raise RuntimeError("boom")
        assert registry._entries == {}
        assert registry._live is None

    async def test_entry_is_removed_on_cancellation(self, registry: LiveRegistry) -> None:
        progress = JobWaitProgress(label="cancelled job", cancelling=False, rows=[])

        async def _run() -> None:
            async with registry.section(progress):
                await asyncio.sleep(10)

        task = asyncio.ensure_future(_run())
        await asyncio.sleep(0)  # let the task enter the section
        assert registry._entries != {}
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert registry._entries == {}
        assert registry._live is None
