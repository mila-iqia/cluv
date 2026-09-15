"""Offline checks on the CI workflows that publish the ImageNet example's status badges.

Three lists have to agree and nothing at runtime makes them: the clusters the example is
configured for in `examples/imagenet/pyproject.toml` (which is what `test_imagenet_example` is
parametrized over), the `e2e-<cluster>.yaml` workflows, and the badges on
`docs/examples/imagenet-verified.md`. Each badge reads a *different* workflow's run history, so a
cluster added to one list and forgotten in the others either silently loses its badge or keeps one
pointing at a workflow that no longer runs - and a badge pointing at a workflow that never runs
renders as "no status", not as a failure. These tests are the tie.

The rest of the offline tier (the job scripts themselves) is in `test_example_scripts.py`.
"""

import re
import types
from pathlib import Path

import pytest
import yaml

from tests import test_examples
from tests.conftest import skip_means_failure

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
REUSABLE_WORKFLOW = "./.github/workflows/_examples-end-to-end.yaml"
VERIFIED_PAGE = REPO_ROOT / "docs" / "examples" / "imagenet-verified.md"
CLUSTER_INPUT = "${{ inputs.cluster }}"


def _end_to_end_clusters() -> set[str]:
    """The clusters `test_imagenet_example` actually runs against."""
    # Imported as a module, not `from ... import test_imagenet_example`: that would re-collect
    # the end-to-end test under this module too.
    for mark in test_examples.test_imagenet_example.pytestmark:
        if mark.name == "parametrize" and mark.args[0] == "cluster":
            return set(mark.args[1])
    raise AssertionError("test_imagenet_example is no longer parametrized over `cluster`.")


def _caller_workflows() -> dict[str, dict]:
    return {
        path.stem.removeprefix("e2e-"): yaml.safe_load(path.read_text())
        for path in sorted(WORKFLOWS_DIR.glob("e2e-*.yaml"))
    }


def _reusable_steps() -> list[dict]:
    workflow = yaml.safe_load((WORKFLOWS_DIR / "_examples-end-to-end.yaml").read_text())
    (job,) = workflow["jobs"].values()
    return job["steps"]


def test_reusable_workflow_runs_only_its_own_cluster() -> None:
    """The run has to select one parametrized case by node id, not the `end_to_end` marker.

    `test_imagenet_example` is parametrized over every cluster the example supports, so selecting
    by marker here would make each of the nine workflows submit a real GPU job to all nine
    clusters. Pinning the node id is the only thing keeping a run to its own cluster, and it is
    one careless edit away from being a marker again.
    """
    commands = [step["run"] for step in _reusable_steps() if "run" in step]
    pytest_commands = [c for c in commands if "pytest" in c]
    assert len(pytest_commands) == 1, "Expected exactly one pytest invocation."
    (command,) = pytest_commands
    assert f"test_imagenet_example[{CLUSTER_INPUT}]" in command, (
        f"The end-to-end run must select one cluster by node id, but runs:\n{command}"
    )
    assert " -m " not in command, (
        f"Selecting by marker would fan this run out to every cluster:\n{command}"
    )


def test_leftover_jobs_are_swept_only_on_its_own_cluster() -> None:
    """An unscoped sweep would cancel a concurrent sibling run's job mid-training."""
    commands = [step["run"] for step in _reusable_steps() if "run" in step]
    (sweep,) = [c for c in commands if "cancel_ci_jobs.py" in c]
    assert f"--cluster {CLUSTER_INPUT}" in sweep, sweep


def test_every_end_to_end_cluster_has_its_own_workflow() -> None:
    assert set(_caller_workflows()) == _end_to_end_clusters()


@pytest.mark.parametrize("cluster", sorted(_caller_workflows()))
def test_workflow_calls_the_reusable_one_with_its_own_cluster(cluster: str) -> None:
    """A copy-pasted workflow that forgot to change its `cluster` would test the wrong cluster
    twice and leave the other one with a badge that is green for someone else's run."""
    jobs = _caller_workflows()[cluster]["jobs"]
    assert len(jobs) == 1, f"e2e-{cluster}.yaml should call the reusable workflow exactly once."
    (job,) = jobs.values()
    assert job["uses"] == REUSABLE_WORKFLOW
    assert job["with"]["cluster"] == cluster


@pytest.mark.parametrize("cluster", sorted(_caller_workflows()))
def test_verified_page_has_a_badge_pointing_at_the_workflow(cluster: str) -> None:
    page = VERIFIED_PAGE.read_text()
    assert f"e2e-{cluster}.yaml?branch=master&label={cluster}" in page, (
        f"No status badge for {cluster} on {VERIFIED_PAGE.relative_to(REPO_ROOT)}."
    )
    assert f"actions/workflows/e2e-{cluster}.yaml" in page, (
        f"{cluster}'s badge doesn't link to its run history."
    )


def test_verified_page_has_no_badge_for_an_untested_cluster() -> None:
    """The page is hand-written now, so a badge can outlive the workflow it reads."""
    badged = set(re.findall(r"actions/workflows/e2e-([\w.-]+)\.yaml", VERIFIED_PAGE.read_text()))
    assert badged == _end_to_end_clusters()


@pytest.mark.parametrize(
    ("report", "dedicated_cluster", "expected"),
    [
        (types.SimpleNamespace(skipped=True), "mila", True),
        # Outside a dedicated run a skip is just a skip: on a dev machine most clusters are.
        (types.SimpleNamespace(skipped=True), None, False),
        (types.SimpleNamespace(skipped=False), "mila", False),
        # An xfail reports as a skip, but it is an expectation, not an unanswered question.
        (types.SimpleNamespace(skipped=True, wasxfail="slow queue"), "mila", False),
    ],
)
def test_skip_means_failure(report, dedicated_cluster: str | None, expected: bool) -> None:
    assert skip_means_failure(report, dedicated_cluster) is expected
