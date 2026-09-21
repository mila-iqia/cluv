"""Simplified 'integration' tests for the examples.

For each example, runs `sbatch --test-only` against all of its supposedly supported clusters,
with each of the supported job scripts (job.sh / safe_job.sh for example).

This is somewhat quick to run, compared to actually running the jobs.
"""

import re
import subprocess
from pathlib import Path, PurePosixPath

import pytest

from cluv.cache import read_cache
from cluv.cli.login import get_remote_without_2fa_prompt
from cluv.cli.submit import get_submissions
from cluv.cli.sync import (
    sync,
)
from cluv.config import load_cluv_config
from cluv.remote import control_socket_is_running
from tests.test_init import REPO_ROOT
from tests.test_integration import (
    IN_SELF_HOSTED_GITHUB_CI,
    REQUIRED_CLUSTERS,
)

repo_root_clusters = load_cluv_config(REPO_ROOT / "pyproject.toml").clusters_names
pytorch_example_clusters = load_cluv_config(
    REPO_ROOT / "examples" / "pytorch-example" / "pyproject.toml"
).clusters_names
hydra_example_clusters = load_cluv_config(
    REPO_ROOT / "examples" / "hydra_example" / "pyproject.toml"
).clusters_names
imagenet_example_clusters = load_cluv_config(
    REPO_ROOT / "examples" / "imagenet" / "pyproject.toml"
).clusters_names

# NOTE: Might come in handy later:
# example_root_to_clusters = {
#     REPO_ROOT: repo_root_clusters,
#     REPO_ROOT / "examples" / "pytorch-example": pytorch_example_clusters,
#     REPO_ROOT / "examples" / "hydra_example": hydra_example_clusters,
#     REPO_ROOT / "examples" / "imagenet": imagenet_example_clusters,
# }


current_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


async def skip_unless_connected(cluster: str) -> None:
    """Skip if there is no live SSH connection to `cluster`, or fail if it is in the `REQUIRED_CLUSTERS` list."""
    if await control_socket_is_running(cluster):
        return
    if IN_SELF_HOSTED_GITHUB_CI and cluster in REQUIRED_CLUSTERS:
        pytest.fail(f"No active SSH connection to {cluster}, which must be tested against!")
    pytest.skip(f"Test requires an active SSH connection to {cluster} to run.")


# TODO: enable parallelizing this test based on the cluster fixture value. Skip unsupported clusters instead.
# This can't directly be done, because the xfails are also based on the cluster value.


@pytest.mark.slow
@pytest.mark.parametrize(
    ("project_dir", "cluster", "job_script", "vram_flag"),
    [
        *[
            pytest.param(
                REPO_ROOT,
                cluster,
                job_script,
                None,
                marks=[
                    pytest.mark.xdist_group(cluster),
                    pytest.mark.xfail(
                        cluster in ("fir", "nibi"),
                        reason="TODO: Multiple _cpu allocations, and account isn't specified in pyproject of cluv root.",
                        strict=True,
                    ),
                    pytest.mark.xfail(
                        cluster in ("trillium", "trillium-gpu"),
                        reason="TODO: job output can't be in $HOME, and --mem is not allowed.",
                        strict=True,
                    ),
                    pytest.mark.xfail(
                        cluster in ("killarney",),
                        reason="TODO: Submitting jobs from directories residing in /home is not permitted.",
                        strict=True,
                    ),
                ],
            )
            for cluster in repo_root_clusters
            for job_script in (REPO_ROOT / "scripts").iterdir()
        ],
        *[
            pytest.param(
                REPO_ROOT / "examples" / "pytorch-example",
                cluster,
                job_script,
                vram,
                marks=[
                    pytest.mark.xdist_group(cluster),
                    pytest.mark.xfail(
                        cluster == "mila" and vram is not None,
                        reason=(
                            "TODO: Mila cluster doesn't have floating H100 gpus, they are in a dedicated partition. "
                            "Using --vram somehow adds --gpus=h100:1."
                        ),
                        strict=True,
                    ),
                    pytest.mark.xfail(
                        cluster == "tamia" and vram is not None,
                        reason=(
                            "TODO: Tamia requires whole-node allocation for H100s, and doesn't have MIG. "
                            "--vram adds --gpus=h100:1."
                        ),
                        strict=True,
                    ),
                    pytest.mark.xfail(
                        cluster in ("killarney", "rorqual", "fir", "nibi"),
                        reason="TODO: Multiple (_cpu) allocations, and account isn't specified in the example's pyproject file.",
                        strict=True,
                    ),
                ],
            )
            for vram in [None, "10GB"]
            for cluster in pytorch_example_clusters
            for job_script in (REPO_ROOT / "examples" / "pytorch-example" / "scripts").iterdir()
        ],
        *[
            pytest.param(
                REPO_ROOT / "examples" / "hydra_example",
                cluster,
                job_script,
                None,
                marks=[
                    pytest.mark.xdist_group(cluster),
                    pytest.mark.xfail(
                        cluster in ("fir", "nibi"),
                        reason="TODO: multiple allocations",
                        strict=True,
                    ),
                    pytest.mark.xfail(
                        cluster in ("killarney",),
                        reason="TODO: multiple allocations + Submitting jobs from directories residing in /home is not permitted.",
                        strict=True,
                    ),
                    pytest.mark.xfail(
                        cluster in ("trillium", "trillium-gpu"),
                        # SBATCH ERROR:
                        #  The --mem=... request is not allowed nor necessary on Trillium; all nodes have
                        #  the same amount of available memory (745 GiB) and each job get all the
                        #  available memory of the node
                        # SBATCH ERROR:
                        #  Job output requested to be written to file $SCRATCH/logs/cluv/%j/slurm-%j.out
                        #  in the directory /home/normandf/ which is read-only on the compute nodes.
                        #  (cluv/examples/hydra_example/scripts/safe_job.sh, line #6)
                        # SBATCH ERROR:
                        #  Job output file specification cannot use environment variables like $SCRATCH
                        #  (cluv/examples/hydra_example/scripts/safe_job.sh, line #6)
                        # SBATCH ERROR:
                        #  Walltime must be at least 15 minutes (except on the debug partition)
                        #  (cluv/examples/hydra_example/scripts/safe_job.sh, line #5)
                        reason="TODO: --mem is not allowed, and output would be written to /home which is read-only on the compute nodes.",
                        strict=True,
                    ),
                ],
            )
            for cluster in hydra_example_clusters
            for job_script in (REPO_ROOT / "examples" / "hydra_example" / "scripts").iterdir()
        ],
        *[
            pytest.param(
                REPO_ROOT / "examples" / "imagenet",
                cluster,
                None,
                None,
                marks=[
                    # Should work everywhere!
                    pytest.mark.xdist_group(cluster),
                ],
            )
            for cluster in imagenet_example_clusters
        ],
    ],
    ids=lambda param: (
        param if isinstance(param, str) else "None" if param is None else PurePosixPath(param).name
    ),
)
async def test_example_would_work(
    project_dir: Path,
    cluster: str,
    job_script: Path | None,
    vram_flag: str | None,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test that `sbatch --test-only` works for that project, cluster and (optional) job script."""
    await skip_unless_connected(cluster)
    monkeypatch.chdir(project_dir)

    # The program args passed to the example job script don't really matter, the job is never actually submitted.
    program_args = ["python", "--version"]

    # Avoid a full sync each time using the cache.
    # TODO: it seems to still be a bit slow, there are still lots of unnecessary ops even when everything
    # is already done, so we help it out a bit.
    if (
        state := read_cache().project_states.get(cluster)
    ) and state.last_uv_sync_git_commit == current_commit:
        remote = await get_remote_without_2fa_prompt(cluster)
        assert remote
    else:
        remote = (await sync([cluster], sync_datasets=False))[0]

    submissions = await get_submissions(
        cluster,
        remote=remote,
        job_script=job_script,
        sbatch_args=[],
        program_args=program_args,
        chunking=None,
        git_commit=current_commit,
        vram=vram_flag,
    )
    for submission in submissions:
        sbatch_command = submission.sbatch_command
        sbatch_command = sbatch_command.replace("--parsable", "--test-only")
        # TODO: Need to move to the project root directory as part of the sbatch command!

        print(f"Running test command: {sbatch_command}")
        result = await remote.run(sbatch_command, display=True, warn=True, hide=False)
        assert result.returncode == 0, (
            f"sbatch --test-only failed with return code {result.returncode}: {result.stderr}"
        )
        # sbatch: Job 10820990 to start at 2026-09-16T12:10:41 a using 1 processors on nodes cn-f003 in partition long-cpu
        print(result.stderr)
        assert re.findall(
            r"Job \d+ to start at .* using \d+ processors on nodes .* in partition .*",
            result.stderr,
        ), result.stderr
