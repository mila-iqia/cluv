"""Offline checks on the examples' job scripts.

These don't touch a cluster at all, so they run on every PR and cover *every* configured cluster,
not just the ones a CI runner happens to have a connection to. They catch the mistakes that would
otherwise only show up when a job actually lands on a node: a syntax error in a job script, or
`#SBATCH` directives that contradict the example's config or the way its `train.sh` launches tasks.

The complementary check that needs a cluster - "would Slurm actually accept this job script here?"
- is `test_job_scripts_are_accepted_by_slurm` in `tests/test_examples.py`, which uses
`sbatch --test-only`.
"""

import re
import subprocess
from pathlib import Path

import pytest

from cluv.config import load_cluv_config

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ["imagenet", "hydra_example"]


def _example_scripts() -> list[Path]:
    return sorted(
        script
        for example in EXAMPLES
        for script in (REPO_ROOT / "examples" / example / "scripts").glob("*.sh")
    )


def _configured_job_scripts() -> list[tuple[str, str, Path]]:
    """Every (example, cluster, job script path) triple configured in the examples."""
    triples: list[tuple[str, str, Path]] = []
    for example in EXAMPLES:
        project_dir = REPO_ROOT / "examples" / example
        config = load_cluv_config(project_dir / "pyproject.toml")
        assert config is not None
        for cluster in config.clusters_names:
            job_script_path = config.get_cluster_config(cluster).job_script_path
            if job_script_path is not None:
                triples.append((example, cluster, project_dir / job_script_path))
    return triples


def _sbatch_directives(job_script: Path) -> dict[str, str]:
    """The `#SBATCH --flag[=value]` directives in a job script's header, as a dict."""
    directives: dict[str, str] = {}
    for line in job_script.read_text().splitlines():
        if not line.strip().startswith("#SBATCH"):
            continue
        for flag in line.split()[1:]:
            name, _, value = flag.lstrip("-").partition("=")
            directives[name] = value
    return directives


def _gpu_count(gpus_per_node: str) -> int:
    """The number of GPUs in a `--gpus-per-node` value, which may name a GPU type (`l40s:2`)."""
    return int(gpus_per_node.rpartition(":")[2])


@pytest.mark.parametrize(
    "script", _example_scripts(), ids=lambda script: f"{script.parent.parent.name}/{script.name}"
)
def test_example_script_is_valid_bash(script: Path) -> None:
    """`bash -n` every script in the examples, so a syntax error can't reach a cluster."""
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("example", "cluster", "job_script"),
    _configured_job_scripts(),
    ids=lambda param: param if isinstance(param, str) else Path(param).name,
)
def test_job_script_directives_agree_with_config(example: str, cluster: str, job_script: Path):
    """The `#SBATCH` directives in each job script have to agree with the example's config.

    Note: whether a cluster *needs* an `--account` at all is cluster state, not something the
    config can be linted for (tamia, for instance, doesn't need one), so that's left to the
    `sbatch --test-only` check.

    Each of these has bitten a real submission at some point:

    - `--account` and `--time` belong in the config's `sbatch_args` (global or per-cluster), not in
      a job script: cluv merges those with the flags passed to `cluv submit`, so a value baked into
      the script silently wins over both.
    - `--output` in the header fights with the `results_path` / `--output` that cluv passes (see
      `cluv.cli.submit`, which warns about it).
    - `--gpus-per-task` breaks this example: Slurm's cgroups then give each task only its own GPU,
      so `torch.cuda.set_device(LOCAL_RANK)` fails in every task but the first. See the long note
      about it in `examples/imagenet/scripts/train.sh`.
    """
    assert job_script.exists(), f"{cluster} points at a job script that doesn't exist"
    directives = _sbatch_directives(job_script)

    for flag in ("account", "time", "output", "gpus-per-task"):
        assert flag not in directives, (
            f"{job_script.name} sets --{flag}, which it shouldn't (see this test's docstring)"
        )


@pytest.mark.parametrize(
    ("example", "cluster", "job_script"),
    [triple for triple in _configured_job_scripts() if triple[0] == "imagenet"],
    ids=lambda param: param if isinstance(param, str) else Path(param).name,
)
def test_imagenet_job_script_has_one_task_per_gpu(example: str, cluster: str, job_script: Path):
    """`scripts/train.sh` `srun`s the training script once per task, and `main.py` maps each task
    to a GPU by its local rank, so every job script has to ask for exactly as many tasks per node
    as it asks for GPUs per node.

    Getting this wrong doesn't fail the submission - the job starts, and then either leaves GPUs
    idle or crashes in `torch.cuda.set_device(LOCAL_RANK)` with "invalid device ordinal".
    """
    directives = _sbatch_directives(job_script)
    assert "gpus-per-node" in directives, f"{job_script.name} doesn't request any GPUs"
    assert "ntasks-per-node" in directives, f"{job_script.name} doesn't set --ntasks-per-node"
    assert int(directives["ntasks-per-node"]) == _gpu_count(directives["gpus-per-node"]), (
        f"{job_script.name} asks for {directives['ntasks-per-node']} tasks per node but "
        f"{directives['gpus-per-node']} GPUs per node"
    )


def test_imagenet_job_scripts_exec_the_shared_body() -> None:
    """Every per-cluster wrapper has to end up in `scripts/train.sh`, which is where all the
    actual work (code checkpointing, data staging, distributed setup) lives."""
    for example, cluster, job_script in _configured_job_scripts():
        if example != "imagenet":
            continue
        assert re.search(r"^exec bash scripts/train\.sh \"\$@\"", job_script.read_text(), re.M), (
            f"{job_script.name} ({cluster}) doesn't exec scripts/train.sh"
        )
