"""Support for the `--vram` flag of `cluv submit`.

Asking for an amount of VRAM instead of a GPU model lets the job run on any GPU that is big
enough, in particular on the MIG slices of the DRAC clusters, which tend to be idle. One job
is submitted per compatible GPU type and the first one to start is kept (see
`cluv.cli.submit.expand_for_vram`).

The GPU types are not hard-coded: they are read from `sinfo` on each cluster (and cached), so
that new GPU models and MIG profiles are picked up automatically.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from cluv.cache import get_cached_gpu_types, save_gpu_types
from cluv.config import SbatchArgs
from cluv.remote import Remote, run
from cluv.sbatch_args import sbatch_args_from_list, sbatch_args_to_list
from cluv.slurm import GRES_RE, gpu_base_model
from cluv.utils import console

logger = logging.getLogger(__name__)

GPU_TYPES_COMMAND = "sinfo --noheader --format='%f|%G' | sort -u"
"""Command used to list the GPU types (including MIG slices) available on a cluster.

`%G` gives the GRES of each node (e.g. `gpu:nvidia_h100_80gb_hbm3_1g.10gb:8`) and `%f` its
features, which on some clusters (e.g. Mila) say how much VRAM the GPUs have (e.g. `80gb`).
"""

VRAM_GB_BY_MODEL: dict[str, float] = {
    # Last-resort fallback, for the GPU types whose name and node features don't say how much
    # VRAM they have (e.g. "h100" on the Fir, Rorqual and Killarney clusters).
    "a100": 40,
    "a100l": 80,
    "a5000": 24,
    "a6000": 48,
    "h100": 80,
    "mi300a": 128,
    "h200": 141,
    "l40s": 48,
    "p100": 12,
    "p100l": 16,
    "rtx8000": 48,
    "t4": 16,
    "v100": 16,
    "v100l": 32,
}

# GPU count flags, in the "--gpus"-like family (value is "[<type>:]<count>").
_GPU_COUNT_FLAGS = ("--gpus", "--gpus-per-node", "--gpus-per-task", "--gpus-per-socket", "-G")
# The --gres flag, whose value is "gpu[:<type>]:<count>" (possibly among other resources).
_GRES_FLAG = "--gres"

# A MIG profile, like "3g.40gb" in "nvidia_h100_80gb_hbm3_3g.40gb": the VRAM is the second number.
_MIG_PROFILE_RE = re.compile(r"(\d+)g\.(\d+)gb", re.IGNORECASE)
# An amount of memory in a GPU type name or in a node feature, e.g. "80gb".
_MEMORY_RE = re.compile(r"(\d+)\s*gb", re.IGNORECASE)
# A VRAM amount as passed to `--vram`, like "10G", "24GB", "10.5GiB" or "20480MB".
_VRAM_RE = re.compile(r"^\s*([\d.]+)\s*(?:([kmgt])(?:i?b)?)?\s*$", re.IGNORECASE)

_UNIT_TO_GB: dict[str, float] = {"k": 1 / 1024**2, "m": 1 / 1024, "g": 1.0, "t": 1024.0}


async def expand_for_vram(
    cluster: str,
    remote: Remote | None,
    sbatch_args: SbatchArgs,
    *,
    job_script: Path,
    vram: str | None,
) -> list[SbatchArgs]:
    """Turn one set of sbatch args into one per GPU type of `cluster` that has enough VRAM.

    Racing between the GPU types that are big enough (in particular the MIG slices of the DRAC
    clusters, which are under-used) makes the job start sooner. When a GPU model is already
    requested (e.g. `--gpus=h100:1`), only that model and its MIG slices are considered.

    Returns `[sbatch_args]` unchanged when `vram` isn't set, when the job asks for more than one
    GPU (MIG slices can't be used for multi-GPU jobs), or when no GPU type on `cluster` has
    enough VRAM.
    """
    if not vram:
        return [sbatch_args]

    sbatch_args_list = sbatch_args_to_list(sbatch_args)
    gpu_request = find_gpu_request(sbatch_args_list, job_script)
    if gpu_request and gpu_request.count > 1:
        console.print(
            f"[yellow]Ignoring --vram on {cluster}: the job asks for {gpu_request.count} GPUs, "
            "and MIG slices can only be used one at a time.[/yellow]"
        )
        return [sbatch_args]

    gpu_request = gpu_request or GpuRequest()
    gpu_types = compatible_gpu_types(
        await get_gpu_types(cluster, remote), parse_vram(vram), gpu_request.model
    )
    if not gpu_types:
        console.print(
            f"[yellow]Ignoring --vram on {cluster}: no GPU type with at least {vram} of VRAM"
            + (f" for the {gpu_request.model} model" if gpu_request.model else "")
            + ".[/yellow]"
        )
        return [sbatch_args]

    logger.info("GPU types with at least %s of VRAM on %s: %s", vram, cluster, gpu_types)
    return [
        sbatch_args_from_list(sbatch_args_for_gpu_type(sbatch_args_list, gpu_request, gpu_type))
        for gpu_type in gpu_types
    ]


def parse_vram(value: str) -> float:
    """Parse a VRAM amount like "10GB", "10G", "20480M" or "10" (GB by default) into GB.

    >>> parse_vram("10GB"), parse_vram("10G"), parse_vram("10")
    (10.0, 10.0, 10.0)
    >>> parse_vram("20480MB")
    20.0
    """
    match = _VRAM_RE.match(value)
    if not match:
        raise ValueError(
            f"Invalid --vram value: {value!r}. Expected something like '10GB', '24G' or '40'."
        )
    amount, unit = float(match.group(1)), (match.group(2) or "g").lower()
    return amount * _UNIT_TO_GB[unit]


def gpu_vram_gb(gpu_type: str, node_features: str = "") -> float | None:
    """Return how much VRAM (in GB) a GPU type has, or None if we can't tell.

    The VRAM is read from the GPU type name when it contains it (MIG slices always do), then
    from the node features, and is finally looked up in `VRAM_GB_BY_MODEL`.

    >>> gpu_vram_gb("nvidia_h100_80gb_hbm3_1g.10gb"), gpu_vram_gb("a100_4g.20gb")
    (10.0, 20.0)
    >>> gpu_vram_gb("v100", "x86_64,volta,nvlink,dgx,32gb"), gpu_vram_gb("v100")
    (32.0, 16.0)
    >>> gpu_vram_gb("some_new_gpu") is None
    True
    """
    if mig_profile := _MIG_PROFILE_RE.search(gpu_type):
        return float(mig_profile.group(2))
    if memory_in_name := _MEMORY_RE.search(gpu_type):
        return float(memory_in_name.group(1))
    if memory_in_features := _MEMORY_RE.search(node_features):
        return float(memory_in_features.group(1))
    known_vram = VRAM_GB_BY_MODEL.get(gpu_base_model(gpu_type))
    return float(known_vram) if known_vram is not None else None


def parse_gpu_types(output: str) -> dict[str, float | None]:
    """Parse the output of `GPU_TYPES_COMMAND` into a `{gpu_type: vram_in_gb}` mapping.

    The GPU types are the raw Slurm GRES names, since they are what has to be passed back to
    `sbatch` (e.g. `--gpus=nvidia_h100_80gb_hbm3_1g.10gb:1`). The VRAM is `None` for the GPU
    types we don't know anything about.

    When the same GPU type is found with different VRAM amounts, the smallest one is used, so
    that a job asking for `--vram` always gets at least what it asked for.

    >>> parse_gpu_types("h100mig|gpu:nvidia_h100_80gb_hbm3_1g.10gb:8\\nh100|gpu:h100:4(S:0)")
    {'h100': 80.0, 'nvidia_h100_80gb_hbm3_1g.10gb': 10.0}
    """
    gpu_types: dict[str, float | None] = {}
    for line in output.splitlines():
        node_features, _, gres_field = line.rpartition("|")
        for gpu_type, _count in GRES_RE.findall(gres_field):
            vram = gpu_vram_gb(gpu_type, node_features)
            previous = gpu_types.get(gpu_type)
            if gpu_type not in gpu_types or vram is None or previous is None:
                gpu_types[gpu_type] = vram
            else:
                gpu_types[gpu_type] = min(previous, vram)
    return dict(sorted(gpu_types.items()))


async def get_gpu_types(cluster: str, remote: Remote | None) -> dict[str, float | None]:
    """Return the GPU types available on a cluster, using the cached values when possible."""
    if (cached := get_cached_gpu_types(cluster)) is not None:
        logger.debug("Using the cached GPU types of %s: %s", cluster, cached)
        return cached

    command = f"bash -l -c {GPU_TYPES_COMMAND!r}"
    if remote:
        output = await remote.get_output(command, hide=True, warn=True, display=False)
    else:
        output = (await run(("bash", "-l", "-c", GPU_TYPES_COMMAND), hide=True, warn=True)).stdout

    gpu_types = parse_gpu_types(output)
    logger.info("GPU types available on %s: %s", cluster, gpu_types)
    if gpu_types:
        save_gpu_types(cluster, gpu_types)
    return gpu_types


def compatible_gpu_types(
    gpu_types: dict[str, float | None], vram_gb: float, model: str | None = None
) -> list[str]:
    """Return the GPU types with at least `vram_gb` of VRAM, smallest (i.e. easiest to get) first.

    When a GPU `model` is requested, only that model and its MIG slices are considered.

    >>> gpu_types = {"h100": 80.0, "h100_1g.10gb": 10.0, "h100_3g.40gb": 40.0, "l40s": 48.0}
    >>> compatible_gpu_types(gpu_types, vram_gb=10)
    ['h100_1g.10gb', 'h100_3g.40gb', 'l40s', 'h100']
    >>> compatible_gpu_types(gpu_types, vram_gb=10, model="h100")
    ['h100_1g.10gb', 'h100_3g.40gb', 'h100']
    >>> compatible_gpu_types(gpu_types, vram_gb=48, model="h100")
    ['h100']
    """
    compatible = {
        gpu_type: vram
        for gpu_type, vram in gpu_types.items()
        if vram is not None
        and vram >= vram_gb
        and (model is None or gpu_base_model(gpu_type) == gpu_base_model(model))
    }
    return sorted(compatible, key=lambda gpu_type: (compatible[gpu_type], gpu_type))


@dataclass(frozen=True)
class GpuRequest:
    """The GPUs requested by a job, as found in the sbatch args or the job script header."""

    flag: str = "--gpus"
    """The flag used to request the GPUs, e.g. "--gpus", "--gpus-per-node" or "--gres"."""

    model: str | None = None
    """The requested GPU model, if any, e.g. "h100" in `--gpus=h100:1`."""

    count: int = 1
    """The number of GPUs requested."""

    at: int | None = None
    """Index of the flag in the sbatch args, or None when it comes from the job script header."""

    n_tokens: int = 1
    """Number of sbatch args tokens taken up by the flag ("--gpus=1" vs "-G 1")."""

    def with_gpu_type(self, gpu_type: str) -> list[str]:
        """Return the sbatch flag that requests `count` GPUs of the given type.

        >>> GpuRequest().with_gpu_type("h100_1g.10gb")
        ['--gpus=h100_1g.10gb:1']
        >>> GpuRequest(flag="--gres", count=2).with_gpu_type("h100")
        ['--gres=gpu:h100:2']
        >>> GpuRequest(flag="-G").with_gpu_type("h100")
        ['-G', 'h100:1']
        """
        value = f"{gpu_type}:{self.count}"
        if self.flag == _GRES_FLAG:
            value = f"gpu:{value}"
        if not self.flag.startswith("--"):
            # `sbatch` doesn't accept "-G=<value>", the value has to be a separate argument.
            return [self.flag, value]
        return [f"{self.flag}={value}"]


def _parse_gpu_flag(flag: str, value: str) -> tuple[str | None, int] | None:
    """Return the (model, count) requested by a GPU flag, or None if it isn't a GPU request."""
    if flag not in (*_GPU_COUNT_FLAGS, _GRES_FLAG):
        return None
    if flag == _GRES_FLAG:
        # Can be a comma-separated list of resources, e.g. "gpu:h100:1,tmpfs:10G".
        gpu_gres = next((v for v in value.split(",") if v.startswith("gpu")), "")
        if not gpu_gres:
            return None
        value = gpu_gres.removeprefix("gpu").lstrip(":")
    # What's left is "<count>" or "<model>:<count>".
    model, _, count = value.rpartition(":")
    if not count.isdigit():
        return None
    return (model or None), int(count)


def find_gpu_request(sbatch_args: list[str], job_script: Path | None = None) -> GpuRequest | None:
    """Find how many GPUs (and of which model) the job asks for.

    The sbatch args (from the command-line and from the cluv config) take precedence over the
    `#SBATCH` directives of the job script header, just like they do for `sbatch` itself.

    >>> find_gpu_request(["--time=1:00:00", "--gpus=h100:1"])
    GpuRequest(flag='--gpus', model='h100', count=1, at=1, n_tokens=1)
    >>> find_gpu_request(["-G", "2"])
    GpuRequest(flag='-G', model=None, count=2, at=0, n_tokens=2)
    >>> find_gpu_request(["--time=1:00:00"]) is None
    True
    """
    for index, arg in enumerate(sbatch_args):
        flag, sep, value = arg.partition("=")
        n_tokens = 1
        if not sep and index + 1 < len(sbatch_args):
            # A flag whose value is a separate token, e.g. ["-G", "1"].
            value = sbatch_args[index + 1]
            n_tokens = 2
        if (parsed := _parse_gpu_flag(flag, value)) is not None:
            model, count = parsed
            return GpuRequest(flag=flag, model=model, count=count, at=index, n_tokens=n_tokens)

    if job_script is None:
        return None

    for line in job_script.read_text().splitlines():
        if not line.strip().startswith("#"):
            break  # Stop parsing once we leave the header.
        if not line.startswith("#SBATCH"):
            continue
        for token in line.removeprefix("#SBATCH").split():
            flag, _, value = token.partition("=")
            if (parsed := _parse_gpu_flag(flag, value)) is not None:
                model, count = parsed
                return GpuRequest(flag=flag, model=model, count=count)

    return None


def sbatch_args_for_gpu_type(
    sbatch_args: list[str], gpu_request: GpuRequest, gpu_type: str
) -> list[str]:
    """Return `sbatch_args` with the GPU request replaced by one for the given GPU type.

    >>> sbatch_args_for_gpu_type(["--gpus=1", "--time=1:00:00"], GpuRequest(at=0), "h100")
    ['--gpus=h100:1', '--time=1:00:00']
    >>> sbatch_args_for_gpu_type(["--time=1:00:00"], GpuRequest(), "h100")
    ['--time=1:00:00', '--gpus=h100:1']
    """
    new_flag = gpu_request.with_gpu_type(gpu_type)
    if gpu_request.at is None:
        # The request comes from the job script header (or there is no GPU request at all):
        # passing the flag on the command-line overrides the `#SBATCH` directive.
        return [*sbatch_args, *new_flag]
    return [
        *sbatch_args[: gpu_request.at],
        *new_flag,
        *sbatch_args[gpu_request.at + gpu_request.n_tokens :],
    ]
