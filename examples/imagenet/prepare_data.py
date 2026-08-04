"""Dataset preprocessing script.

Extracts the ImageNet archives onto the local disk of each node ($SLURM_TMPDIR), which is much
faster to read from during training than the shared filesystem.

Run this with `srun --ntasks-per-node=1 --pty uv run python prepare_data.py`

The archives are read from the `datasets_path` of the `[tool.cluv]` config for the current cluster.
On the cluster listed in `data_source` (the Mila cluster here), this is the shared dataset folder
itself, so nothing needs to be copied. On the other clusters, this is where `cluv sync` replicated
the archives.
"""

import argparse
import datetime
import os
from pathlib import Path
from typing import Literal

from cluv.job import get_datasets_path
from torchvision.datasets import ImageNet

SLURM_TMPDIR = Path(os.environ.get("SLURM_TMPDIR", "/tmp"))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=SLURM_TMPDIR / "data",
        help="Where to prepare the dataset.",
    )
    parser.add_argument(
        "--imagenet-dir",
        type=Path,
        default=get_datasets_path(),
        help="The path to the folder containing the ILSVRC2012 train and val archives and devkit.",
    )
    args = parser.parse_args()
    dest: Path = args.dest
    # to see it as soon as it happens in logs.
    # `srun` can keep output in a buffer for quite a while otherwise.
    print(f"Preparing ImageNet dataset in {dest}", flush=True)
    _, _ = prepare_imagenet(dest, imagenet_dir=args.imagenet_dir)
    print(f"Done preparing ImageNet dataset in {dest}")


def prepare_imagenet(output_directory: Path, imagenet_dir: Path | None = None):
    imagenet_dir = imagenet_dir or get_datasets_path()
    if imagenet_dir is None:
        raise RuntimeError(
            "A `datasets_path` must be set in the [tool.cluv] config (or --imagenet-dir passed) "
            "so we know where to read the ImageNet archives from."
        )
    devkit_archive = imagenet_dir / "ILSVRC2012_devkit_t12.tar.gz"
    train_archive = imagenet_dir / "ILSVRC2012_img_train.tar"
    val_archive = imagenet_dir / "ILSVRC2012_img_val.tar"
    checksums_file = imagenet_dir / "md5sums"
    if missing := [
        str(p)
        for p in (imagenet_dir, devkit_archive, train_archive, val_archive, checksums_file)
        if not p.exists()
    ]:
        raise FileNotFoundError(
            f"Could not find the ImageNet dataset archives at {imagenet_dir}: "
            f"{', '.join(missing)} do not exist.\n"
            f"Run `cluv sync` to replicate the archives from the `data_source` cluster to this "
            f"cluster, or pass `--use_fake_data` to main.py to run without the real dataset."
        )
    output_directory.mkdir(parents=True, exist_ok=True)

    _make_symlink_in_dest(devkit_archive, output_directory)
    _make_symlink_in_dest(train_archive, output_directory)
    _make_symlink_in_dest(val_archive, output_directory)
    _make_symlink_in_dest(checksums_file, output_directory)

    train_dataset = _make_split(output_directory, "train")
    test_dataset = _make_split(output_directory, "val")
    return train_dataset, test_dataset


def _make_symlink_in_dest(file: Path, dest_dir: Path):
    if not (symlink_to_file := (dest_dir / file.name)).exists():
        symlink_to_file.symlink_to(file)
    return symlink_to_file


def _make_split(root: Path, split: Literal["train", "val"]):
    """Use the torchvision.datasets.ImageNet class constructor to prepare the data.

    There are faster ways of doing this with the `tarfile` package or fancy bash
    commands but this is simplest.
    """
    print(f"Preparing ImageNet {split} split in {root}", flush=True)
    t = datetime.datetime.now()
    d = ImageNet(root=str(root), split=split)
    print(f"Preparing ImageNet {split} split took {datetime.datetime.now() - t}")
    return d


if __name__ == "__main__":
    main()
