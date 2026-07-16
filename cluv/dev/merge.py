import argparse
import csv
from pathlib import Path


def base_job_id(job_id: str) -> str:
    """Collapse an array task id (`<id>_<task>`) down to its parent job id."""
    parent, _, task = job_id.partition("_")
    return parent if task.isdigit() else job_id


def merge_csvs(input_dir: Path, output_path: Path) -> None:
    """Merge all `<cluster>_jobs.csv` files in `input_dir` into a single csv at `output_path`, adding a `cluster` column.

    Array job tasks (JobID `<id>_<task>`) are collapsed into a single row for `<id>`, keeping only the first task's info.
    """
    csv_paths = sorted(input_dir.glob("*_jobs.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No *_jobs.csv files found in {input_dir}")

    header: list[str] | None = None
    seen: dict[tuple[str, str], list[str]] = {}

    for csv_path in csv_paths:
        cluster = csv_path.stem.removesuffix("_jobs")
        with open(csv_path, newline="") as in_f:
            reader = csv.reader(in_f)
            file_header = next(reader)
            if header is None:
                header = file_header
            job_id_idx = file_header.index("JobID")
            for row in reader:
                row = row.copy()
                row[job_id_idx] = base_job_id(row[job_id_idx])
                seen.setdefault((cluster, row[job_id_idx]), row)

    assert header is not None
    with open(output_path, "w", newline="") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["cluster", *header, "display_name"])
        for (cluster, _), row in seen.items():
            writer.writerow([cluster, *row, ""])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge `<cluster>_jobs.csv` files in a directory into a single csv, adding a `cluster` column."
    )
    parser.add_argument("input_dir", type=Path, help="Directory containing `<cluster>_jobs.csv` files.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("merged_jobs.csv"),
        help="Path to write the merged csv to (default: merged_jobs.csv).",
    )
    args = parser.parse_args()

    merge_csvs(args.input_dir, args.output)
