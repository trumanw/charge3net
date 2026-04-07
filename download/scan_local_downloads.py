# Copyright (c) 2023, MASSACHUSETTS INSTITUTE OF TECHNOLOGY
# Subject to FAR 52.227-11 - Patent Rights - Ownership by the Contractor (May 2014).
"""Scan a local mp_raw directory and save a list of all successfully downloaded mpids.

An mpid is considered downloaded if its subdirectory contains a CHGCAR file.
The output file can be passed to download_materials_project_aliyun.py via
--downloaded_list to skip already-uploaded entries.

Usage:
    python download/scan_local_downloads.py \\
        --mp_raw_dir ./data/mp_raw \\
        --out_file ./data/downloaded_mpids.txt
"""
from pathlib import Path
import argparse


parser = argparse.ArgumentParser(
    description="Scan local mp_raw directory and save list of downloaded mpids."
)
parser.add_argument(
    "--mp_raw_dir",
    type=str,
    default="./data/mp_raw",
    help="Root directory containing per-mpid subdirectories (default: ./data/mp_raw)",
)
parser.add_argument(
    "--out_file",
    type=str,
    default="./data/downloaded_mpids.txt",
    help="Output file path (default: ./data/downloaded_mpids.txt)",
)
parser.add_argument(
    "--require_task_id",
    action="store_true",
    help="Only include mpids that also have a task_id.txt file",
)


def scan_downloads(mp_raw_dir: str, require_task_id: bool = False) -> list:
    """Return sorted list of mpids that have a complete CHGCAR download."""
    root = Path(mp_raw_dir)
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")

    completed = []
    missing_task_id = []

    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        has_chgcar = (d / "CHGCAR").exists()
        has_task_id = (d / "task_id.txt").exists()

        if not has_chgcar:
            continue

        if require_task_id and not has_task_id:
            missing_task_id.append(d.name)
            continue

        completed.append(d.name)

    if missing_task_id:
        print(
            f"  Skipped {len(missing_task_id)} mpid(s) with CHGCAR but no task_id.txt "
            f"(use without --require_task_id to include them)"
        )

    return completed


def main(args):
    print(f"Scanning {args.mp_raw_dir} ...")
    mpids = scan_downloads(args.mp_raw_dir, require_task_id=args.require_task_id)

    out_path = Path(args.out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w") as f:
        for mpid in mpids:
            f.write(f"{mpid}\n")

    print(f"Found {len(mpids)} downloaded mpid(s).")
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
