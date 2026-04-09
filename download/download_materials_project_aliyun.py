# Copyright (c) 2023, MASSACHUSETTS INSTITUTE OF TECHNOLOGY
# Subject to FAR 52.227-11 - Patent Rights - Ownership by the Contractor (May 2014).
"""Download Materials Project charge densities and upload directly to Aliyun OSS.

Designed to run on an Aliyun ECS server where local disk space is limited.
Each CHGCAR is written to a temporary file, uploaded to OSS, then deleted locally.

OSS key layout (flat):
    {prefix}mp-10/CHGCAR
    {prefix}mp-10/task_id.txt
    (prefix from --oss_prefix, --oss_key, or --oss_folder, e.g. mp_raw/)

Typical usage with a pre-built mpid->task_id map:

    python download/download_materials_project_aliyun.py \\
        --task_id_file ./data/mpid_to_task_id_map.json \\
        --mp_api_key YOUR_MP_API_KEY \\
        --oss_bucket my-bucket \\
        --oss_endpoint oss-cn-hangzhou.aliyuncs.com \\
        --oss_access_key_id YOUR_KEY_ID \\
        --oss_access_key_secret YOUR_KEY_SECRET \\
        --downloaded_list ./data/downloaded_mpids.txt \\
        --workers 4

Generate the skip list first with:

    python download/scan_local_downloads.py \\
        --mp_raw_dir ./data/mp_raw \\
        --out_file ./data/downloaded_mpids.txt
"""
import argparse
import json
import os
import tempfile
from multiprocessing import pool
from pathlib import Path
from typing import Any, Optional, Set, Tuple

import oss2
from mp_api.client import MPRester
from pymatgen.io.vasp import Chgcar


parser = argparse.ArgumentParser(
    description="Download MP charge densities and upload to Aliyun OSS."
)

# --- Materials Project args ---
parser.add_argument("--mp_api_key", type=str, required=True,
    help="API key from the Materials Project")
parser.add_argument("--task_id_file", type=str, default=None,
    help="(optional) JSON file mapping material_id -> task_id")
parser.add_argument("--download_latest_for_missing_task_id", action="store_true",
    help="Fall back to latest calculation when a task_id lookup fails")
parser.add_argument("--limit", type=int, default=0,
    help="Limit number of mpids to process (for debugging)")
parser.add_argument("--workers", type=int, default=1,
    help="Number of parallel download workers (max 5)")

# --- Skip list ---
parser.add_argument("--downloaded_list", type=str, default=None,
    help="Path to a file listing mpids to skip. Supports .txt (one mpid per line) "
         "or .json (object with mpids as keys). Generate with scan_local_downloads.py.")

# --- Aliyun OSS args ---
parser.add_argument("--oss_bucket", type=str, required=True,
    help="OSS bucket name")
parser.add_argument("--oss_endpoint", type=str, required=True,
    help="OSS endpoint, e.g. oss-cn-hangzhou.aliyuncs.com")
parser.add_argument("--oss_access_key_id", type=str,
    default=None,
    help="Aliyun access key ID (falls back to env var OSS_ACCESS_KEY_ID)")
parser.add_argument("--oss_access_key_secret", type=str,
    default=None,
    help="Aliyun access key secret (falls back to env var OSS_ACCESS_KEY_SECRET)")
parser.add_argument(
    "--oss_prefix",
    "--oss_key",
    "--oss_folder",
    dest="oss_prefix",
    type=str,
    default="",
    help="Optional object key prefix (folder) inside the bucket, e.g. 'mp_raw/'. "
         "Use --oss_key or --oss_folder for the same option. (default: '')",
)


# ---------------------------------------------------------------------------
# OSS helpers
# ---------------------------------------------------------------------------

def init_oss_bucket(
    bucket_name: str,
    endpoint: str,
    access_key_id: Optional[str] = None,
    access_key_secret: Optional[str] = None,
) -> oss2.Bucket:
    """Return an authenticated oss2.Bucket instance."""
    key_id = access_key_id or os.environ.get("OSS_ACCESS_KEY_ID")
    key_secret = access_key_secret or os.environ.get("OSS_ACCESS_KEY_SECRET")
    if not key_id or not key_secret:
        raise ValueError(
            "OSS credentials not provided. Pass --oss_access_key_id / "
            "--oss_access_key_secret or set OSS_ACCESS_KEY_ID / "
            "OSS_ACCESS_KEY_SECRET environment variables."
        )
    auth = oss2.Auth(key_id, key_secret)
    return oss2.Bucket(auth, f"https://{endpoint}", bucket_name)


def _oss_key(prefix: str, mpid: str, filename: str) -> str:
    """Build an OSS object key: {prefix}{mpid}/{filename}."""
    return f"{prefix}{mpid}/{filename}"


def object_exists(bucket: oss2.Bucket, key: str) -> bool:
    """Return True if the OSS object already exists."""
    return bucket.object_exists(key)


def upload_chgcar_to_oss(
    bucket: oss2.Bucket,
    chgcar: Chgcar,
    mpid: str,
    prefix: str = "",
) -> None:
    """Write CHGCAR to a temp file and upload to OSS, then delete temp file."""
    key = _oss_key(prefix, mpid, "CHGCAR")
    with tempfile.NamedTemporaryFile(suffix=".CHGCAR", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        chgcar.write_file(tmp_path)
        bucket.put_object_from_file(key, tmp_path)
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def upload_task_id_to_oss(
    bucket: oss2.Bucket,
    taskdoc: Any,
    mpid: str,
    prefix: str = "",
) -> None:
    """Upload the task_id string as a small text object to OSS."""
    key = _oss_key(prefix, mpid, "task_id.txt")
    content = f"{taskdoc.task_id}\n"
    bucket.put_object(key, content.encode("utf-8"))


# ---------------------------------------------------------------------------
# Materials Project fetch helpers (mirrored from download_materials_project.py)
# ---------------------------------------------------------------------------

def _get_charge_density_with_task_docs(
    mp_api_key: str, mpid: str, deserialize: bool = False
) -> Tuple[Optional[Chgcar], Optional[Any]]:
    with MPRester(mp_api_key, monty_decode=deserialize) as mpr:
        chgcar, taskdoc = mpr.get_charge_density_from_material_id(
            mpid, inc_task_doc=True
        )
    return chgcar, taskdoc


def _get_charge_density_by_task_id(
    mp_api_key: str, task_id: str, deserialize: bool = False
) -> Tuple[Optional[Chgcar], Optional[Any]]:
    with MPRester(mp_api_key, monty_decode=deserialize) as mpr:
        chgcar, taskdoc = mpr.get_charge_density_from_task_id(
            task_id, inc_task_doc=True
        )
    return chgcar, taskdoc


def _get_all_mpids_with_charge_density(mp_api_key: str) -> list:
    with MPRester(mp_api_key) as mpr:
        docs = mpr.materials.summary.search(
            has_props=["charge_density"], fields=["material_id"]
        )
    return [doc.material_id for doc in docs]


# ---------------------------------------------------------------------------
# Worker functions
# ---------------------------------------------------------------------------

def _fetch_and_upload(
    mp_api_key: str,
    mpid: str,
    bucket_name: str,
    endpoint: str,
    access_key_id: str,
    access_key_secret: str,
    oss_prefix: str,
) -> None:
    """Fetch by material_id and upload to OSS. Used when no task_id is available."""
    bucket = init_oss_bucket(bucket_name, endpoint, access_key_id, access_key_secret)
    try:
        chgcar, taskdoc = _get_charge_density_with_task_docs(
            mp_api_key, mpid, deserialize=True
        )
    except Exception as e:
        print(f"[{mpid}] fetch error: {e}")
        return

    if chgcar is not None:
        try:
            upload_chgcar_to_oss(bucket, chgcar, mpid, oss_prefix)
            print(f"[{mpid}] CHGCAR uploaded.")
        except Exception as e:
            print(f"[{mpid}] CHGCAR upload error: {e}")

    if taskdoc is not None:
        try:
            upload_task_id_to_oss(bucket, taskdoc, mpid, oss_prefix)
        except Exception as e:
            print(f"[{mpid}] task_id upload error: {e}")


def _fetch_and_upload_by_task(
    mp_api_key: str,
    mpid: str,
    task_id: str,
    bucket_name: str,
    endpoint: str,
    access_key_id: str,
    access_key_secret: str,
    oss_prefix: str,
    download_latest_for_missing_task_id: bool = False,
) -> None:
    """Fetch by task_id and upload to OSS."""
    bucket = init_oss_bucket(bucket_name, endpoint, access_key_id, access_key_secret)
    try:
        chgcar, taskdoc = _get_charge_density_by_task_id(
            mp_api_key, task_id, deserialize=True
        )
    except Exception as e:
        print(f"[{mpid}] task {task_id} fetch error: {e}")
        if download_latest_for_missing_task_id:
            print(f"[{mpid}] falling back to latest calculation ...")
            _fetch_and_upload(
                mp_api_key, mpid,
                bucket_name, endpoint, access_key_id, access_key_secret, oss_prefix,
            )
        return

    if chgcar is not None:
        try:
            upload_chgcar_to_oss(bucket, chgcar, mpid, oss_prefix)
            print(f"[{mpid}] CHGCAR uploaded.")
        except Exception as e:
            print(f"[{mpid}] CHGCAR upload error: {e}")
    else:
        print(f"[{mpid}] no CHGCAR returned for task {task_id}.")

    if taskdoc is not None:
        try:
            upload_task_id_to_oss(bucket, taskdoc, mpid, oss_prefix)
        except Exception as e:
            print(f"[{mpid}] task_id upload error: {e}")
    else:
        print(f"[{mpid}] no TaskDoc returned for task {task_id}.")


# ---------------------------------------------------------------------------
# Skip-list helpers
# ---------------------------------------------------------------------------

def load_skip_set(downloaded_list: Optional[str]) -> Set[str]:
    """Load a set of mpids to skip from a plain-text file or JSON file.

    Supports:
    - Plain text: one mpid per line
    - JSON: object with mpids as keys (e.g., {"mp-123": "task-456", ...})
    """
    if not downloaded_list:
        return set()
    skip_path = Path(downloaded_list)
    if not skip_path.exists():
        print(f"Warning: --downloaded_list file not found: {skip_path}. Continuing without skip list.")
        return set()

    # Detect file format by extension
    if skip_path.suffix.lower() == ".json":
        with open(skip_path) as f:
            data = json.load(f)
        if isinstance(data, dict):
            skip = set(data.keys())
        elif isinstance(data, list):
            skip = set(str(item) for item in data)
        else:
            skip = set()
        print(f"Loaded {len(skip)} mpid(s) to skip from JSON file {skip_path}.")
    else:
        with open(skip_path) as f:
            skip = {line.strip() for line in f if line.strip()}
        print(f"Loaded {len(skip)} mpid(s) to skip from {skip_path}.")

    return skip


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(args):
    skip_set = load_skip_set(args.downloaded_list)

    oss_credentials = dict(
        bucket_name=args.oss_bucket,
        endpoint=args.oss_endpoint,
        access_key_id=args.oss_access_key_id,
        access_key_secret=args.oss_access_key_secret,
    )

    if args.workers > 5:
        raise ValueError("--workers must be <= 5 to avoid overloading the MP API.")

    if args.task_id_file is not None:
        print(f"Loading mpid->task_id map from {args.task_id_file} ...")
        with open(args.task_id_file) as f:
            items = json.load(f)

        mpids = [k for k in items.keys()]
        task_ids = [v for v in items.values()]

        # Filter out already-uploaded
        pairs = [(m, t) for m, t in zip(mpids, task_ids) if m not in skip_set]
        print(f"  {len(items)} total entries, {len(pairs)} remaining after skip list.")

        if args.limit > 0:
            print(f"  Limiting to {args.limit} entries.")
            pairs = pairs[:args.limit]

        mpids, task_ids = zip(*pairs) if pairs else ([], [])

        worker_args = [
            (
                args.mp_api_key, mpid, task_id,
                args.oss_bucket, args.oss_endpoint,
                args.oss_access_key_id, args.oss_access_key_secret,
                args.oss_prefix, args.download_latest_for_missing_task_id,
            )
            for mpid, task_id in zip(mpids, task_ids)
        ]

        if args.workers > 1:
            print(f"Starting with {args.workers} workers ...")
            pool.Pool(args.workers).starmap(_fetch_and_upload_by_task, worker_args)
        else:
            print("Starting with a single worker. Use --workers to parallelize.")
            for wargs in worker_args:
                _fetch_and_upload_by_task(*wargs)

    else:
        print("Querying MP API for all materials with charge density data ...")
        all_mpids = _get_all_mpids_with_charge_density(args.mp_api_key)
        mpids = [m for m in all_mpids if m not in skip_set]
        print(f"  {len(all_mpids)} total, {len(mpids)} remaining after skip list.")

        if args.limit > 0:
            print(f"  Limiting to {args.limit} entries.")
            mpids = mpids[:args.limit]

        worker_args = [
            (
                args.mp_api_key, mpid,
                args.oss_bucket, args.oss_endpoint,
                args.oss_access_key_id, args.oss_access_key_secret,
                args.oss_prefix,
            )
            for mpid in mpids
        ]

        if args.workers > 1:
            print(f"Starting with {args.workers} workers ...")
            pool.Pool(args.workers).starmap(_fetch_and_upload, worker_args)
        else:
            print("Starting with a single worker. Use --workers to parallelize.")
            for wargs in worker_args:
                _fetch_and_upload(*wargs)

    print("Script completed.")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
