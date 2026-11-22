#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NOAA CPC Bronze Data Inspector
==============================
Verifies downloaded Bronze snapshot integrity (SHA-256 checksums against
_manifest.json), checks file structure, row counts, and date spans, and prints
a concise inspection report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Dict, List

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def resolve_repo_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    candidate = script_dir.parent.parent
    return candidate if (candidate / "bronze").exists() else script_dir.parent


def find_latest_snapshot(bronze_data_dir: Path) -> Path:
    snapshots = sorted(bronze_data_dir.glob("ingest_date=*"))
    if not snapshots:
        raise FileNotFoundError(f"No ingest_date=* snapshot found in {bronze_data_dir}")
    return snapshots[-1]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect_snapshot(snapshot_dir: Path) -> bool:
    manifest_path = snapshot_dir / "_manifest.json"
    print("=" * 68)
    print("BÁO CÁO KIỂM TRA DỮ LIỆU CRAWL NOAA TELECONNECTIONS (BRONZE)")
    print("=" * 68)
    print(f"Snapshot directory : {snapshot_dir}")

    if not manifest_path.exists():
        print(f"[ERROR] Thiếu tệp manifest: {manifest_path}")
        return False

    manifest: List[Dict[str, object]] = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(f"Số nguồn dữ liệu   : {len(manifest)}")

    all_ok = True
    total_bytes = 0

    for entry in manifest:
        fname = str(entry["file"])
        fpath = snapshot_dir / fname
        print("-" * 68)
        print(f"File       : {fname}")
        print(f"Source URL : {entry.get('source_url')}")
        print(f"Ingest date: {entry.get('ingest_date')} | Range: {entry.get('kept_range')}")

        if not fpath.exists():
            print("Status     : [MISSING] Không tìm thấy file!")
            all_ok = False
            continue

        size_bytes = fpath.stat().st_size
        total_bytes += size_bytes
        actual_sha = sha256_file(fpath)
        expected_sha = str(entry.get("saved_sha256", ""))
        sha_match = actual_sha == expected_sha
        if not sha_match:
            all_ok = False

        lines = fpath.read_text(encoding="utf-8").splitlines()
        is_for = fname.endswith(".for")
        header_lines = 4 if is_for else 1
        data_lines = [ln for ln in lines[header_lines:] if ln.strip()]
        expected_rows = entry.get("rows_kept")
        rows_match = expected_rows is None or len(data_lines) == int(expected_rows)
        if not rows_match:
            all_ok = False

        print(
            f"Size       : {size_bytes:,} bytes | Rows: {len(data_lines)} "
            f"(expected: {expected_rows}) -> {'OK' if rows_match else 'MISMATCH'}"
        )
        print(f"SHA-256    : {'VERIFIED' if sha_match else 'FAILED'} ({actual_sha[:16]}...)")
        print("Sample     :")
        for sample in lines[: header_lines + 2]:
            print(f"  {sample}")
        if data_lines:
            print(f"  ... last: {data_lines[-1]}")

    print("=" * 68)
    print(f"Tổng dung lượng lưu trữ: {total_bytes / 1024:.2f} KB")
    manifest_csv = snapshot_dir.parent / "manifest_download.csv"
    if manifest_csv.exists():
        print(f"Tệp nhật ký tải        : {manifest_csv.name} ({manifest_csv.stat().st_size / 1024:.2f} KB)")
    print(f"Kết luận kiểm định     : {'PASS (100% toàn vẹn)' if all_ok else 'FAIL'}")
    print("=" * 68)
    return all_ok


def main() -> None:
    repo_root = resolve_repo_root()
    parser = argparse.ArgumentParser(description="Inspect NOAA Bronze snapshot integrity.")
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=repo_root / "bronze" / "data",
        help="Path to bronze/data directory.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=None,
        help="Specific ingest_date=* directory to inspect (defaults to latest).",
    )
    args = parser.parse_args()
    snapshot_dir = args.snapshot_dir or find_latest_snapshot(args.bronze_root)
    ok = inspect_snapshot(snapshot_dir)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
