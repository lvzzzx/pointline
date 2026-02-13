#!/usr/bin/env python3
"""Decompress Quant360 7z archives to a target directory.

This script uses the pointline upstream module to extract CSV files
from Quant360 7z archives while preserving the archive structure.

Examples:
    # Extract all archives to a temp directory
    python scripts/decompress_quant360_archives.py

    # Extract specific archive to a custom output directory
    python scripts/decompress_quant360_archives.py \
        --archive ~/data/lake/bronze/quant360/archive/order_new_STK_SH_20240102.7z \
        --output-dir /tmp/extracted

    # Extract all archives with verbose output
    python scripts/decompress_quant360_archives.py -v

    # List archives without extracting
    python scripts/decompress_quant360_archives.py --list-only
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decompress Quant360 7z archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
    QUANT360_ARCHIVE_DIR    Default archive directory (default: ~/data/lake/bronze/quant360/archive)
    QUANT360_EXTRACT_DIR    Default extraction directory (default: ~/data/lake/bronze/quant360)
        """,
    )
    parser.add_argument(
        "--archive",
        type=Path,
        help="Specific archive file to extract (default: all *.7z in archive dir)",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path.home() / "data/lake/bronze/quant360/archive",
        help="Directory containing 7z archives",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "data/lake/bronze/quant360",
        help="Directory to extract files to (default: ~/data/lake/bronze/quant360)",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        default=True,
        help="Re-compress extracted CSVs with gzip (.csv.gz) (default: True for bronze)",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="List archives and members without extracting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be extracted without doing it",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--pattern",
        help="Glob pattern to filter archives (e.g., 'order_new_*', '*_SH_*.7z')",
    )
    return parser.parse_args(argv)


def format_bytes(size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


def discover_archives(archive_dir: Path, pattern: str | None = None) -> list[Path]:
    """Discover 7z archives in directory."""
    glob_pattern = f"{pattern}.7z" if pattern else "*.7z"
    archives = sorted(archive_dir.glob(glob_pattern))
    if not archives:
        glob_all = "*.7z"
        all_archives = list(archive_dir.glob(glob_all))
        if all_archives and pattern:
            print(f"Warning: Pattern '{pattern}' matched no archives.")
            print(f"Available archives: {[a.name for a in all_archives[:5]]}...")
    return archives


def list_archive_members(archive_path: Path) -> list[str]:
    """List CSV members in a 7z archive."""
    import py7zr

    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        return sorted(name for name in archive.getnames() if name.lower().endswith(".csv"))


def _build_bronze_path(
    output_dir: Path,
    exchange: str,
    data_type: str,
    trading_date: str,
    symbol: str,
) -> Path:
    """Build bronze Hive-style path: exchange=X/type=Y/date=Z/symbol=W/W.csv.gz"""
    return (
        output_dir
        / f"exchange={exchange}"
        / f"type={data_type}"
        / f"date={trading_date}"
        / f"symbol={symbol}"
        / f"{symbol}.csv.gz"
    )


def extract_archive(
    archive_path: Path,
    output_dir: Path,
    *,
    compress: bool = False,
    verbose: bool = False,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Extract archive members to bronze directory with Hive-style partitioning.

    Returns:
        (extracted_count, total_bytes)
    """
    import tempfile

    import py7zr

    from pointline.vendors.quant360.filenames import (
        parse_archive_filename,
        parse_symbol_from_member_path,
    )

    if verbose:
        print(f"\nArchive: {archive_path.name}")

    # Parse archive metadata
    try:
        archive_meta = parse_archive_filename(archive_path.name)
        exchange = archive_meta.exchange
        data_type = archive_meta.stream_type
        trading_date = archive_meta.trading_date.isoformat()
    except ValueError as e:
        print(f"  Warning: Could not parse archive filename: {e}")
        exchange = "unknown"
        data_type = "unknown"
        trading_date = "unknown"

    if dry_run:
        members = list_archive_members(archive_path)
        print(f"  Would extract {len(members)} members")
        for member in members[:5]:
            symbol = parse_symbol_from_member_path(member)
            bronze_path = _build_bronze_path(output_dir, exchange, data_type, trading_date, symbol)
            print(f"    - {member} -> {bronze_path.relative_to(output_dir)}")
        if len(members) > 5:
            print(f"    ... and {len(members) - 5} more")
        return len(members), 0

    total_bytes = 0
    extracted_count = 0

    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        # Get list of CSV members
        csv_members = [n for n in archive.getnames() if n.lower().endswith(".csv")]

        if verbose:
            print(f"  Members: {len(csv_members)} CSV files")

        # Extract to temp location then move to bronze structure
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            archive.extract(path=tmp_path, targets=csv_members)

            for member_name in csv_members:
                src = tmp_path / member_name
                if not src.exists():
                    print(f"  Warning: Expected member not found: {member_name}")
                    continue

                symbol = parse_symbol_from_member_path(member_name)
                dst = _build_bronze_path(output_dir, exchange, data_type, trading_date, symbol)
                dst.parent.mkdir(parents=True, exist_ok=True)

                # Always compress for bronze storage
                with open(src, "rb") as f_in, gzip.open(dst, "wb", compresslevel=6) as f_out:
                    shutil.copyfileobj(f_in, f_out)

                file_size = dst.stat().st_size
                total_bytes += file_size
                extracted_count += 1

                if verbose:
                    print(f"  ✓ {symbol}.csv.gz ({format_bytes(file_size)})")

    return extracted_count, total_bytes


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    # Override with environment variables if set
    import os

    if env_archive_dir := os.environ.get("QUANT360_ARCHIVE_DIR"):
        args.archive_dir = Path(env_archive_dir)
    if env_extract_dir := os.environ.get("QUANT360_EXTRACT_DIR"):
        args.output_dir = Path(env_extract_dir)

    # Validate paths
    if not args.archive_dir.exists():
        print(f"Error: Archive directory not found: {args.archive_dir}")
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Discover archives
    if args.archive:
        if not args.archive.exists():
            print(f"Error: Archive not found: {args.archive}")
            return 1
        archives = [args.archive]
    else:
        archives = discover_archives(args.archive_dir, args.pattern)

    if not archives:
        print(f"No 7z archives found in {args.archive_dir}")
        return 0

    print(f"Found {len(archives)} archive(s)")

    if args.list_only:
        total_members = 0
        for archive_path in archives:
            members = list_archive_members(archive_path)
            total_members += len(members)
            print(f"\n{archive_path.name}")
            print(f"  Size: {format_bytes(archive_path.stat().st_size)}")
            print(f"  Members: {len(members)}")
            for member in members[:10]:
                print(f"    - {member}")
            if len(members) > 10:
                print(f"    ... and {len(members) - 10} more")
        print(f"\nTotal: {len(archives)} archives, {total_members} members")
        return 0

    # Extract archives
    print(f"\nOutput directory: {args.output_dir}")
    print("Format: Hive-partitioned bronze structure (exchange=X/type=Y/date=Z/symbol=W/W.csv.gz)")
    if args.dry_run:
        print("Mode: DRY RUN (no actual extraction)")

    total_archives = 0
    total_members = 0
    total_bytes = 0

    for archive_path in archives:
        try:
            extracted, bytes_written = extract_archive(
                archive_path,
                args.output_dir,
                compress=True,  # Always compress for bronze storage
                verbose=args.verbose,
                dry_run=args.dry_run,
            )
            total_archives += 1
            total_members += extracted
            total_bytes += bytes_written

            if not args.verbose and not args.dry_run:
                print(f"  ✓ {archive_path.name}: {extracted} files ({format_bytes(bytes_written)})")

        except Exception as e:
            print(f"  ✗ {archive_path.name}: ERROR - {e}")
            if args.verbose:
                import traceback

                traceback.print_exc()
            continue

    # Summary
    print(f"\n{'=' * 50}")
    print(f"Extracted: {total_members} files from {total_archives} archives")
    print(f"Total size: {format_bytes(total_bytes)}")
    print(f"Output: {args.output_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
