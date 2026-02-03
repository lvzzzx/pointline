#!/usr/bin/env python3
"""CLI tool to reorganize Quant360 archives into Hive-partitioned bronze layout.

Usage:
    python scripts/reorganize_quant360.py --source ~/data/lake/bronze/data.quant360.com --bronze ~/data/lake/bronze --dry-run
    python scripts/reorganize_quant360.py --source ~/data/lake/bronze/data.quant360.com --bronze ~/data/lake/bronze
"""

import argparse
from pathlib import Path

from pointline.io.vendors.quant360 import reorganize_quant360_archives


def main():
    parser = argparse.ArgumentParser(
        description="Reorganize Quant360 SZSE L3 archives into Hive-partitioned bronze layout"
    )
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="Source directory containing .7z archives",
    )
    parser.add_argument(
        "--bronze",
        type=Path,
        required=True,
        help="Bronze root directory (target)",
    )
    parser.add_argument(
        "--vendor",
        type=str,
        default="quant360",
        help="Vendor name (default: quant360)",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="*_new_STK_*.7z",
        help="Archive filename pattern (default: *_new_STK_*.7z)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without executing",
    )

    args = parser.parse_args()

    # Validate paths
    if not args.source.exists():
        print(f"Error: Source directory does not exist: {args.source}")
        return 1

    if not args.bronze.exists():
        print(f"Error: Bronze directory does not exist: {args.bronze}")
        return 1

    # Run reorganization
    results = reorganize_quant360_archives(
        source_dir=args.source,
        bronze_root=args.bronze,
        vendor=args.vendor,
        pattern=args.pattern,
        dry_run=args.dry_run,
    )

    # Return exit code
    failed = sum(1 for c in results.values() if c < 0)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    exit(main())
