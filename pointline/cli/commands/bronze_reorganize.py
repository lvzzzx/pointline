"""Bronze layer reorganization commands for vendor-specific archive formats."""

from __future__ import annotations

import argparse
from pathlib import Path


def cmd_bronze_reorganize(args: argparse.Namespace) -> int:
    """
    Reorganize vendor archives into Hive-partitioned bronze layout.

    Delegates to vendor plugin's run_prehook() method.
    """
    source_dir = Path(args.source_dir)
    bronze_root = Path(args.bronze_root)
    vendor = args.vendor

    # Validate paths
    if not source_dir.exists():
        print(f"Error: Source directory does not exist: {source_dir}")
        return 1

    if not bronze_root.exists():
        print(f"Error: Bronze directory does not exist: {bronze_root}")
        return 1

    # Get vendor plugin
    from pointline.io.vendors.registry import get_vendor, is_vendor_registered

    if not is_vendor_registered(vendor):
        from pointline.io.vendors.registry import list_vendors

        available = list_vendors()
        print(f"Error: Unknown vendor: {vendor}")
        print(f"Available vendors: {', '.join(available)}")
        return 1

    plugin = get_vendor(vendor)

    # Check if vendor supports prehooks
    if not plugin.supports_prehooks:
        print(f"Error: Vendor '{vendor}' does not support reorganization")
        print(f"Vendor '{vendor}' does not require preprocessing (supports_prehooks=False)")
        return 1

    # Execute vendor's prehook
    print(f"Running {plugin.display_name} reorganization...")
    print(f"  Source: {source_dir}")
    print(f"  Bronze root: {bronze_root}")
    print()

    try:
        plugin.run_prehook(bronze_root, source_dir)
        print("\n✓ Reorganization completed successfully")
        return 0
    except Exception as e:
        print(f"\n✗ Reorganization failed: {e}")
        return 1
