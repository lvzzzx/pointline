"""Bronze layer reorganization commands for vendor-specific archive formats."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def cmd_bronze_reorganize(args: argparse.Namespace) -> int:
    """
    Reorganize vendor archives into Hive-partitioned bronze layout.

    Currently supports:
    - quant360: .7z archives → exchange=X/type=Y/date=Z/symbol=S/*.csv.gz

    Future: Auto-detect vendor from archive patterns and route to appropriate handler.
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

    # Route to vendor-specific reorganizer
    if vendor == "quant360":
        return _reorganize_quant360(source_dir, bronze_root, dry_run=args.dry_run)

    print(f"Error: Unsupported vendor: {vendor}")
    print("Supported vendors: quant360")
    return 1


def _reorganize_quant360(source_dir: Path, bronze_root: Path, dry_run: bool = False) -> int:
    """
    Reorganize quant360 .7z archives using the fast bash script.

    Delegates to scripts/reorganize_quant360.sh for 50-100x performance vs Python.
    """
    # Find the bash script - try multiple locations for robustness
    script_locations = [
        # Relative to package root (installed)
        Path(__file__).parent.parent.parent.parent / "scripts" / "reorganize_quant360.sh",
        # Current working directory (development)
        Path.cwd() / "scripts" / "reorganize_quant360.sh",
    ]

    script_path = None
    for loc in script_locations:
        if loc.exists():
            script_path = loc
            break

    if not script_path:
        print("Error: Reorganization script not found")
        print("Searched locations:")
        for loc in script_locations:
            print(f"  - {loc}")
        print()
        print("Please ensure scripts/reorganize_quant360.sh exists in your repository.")
        return 1

    # Build command
    cmd = [str(script_path), str(source_dir), str(bronze_root)]
    if dry_run:
        cmd.append("--dry-run")

    print(f"Running: {' '.join(cmd)}")
    print()

    # Execute bash script
    try:
        result = subprocess.run(
            cmd,
            check=False,  # Let script handle exit codes
            text=True,
        )
        return result.returncode
    except Exception as e:
        print(f"Error executing reorganization script: {e}")
        return 1
