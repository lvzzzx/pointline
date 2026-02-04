#!/usr/bin/env python3
"""Reset manifest for clean-break migration to flexible bronze layer.

This script deletes the old manifest table, allowing the new schema to be
created when ingestion runs. This is a one-time migration step.

Usage:
    python scripts/reset_manifest.py

WARNING: This will delete all ingestion history. After running this script,
all bronze files will be considered "pending" and will be re-ingested.
"""

import shutil

from pointline.config import get_table_path


def main():
    """Delete old manifest table."""
    manifest_path = get_table_path("ingest_manifest")
    print(f"Manifest path: {manifest_path}")

    if manifest_path.exists():
        print("Deleting old manifest...")
        shutil.rmtree(manifest_path)
        print("✓ Manifest deleted successfully")
        print("\nNext steps:")
        print("1. Run 'pointline ingest discover --pending-only' to see files to ingest")
        print("2. Run 'pointline ingest run --table <table> --force' to rebuild from bronze")
    else:
        print("✓ Manifest not found. Nothing to delete.")
        print("The new manifest will be created automatically on first ingestion.")


if __name__ == "__main__":
    main()
