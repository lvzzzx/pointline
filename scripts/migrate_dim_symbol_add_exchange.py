#!/usr/bin/env python3
"""Migration script to add 'exchange' column to existing dim_symbol table.

This script:
1. Reads the current dim_symbol table
2. Derives the 'exchange' column from 'exchange_id' using EXCHANGE_MAP
3. Writes the updated table back

Usage:
    python scripts/migrate_dim_symbol_add_exchange.py [--dry-run] [--yes]
"""

import argparse
import sys
from pathlib import Path

import polars as pl

# Add parent directory to path to import pointline
sys.path.insert(0, str(Path(__file__).parent.parent))

from pointline.config import get_table_path, get_exchange_name
from pointline.io.base_repository import BaseDeltaRepository


def migrate_dim_symbol_add_exchange(dry_run: bool = False) -> None:
    """Add exchange column to dim_symbol table."""
    table_path = get_table_path("dim_symbol")
    repo = BaseDeltaRepository(table_path)
    
    print(f"Reading dim_symbol from: {table_path}")
    df = repo.read_all()
    
    if df.is_empty():
        print("dim_symbol table is empty. Nothing to migrate.")
        return
    
    print(f"Current rows: {df.height:,}")
    print(f"Current columns: {df.columns}")
    print()
    
    # Check if exchange column already exists
    if "exchange" in df.columns:
        print("✓ 'exchange' column already exists. Migration not needed.")
        # Verify all rows have exchange values
        null_count = df.filter(pl.col("exchange").is_null()).height
        if null_count > 0:
            print(f"⚠️  Warning: {null_count} rows have null exchange values. Fixing...")
            if not dry_run:
                # Derive exchange from exchange_id for null rows
                df = df.with_columns(
                    pl.when(pl.col("exchange").is_null())
                    .then(pl.col("exchange_id").map_elements(get_exchange_name, return_dtype=pl.Utf8))
                    .otherwise(pl.col("exchange"))
                    .alias("exchange")
                )
        else:
            print("✓ All rows have exchange values.")
            return
    
    # Derive exchange from exchange_id
    print("Deriving 'exchange' column from 'exchange_id'...")
    df = df.with_columns(
        pl.col("exchange_id").map_elements(get_exchange_name, return_dtype=pl.Utf8).alias("exchange")
    )
    
    # Verify
    print(f"New columns: {df.columns}")
    print()
    print("Sample data:")
    print(df.head(5).select(["exchange_id", "exchange", "exchange_symbol", "is_current"]))
    print()
    
    # Check for any unmapped exchange_ids
    unique_exchange_ids = df["exchange_id"].unique().sort()
    print(f"Unique exchange_ids: {unique_exchange_ids.to_list()}")
    
    if not dry_run:
        print()
        print("Writing updated table...")
        # Use write_full which overwrites the table with new schema
        # This is safe because we're adding a column, not removing data
        from deltalake import WriterProperties, write_deltalake
        from pointline.config import STORAGE_OPTIONS
        
        writer_properties = None
        if "compression" in STORAGE_OPTIONS:
            writer_properties = WriterProperties(
                compression=STORAGE_OPTIONS["compression"].upper()
            )
        
        # Convert to PyArrow for delta-rs
        arrow_table = df.to_arrow()
        
        # Use schema_mode="overwrite" to replace the schema entirely
        # This is safe because we're adding a column, not removing data
        write_deltalake(
            str(table_path),
            arrow_table,
            mode="overwrite",
            schema_mode="overwrite",  # Replace schema to allow new column
            writer_properties=writer_properties,
        )
        print("✓ Migration complete!")
    else:
        print()
        print("DRY RUN: Would write updated table (use --yes to apply)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add 'exchange' column to dim_symbol table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Proceed without confirmation",
    )
    
    args = parser.parse_args()
    
    if not args.dry_run and not args.yes:
        response = input("This will modify the dim_symbol table. Continue? [y/N]: ")
        if response.lower() != "y":
            print("Aborted.")
            return 1
    
    try:
        migrate_dim_symbol_add_exchange(dry_run=args.dry_run)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
