#!/usr/bin/env python3
"""One-time migration for 2026-02-02 schema renames.

Renames fixed-point columns to the latest naming convention:
- single-price tables use `px_int`
- multi-price tables use `*_px_int`/`*_sz_int`

This rewrites Delta tables in-place (overwrite).

Usage:
  python scripts/migrate_schema_20260202.py --dry-run
  python scripts/migrate_schema_20260202.py --tables trades,book_snapshot_25
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from pointline.config import get_table_path

MIGRATIONS: dict[str, dict[str, str]] = {
    "trades": {"price_int": "px_int", "price_px_int": "px_int"},
    "liquidations": {"price_int": "px_int", "price_px_int": "px_int"},
    "book_snapshot_25": {
        "bids_px": "bids_px_int",
        "bids_sz": "bids_sz_int",
        "asks_px": "asks_px_int",
        "asks_sz": "asks_sz_int",
    },
    "szse_l3_orders": {"price_px_int": "px_int"},
    "szse_l3_ticks": {"price_px_int": "px_int"},
}

TABLE_PARTITIONS: dict[str, list[str]] = {
    "trades": ["exchange", "date"],
    "book_snapshot_25": ["exchange", "date"],
    "liquidations": ["exchange", "date"],
    "szse_l3_orders": ["exchange", "date"],
    "szse_l3_ticks": ["exchange", "date"],
}


def _table_exists(table_path: Path) -> bool:
    return (table_path / "_delta_log").exists()


def _validate_columns(cols: list[str], mapping: dict[str, str]) -> tuple[bool, str]:
    old_cols = [col for col in mapping if col in cols]
    new_cols = [mapping[col] for col in mapping if mapping[col] in cols]
    if len(old_cols) > 1 and len({mapping[col] for col in old_cols}) == 1:
        return False, "multiple_source_columns"
    if not old_cols and new_cols:
        return False, "already_migrated"
    if not old_cols:
        return False, "missing_old_columns"
    if new_cols:
        return False, "conflict_old_and_new"
    return True, ""


def _list_partitions(table_path: Path, partition_cols: list[str]) -> list[dict[str, str]]:
    if not partition_cols:
        return [{}]
    if len(partition_cols) != 2:
        raise ValueError("Only 2-level partitions are supported by this script.")

    first, second = partition_cols
    partitions: list[dict[str, str]] = []
    for first_dir in (table_path / f"{first}=").parent.glob(f"{first}=*"):
        if not first_dir.is_dir():
            continue
        first_val = first_dir.name.split("=", 1)[-1]
        for second_dir in first_dir.glob(f"{second}=*"):
            if not second_dir.is_dir():
                continue
            second_val = second_dir.name.split("=", 1)[-1]
            partitions.append({first: first_val, second: second_val})
    return partitions


def _partition_predicate(partition: dict[str, str]) -> str:
    parts = []
    for key, value in partition.items():
        parts.append(f"{key} = '{value}'")
    return " AND ".join(parts)


def _partition_filters(partition: dict[str, str]) -> list[tuple[str, str, object]]:
    from datetime import date

    filters: list[tuple[str, str, object]] = []
    for key, value in partition.items():
        if key == "date":
            year, month, day = (int(part) for part in value.split("-"))
            filters.append((key, "=", date(year, month, day)))
        else:
            filters.append((key, "=", value))
    return filters


def migrate_table(table_name: str, mapping: dict[str, str], *, dry_run: bool) -> None:
    try:
        table_path = get_table_path(table_name)
    except KeyError as exc:
        print(f"[skip] {table_name}: not registered in TABLE_PATHS ({exc})")
        return
    if not _table_exists(table_path):
        print(f"[skip] {table_name}: no Delta log at {table_path}")
        return

    from deltalake import DeltaTable

    dt = DeltaTable(str(table_path))
    cols = dt.schema().to_arrow().names
    ok, reason = _validate_columns(cols, mapping)
    if not ok:
        print(f"[skip] {table_name}: {reason}")
        return

    partition_cols = list(dt.metadata().partition_columns)

    if dry_run:
        print(f"[dry-run] {table_name}: {mapping}")
        print(f"  columns_before={cols}")
        cols_after = [mapping.get(col, col) for col in cols]
        print(f"  columns_after ={cols_after}")
        if partition_cols:
            partitions = _list_partitions(table_path, partition_cols)
            print(f"  partitions={len(partitions)}")
        return

    from deltalake import write_deltalake

    partition_by = TABLE_PARTITIONS.get(table_name, partition_cols)
    if not partition_by:
        raise ValueError(f"{table_name}: missing partition config")

    partitions = _list_partitions(table_path, partition_by)
    if not partitions:
        raise ValueError(f"{table_name}: no partitions found under {table_path}")

    for partition in partitions:
        predicate = _partition_predicate(partition)
        table = dt.to_pyarrow_table(filters=_partition_filters(partition))
        df = pl.from_arrow(table)
        if df.is_empty():
            continue
        ok, reason = _validate_columns(df.columns, mapping)
        if not ok:
            print(f"[skip] {table_name} {partition}: {reason}")
            continue
        active_mapping = {col: new for col, new in mapping.items() if col in df.columns}
        new_df = df.rename(active_mapping)
        write_deltalake(
            str(table_path),
            new_df.to_arrow(),
            mode="overwrite",
            partition_by=partition_by,
            predicate=predicate,
            schema_mode="overwrite",
        )
        print(f"[ok] {table_name} {partition}: migrated")


def main() -> int:
    parser = argparse.ArgumentParser(description="One-time Delta schema migration (2026-02-02).")
    parser.add_argument(
        "--tables",
        type=str,
        default=",".join(MIGRATIONS.keys()),
        help="Comma-separated list of tables to migrate.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show planned changes only.")
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = [t for t in tables if t not in MIGRATIONS]
    if unknown:
        print(f"Unknown tables: {unknown}. Known: {sorted(MIGRATIONS.keys())}")
        return 1

    for table_name in tables:
        migrate_table(table_name, MIGRATIONS[table_name], dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
