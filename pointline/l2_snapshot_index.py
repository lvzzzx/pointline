"""Build utilities for gold.l2_snapshot_index."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl
from deltalake import DeltaTable, WriterProperties, write_deltalake

from pointline.config import STORAGE_OPTIONS

REQUIRED_COLUMNS = {
    "exchange",
    "exchange_id",
    "symbol_id",
    "ts_local_us",
    "date",
    "file_id",
    "file_line_number",
    "is_snapshot",
}

GROUP_COLUMNS = ["exchange", "exchange_id", "symbol_id", "ts_local_us", "date", "file_id"]


def build_snapshot_index(lf: pl.LazyFrame) -> pl.DataFrame:
    """Build snapshot anchor index rows from l2_updates."""
    missing = sorted(REQUIRED_COLUMNS.difference(lf.columns))
    if missing:
        raise ValueError(f"build_snapshot_index: missing required columns: {missing}")

    return (
        lf.filter(pl.col("is_snapshot"))
        .group_by(GROUP_COLUMNS)
        .agg(pl.col("file_line_number").min().alias("file_line_number"))
        .collect()
    )


def write_snapshot_index(
    df: pl.DataFrame,
    table_path: Path | str,
    *,
    partition_by: list[str] | None = None,
) -> None:
    """Write snapshot index rows to Delta, deleting affected partitions first."""
    if df.is_empty():
        return

    if partition_by is None:
        partition_by = ["exchange", "date"]

    partitions = df.select(["exchange", "date"]).unique()
    table_path = str(table_path)

    writer_properties = None
    if "compression" in STORAGE_OPTIONS:
        writer_properties = WriterProperties(compression=STORAGE_OPTIONS["compression"].upper())

    table_exists = Path(table_path, "_delta_log").exists()

    if table_exists:
        try:
            dt = DeltaTable(table_path)
        except Exception:
            dt = None

        if dt is not None:
            for row in partitions.iter_rows(named=True):
                exchange = row["exchange"]
                date_value = row["date"]
                dt.delete(f"exchange = '{exchange}' AND date = '{date_value}'")

    mode = "append" if table_exists else "overwrite"
    write_deltalake(
        table_path,
        df.to_arrow(),
        mode=mode,
        partition_by=partition_by,
        writer_properties=writer_properties,
    )


def build_and_write_snapshot_index(
    lf: pl.LazyFrame,
    table_path: Path | str,
    *,
    partition_by: list[str] | None = None,
) -> int:
    """Build snapshot index rows and write them to Delta."""
    df = build_snapshot_index(lf)
    write_snapshot_index(df, table_path, partition_by=partition_by)
    return df.height
