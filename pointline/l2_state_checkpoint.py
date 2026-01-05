"""Build utilities for gold.l2_state_checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
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
    "ingest_seq",
    "file_line_number",
    "file_id",
    "date",
    "is_snapshot",
    "side",
    "price_int",
    "size_int",
}

ORDER_COLUMNS = ["ts_local_us", "ingest_seq", "file_line_number"]

CHECKPOINT_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,
    "exchange_id": pl.Int16,
    "symbol_id": pl.Int64,
    "date": pl.Date,
    "ts_local_us": pl.Int64,
    "bids": pl.List(
        pl.Struct(
            [
                pl.Field("price_int", pl.Int64),
                pl.Field("size_int", pl.Int64),
            ]
        )
    ),
    "asks": pl.List(
        pl.Struct(
            [
                pl.Field("price_int", pl.Int64),
                pl.Field("size_int", pl.Int64),
            ]
        )
    ),
    "file_id": pl.Int32,
    "ingest_seq": pl.Int32,
    "file_line_number": pl.Int32,
    "checkpoint_kind": pl.Utf8,
}


@dataclass
class _CheckpointConfig:
    checkpoint_every_us: int | None
    checkpoint_every_updates: int | None
    validate_monotonic: bool


def _normalize_config(
    checkpoint_every_us: int | None,
    checkpoint_every_updates: int | None,
    validate_monotonic: bool,
) -> _CheckpointConfig:
    every_us = checkpoint_every_us if checkpoint_every_us and checkpoint_every_us > 0 else None
    every_updates = (
        checkpoint_every_updates if checkpoint_every_updates and checkpoint_every_updates > 0 else None
    )

    if every_us is None and every_updates is None:
        raise ValueError(
            "build_state_checkpoints: at least one of checkpoint_every_us or "
            "checkpoint_every_updates must be set"
        )

    return _CheckpointConfig(
        checkpoint_every_us=every_us,
        checkpoint_every_updates=every_updates,
        validate_monotonic=validate_monotonic,
    )


def build_state_checkpoints(
    lf: pl.LazyFrame,
    *,
    checkpoint_every_us: int | None = None,
    checkpoint_every_updates: int | None = None,
    validate_monotonic: bool = False,
) -> pl.DataFrame:
    """Build full-depth L2 state checkpoints from l2_updates."""
    missing = sorted(REQUIRED_COLUMNS.difference(lf.columns))
    if missing:
        raise ValueError(f"build_state_checkpoints: missing required columns: {missing}")

    config = _normalize_config(checkpoint_every_us, checkpoint_every_updates, validate_monotonic)

    updates = lf.sort(ORDER_COLUMNS).collect()
    if updates.is_empty():
        return pl.DataFrame(schema=CHECKPOINT_SCHEMA)

    bids: dict[int, int] = {}
    asks: dict[int, int] = {}
    checkpoints: list[dict[str, object]] = []

    last_checkpoint_us: int | None = None
    updates_since = 0
    prev_key: tuple[int, int, int] | None = None
    snapshot_key: tuple[int, int] | None = None

    for row in updates.iter_rows(named=True):
        key = (row["ts_local_us"], row["ingest_seq"], row["file_line_number"])
        if config.validate_monotonic and prev_key is not None and key < prev_key:
            raise ValueError(f"build_state_checkpoints: updates out of order: {key} < {prev_key}")
        prev_key = key

        if row["is_snapshot"]:
            snapshot_group = (row["ts_local_us"], row["file_id"])
            if snapshot_group != snapshot_key:
                bids.clear()
                asks.clear()
                snapshot_key = snapshot_group
        else:
            snapshot_key = None

        side_map = bids if row["side"] == 0 else asks
        if row["size_int"] == 0:
            side_map.pop(row["price_int"], None)
        else:
            side_map[row["price_int"]] = row["size_int"]

        if last_checkpoint_us is None:
            last_checkpoint_us = row["ts_local_us"]

        updates_since += 1

        emit_checkpoint = False
        if config.checkpoint_every_us is not None:
            if row["ts_local_us"] - last_checkpoint_us >= config.checkpoint_every_us:
                emit_checkpoint = True
        if config.checkpoint_every_updates is not None:
            if updates_since >= config.checkpoint_every_updates:
                emit_checkpoint = True

        if emit_checkpoint:
            checkpoints.append(
                {
                    "exchange": row["exchange"],
                    "exchange_id": row["exchange_id"],
                    "symbol_id": row["symbol_id"],
                    "date": row["date"],
                    "ts_local_us": row["ts_local_us"],
                    "bids": [
                        {"price_int": price, "size_int": size}
                        for price, size in sorted(bids.items(), reverse=True)
                    ],
                    "asks": [
                        {"price_int": price, "size_int": size}
                        for price, size in sorted(asks.items())
                    ],
                    "file_id": row["file_id"],
                    "ingest_seq": row["ingest_seq"],
                    "file_line_number": row["file_line_number"],
                    "checkpoint_kind": "periodic",
                }
            )
            updates_since = 0
            last_checkpoint_us = row["ts_local_us"]

    if not checkpoints:
        return pl.DataFrame(schema=CHECKPOINT_SCHEMA)

    return pl.DataFrame(checkpoints, schema=CHECKPOINT_SCHEMA)


def write_state_checkpoints(
    df: pl.DataFrame,
    table_path: Path | str,
    *,
    partition_by: list[str] | None = None,
) -> None:
    """Write checkpoint rows to Delta, deleting affected partitions first."""
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


def build_and_write_state_checkpoints(
    lf: pl.LazyFrame,
    table_path: Path | str,
    *,
    checkpoint_every_us: int | None = None,
    checkpoint_every_updates: int | None = None,
    validate_monotonic: bool = False,
    partition_by: list[str] | None = None,
) -> int:
    """Build checkpoint rows and write them to Delta."""
    df = build_state_checkpoints(
        lf,
        checkpoint_every_us=checkpoint_every_us,
        checkpoint_every_updates=checkpoint_every_updates,
        validate_monotonic=validate_monotonic,
    )
    write_state_checkpoints(df, table_path, partition_by=partition_by)
    return df.height
