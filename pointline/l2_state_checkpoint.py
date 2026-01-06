"""Build utilities for gold.l2_state_checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

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


def build_state_checkpoints_delta(
    *,
    updates_path: Path | str,
    output_path: Path | str,
    exchange: str | None,
    exchange_id: int | None,
    symbol_id: int | list[int] | None,
    start_date: date | str,
    end_date: date | str,
    checkpoint_every_us: int | None = None,
    checkpoint_every_updates: int | None = None,
    validate_monotonic: bool = False,
) -> int:
    """Build state checkpoints via the Rust replay engine and write to Delta."""
    try:
        import l2_replay as _l2_replay
    except Exception as exc:  # pragma: no cover - optional import
        raise ImportError(
            "l2_replay Rust extension not available; build the PyO3 module to use this path."
        ) from exc

    start_str = start_date.isoformat() if isinstance(start_date, date) else str(start_date)
    end_str = end_date.isoformat() if isinstance(end_date, date) else str(end_date)

    return _l2_replay.build_state_checkpoints(
        updates_path=str(updates_path),
        output_path=str(output_path),
        exchange=exchange,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        start_date=start_str,
        end_date=end_str,
        checkpoint_every_us=checkpoint_every_us,
        checkpoint_every_updates=checkpoint_every_updates,
        validate_monotonic=validate_monotonic,
    )
