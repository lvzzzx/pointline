"""Minimal v2 event query API with explicit metadata opt-in."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.schemas.registry import get_table_spec
from pointline.v2.research._time import TimestampInput, derive_trading_date_bounds, normalize_ts_us
from pointline.v2.storage.delta.dimension_store import DeltaDimensionStore
from pointline.v2.storage.delta.layout import table_path


def load_events(
    *,
    silver_root: Path,
    table: str,
    exchange: str,
    symbol: str,
    start: TimestampInput,
    end: TimestampInput,
    columns: list[str] | None = None,
    include_lineage: bool = False,
    symbol_meta_columns: list[str] | None = None,
) -> pl.DataFrame:
    """Load event rows from one canonical v2 event table.

    Notes:
    - No implicit dim_symbol join is performed.
    - Metadata enrichment is explicit via ``symbol_meta_columns``.
    - Time window is ``[start, end)`` on ``ts_event_us``.
    """
    spec = get_table_spec(table)
    if spec.kind != "event":
        raise ValueError(f"load_events only supports event tables, got {table!r}")

    exchange_norm = exchange.strip().lower()
    if not exchange_norm:
        raise ValueError("exchange must be non-empty")
    if not symbol.strip():
        raise ValueError("symbol must be non-empty")

    start_ts_us = normalize_ts_us(start, param_name="start")
    end_ts_us = normalize_ts_us(end, param_name="end")
    start_date, end_date = derive_trading_date_bounds(
        exchange=exchange_norm,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
    )

    event_schema = spec.to_polars()
    selected_cols = _resolve_selected_columns(
        event_schema=event_schema,
        columns=columns,
        include_lineage=include_lineage,
    )
    meta_cols = _resolve_meta_columns(symbol_meta_columns)

    path = table_path(silver_root=silver_root, table_name=spec.name)
    if not path.exists():
        return _empty_result_frame(
            event_schema=event_schema,
            selected_cols=selected_cols,
            meta_cols=meta_cols,
        )

    scan_cols = list(selected_cols)
    for tie_col in spec.tie_break_keys:
        if tie_col not in scan_cols:
            scan_cols.append(tie_col)
    if meta_cols:
        for required in ("exchange", "symbol_id", "ts_event_us"):
            if required not in scan_cols:
                scan_cols.append(required)

    lf = pl.scan_delta(str(path)).filter(
        (pl.col("exchange") == exchange_norm)
        & (pl.col("symbol") == symbol)
        & (pl.col("trading_date") >= pl.lit(start_date))
        & (pl.col("trading_date") <= pl.lit(end_date))
        & (pl.col("ts_event_us") >= start_ts_us)
        & (pl.col("ts_event_us") < end_ts_us)
    )
    frame = lf.select(scan_cols).collect()

    sort_cols = [name for name in spec.tie_break_keys if name in frame.columns]
    if sort_cols:
        frame = frame.sort(sort_cols)

    if meta_cols:
        frame = _attach_symbol_metadata(
            frame,
            silver_root=silver_root,
            exchange=exchange_norm,
            meta_cols=meta_cols,
        )

    final_cols = list(selected_cols)
    for meta in meta_cols:
        if meta not in final_cols:
            final_cols.append(meta)
    return frame.select(final_cols)


def _resolve_selected_columns(
    *,
    event_schema: dict[str, pl.DataType],
    columns: list[str] | None,
    include_lineage: bool,
) -> list[str]:
    if columns is not None:
        unknown = sorted(set(columns) - set(event_schema))
        if unknown:
            raise ValueError(f"Unknown columns requested: {unknown}")
        return list(columns)

    selected = list(event_schema)
    if not include_lineage:
        selected = [col for col in selected if col not in {"file_id", "file_seq"}]
    return selected


def _resolve_meta_columns(symbol_meta_columns: list[str] | None) -> list[str]:
    if not symbol_meta_columns:
        return []
    dim_schema = DIM_SYMBOL.to_polars()
    forbidden = {"exchange", "symbol_id", "valid_from_ts_us", "valid_until_ts_us"}
    allowed = set(dim_schema) - forbidden
    unknown = sorted(set(symbol_meta_columns) - allowed)
    if unknown:
        raise ValueError(f"Unknown symbol metadata columns requested: {unknown}")
    return list(dict.fromkeys(symbol_meta_columns))


def _attach_symbol_metadata(
    frame: pl.DataFrame,
    *,
    silver_root: Path,
    exchange: str,
    meta_cols: list[str],
) -> pl.DataFrame:
    if frame.is_empty():
        dim_schema = DIM_SYMBOL.to_polars()
        return frame.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        )

    dim = (
        DeltaDimensionStore(silver_root=silver_root)
        .load_dim_symbol()
        .filter(pl.col("exchange") == exchange)
    )
    if dim.is_empty():
        dim_schema = DIM_SYMBOL.to_polars()
        return frame.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        )

    dim_cols = ["exchange", "symbol_id", "valid_from_ts_us", "valid_until_ts_us", *meta_cols]
    rows = frame.with_row_index("_row_id")
    matches = rows.join(
        dim.select(dim_cols),
        on=["exchange", "symbol_id"],
        how="left",
    ).filter(
        (pl.col("valid_from_ts_us") <= pl.col("ts_event_us"))
        & (pl.col("ts_event_us") < pl.col("valid_until_ts_us"))
    )

    if matches.is_empty():
        dim_schema = DIM_SYMBOL.to_polars()
        return rows.with_columns(
            [pl.lit(None, dtype=dim_schema[col]).alias(col) for col in meta_cols]
        ).drop("_row_id")

    per_row_meta = matches.group_by("_row_id").agg(
        [pl.col(col).first().alias(col) for col in meta_cols]
    )
    return rows.join(per_row_meta, on="_row_id", how="left").drop("_row_id")


def _empty_result_frame(
    *,
    event_schema: dict[str, pl.DataType],
    selected_cols: list[str],
    meta_cols: list[str],
) -> pl.DataFrame:
    dim_schema = DIM_SYMBOL.to_polars()
    schema: dict[str, pl.DataType] = {}
    for col in selected_cols:
        schema[col] = event_schema[col]
    for col in meta_cols:
        if col not in schema:
            schema[col] = dim_schema[col]
    return pl.DataFrame(schema=schema)
