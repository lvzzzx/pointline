"""Internal builder implementations for v2 research spine generation."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.research._spine_types import (
    ClockSpineConfig,
    DollarSpineConfig,
    TradesSpineConfig,
    VolumeSpineConfig,
)
from pointline.research._time import derive_trading_date_bounds
from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.schemas.events import TRADES
from pointline.schemas.types import QTY_SCALE
from pointline.storage.delta.dimension_store import DeltaDimensionStore
from pointline.storage.delta.layout import table_path

SPINE_COLUMNS: list[str] = ["exchange", "symbol", "symbol_id", "ts_spine_us"]


def build_clock_spine(
    *,
    silver_root: Path,
    exchange: str,
    symbols: list[str],
    start_ts_us: int,
    end_ts_us: int,
    config: ClockSpineConfig,
) -> pl.DataFrame:
    if config.step_us <= 0:
        raise ValueError(f"step_us must be > 0, got {config.step_us}")
    if config.max_rows <= 0:
        raise ValueError(f"max_rows must be > 0, got {config.max_rows}")

    dim = DeltaDimensionStore(silver_root=silver_root).load_dim_symbol()
    if dim.is_empty():
        return empty_spine_frame()

    active = (
        dim.filter(pl.col("exchange") == exchange)
        .filter(pl.col("exchange_symbol").is_in(symbols))
        .filter(
            (pl.col("valid_from_ts_us") < end_ts_us) & (pl.col("valid_until_ts_us") > start_ts_us)
        )
        .sort(["exchange_symbol", "valid_from_ts_us"])
    )
    if active.is_empty():
        return empty_spine_frame()

    rows: list[tuple[str, str, int, int]] = []
    row_count = 0
    for row in active.iter_rows(named=True):
        interval_start = max(start_ts_us, int(row["valid_from_ts_us"]))
        interval_end = min(end_ts_us, int(row["valid_until_ts_us"]))
        first = ((interval_start // config.step_us) + 1) * config.step_us
        if first > interval_end:
            continue

        count = ((interval_end - first) // config.step_us) + 1
        row_count += count
        if row_count > config.max_rows:
            raise RuntimeError(
                f"Clock spine would generate too many rows: {row_count} > {config.max_rows}"
            )

        for ts in range(first, interval_end + 1, config.step_us):
            rows.append((exchange, str(row["exchange_symbol"]), int(row["symbol_id"]), ts))

    if not rows:
        return empty_spine_frame()

    out = pl.DataFrame(
        rows,
        schema={
            "exchange": pl.Utf8,
            "symbol": pl.Utf8,
            "symbol_id": pl.Int64,
            "ts_spine_us": pl.Int64,
        },
        orient="row",
    )
    return (
        out.unique(subset=["exchange", "symbol", "ts_spine_us"], keep="first")
        .sort(["exchange", "symbol", "ts_spine_us"])
        .select(SPINE_COLUMNS)
    )


def build_trades_spine(
    *,
    silver_root: Path,
    exchange: str,
    symbols: list[str],
    start_ts_us: int,
    end_ts_us: int,
    config: TradesSpineConfig,
) -> pl.DataFrame:
    if config.max_rows <= 0:
        raise ValueError(f"max_rows must be > 0, got {config.max_rows}")

    trades = _load_trades(
        silver_root=silver_root,
        exchange=exchange,
        symbols=symbols,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
    )
    if trades.is_empty():
        return empty_spine_frame()

    out = (
        trades.unique(subset=["exchange", "symbol", "symbol_id", "ts_event_us"], keep="first")
        .select(["exchange", "symbol", "symbol_id", "ts_event_us"])
        .rename({"ts_event_us": "ts_spine_us"})
        .sort(["exchange", "symbol", "ts_spine_us"])
    )
    _enforce_max_rows(out, max_rows=config.max_rows)
    return out.select(SPINE_COLUMNS)


def build_volume_spine(
    *,
    silver_root: Path,
    exchange: str,
    symbols: list[str],
    start_ts_us: int,
    end_ts_us: int,
    config: VolumeSpineConfig,
) -> pl.DataFrame:
    if config.volume_threshold_scaled <= 0:
        raise ValueError(
            f"volume_threshold_scaled must be > 0, got {config.volume_threshold_scaled}"
        )
    if config.max_rows <= 0:
        raise ValueError(f"max_rows must be > 0, got {config.max_rows}")

    trades = _load_trades(
        silver_root=silver_root,
        exchange=exchange,
        symbols=symbols,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
    )
    return _threshold_spine_from_trades(
        trades=trades,
        measure_expr=pl.col("qty").abs(),
        threshold=config.volume_threshold_scaled,
        max_rows=config.max_rows,
    )


def build_dollar_spine(
    *,
    silver_root: Path,
    exchange: str,
    symbols: list[str],
    start_ts_us: int,
    end_ts_us: int,
    config: DollarSpineConfig,
) -> pl.DataFrame:
    if config.dollar_threshold_scaled <= 0:
        raise ValueError(
            f"dollar_threshold_scaled must be > 0, got {config.dollar_threshold_scaled}"
        )
    if config.max_rows <= 0:
        raise ValueError(f"max_rows must be > 0, got {config.max_rows}")

    trades = _load_trades(
        silver_root=silver_root,
        exchange=exchange,
        symbols=symbols,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
    )
    # Use Python big-int per row to avoid int64 overflow on intermediate multiply.
    notional = pl.struct(["price", "qty"]).map_elements(
        lambda row: abs(int(row["price"])) * abs(int(row["qty"])) // QTY_SCALE,
        return_dtype=pl.Int64,
    )
    return _threshold_spine_from_trades(
        trades=trades,
        measure_expr=notional,
        threshold=config.dollar_threshold_scaled,
        max_rows=config.max_rows,
    )


def _threshold_spine_from_trades(
    *,
    trades: pl.DataFrame,
    measure_expr: pl.Expr,
    threshold: int,
    max_rows: int,
) -> pl.DataFrame:
    if trades.is_empty():
        return empty_spine_frame()

    group_cols = ["exchange", "symbol"]
    measured = (
        trades.with_columns(measure_expr.cast(pl.Int64).alias("_measure"))
        .with_columns(pl.col("_measure").cum_sum().over(group_cols).alias("_cum_measure"))
        .with_columns((pl.col("_cum_measure") // threshold).cast(pl.Int64).alias("_bucket"))
        .with_columns(
            pl.col("_bucket").shift(1).over(group_cols).fill_null(0).alias("_prev_bucket")
        )
    )

    out = (
        measured.filter((pl.col("_bucket") >= 1) & (pl.col("_bucket") > pl.col("_prev_bucket")))
        .select(["exchange", "symbol", "symbol_id", "ts_event_us"])
        .rename({"ts_event_us": "ts_spine_us"})
        .unique(subset=["exchange", "symbol", "ts_spine_us"], keep="first")
        .sort(["exchange", "symbol", "ts_spine_us"])
    )
    _enforce_max_rows(out, max_rows=max_rows)
    return out.select(SPINE_COLUMNS)


def _load_trades(
    *,
    silver_root: Path,
    exchange: str,
    symbols: list[str],
    start_ts_us: int,
    end_ts_us: int,
) -> pl.DataFrame:
    path = table_path(silver_root=silver_root, table_name="trades")
    if not path.exists():
        return _empty_trades_frame()

    start_date, end_date = derive_trading_date_bounds(
        exchange=exchange,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
    )
    lf = pl.scan_delta(str(path)).filter(
        (pl.col("exchange") == exchange)
        & (pl.col("symbol").is_in(symbols))
        & (pl.col("trading_date") >= pl.lit(start_date))
        & (pl.col("trading_date") <= pl.lit(end_date))
        & (pl.col("ts_event_us") >= start_ts_us)
        & (pl.col("ts_event_us") < end_ts_us)
    )

    cols = ["exchange", "symbol", "symbol_id", "ts_event_us", "price", "qty", "file_id", "file_seq"]
    return (
        lf.select(cols).collect().sort(["exchange", "symbol", "ts_event_us", "file_id", "file_seq"])
    )


def _empty_trades_frame() -> pl.DataFrame:
    schema = TRADES.to_polars()
    cols = ["exchange", "symbol", "symbol_id", "ts_event_us", "price", "qty", "file_id", "file_seq"]
    return pl.DataFrame(schema={col: schema[col] for col in cols})


def empty_spine_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "exchange": pl.Utf8,
            "symbol": pl.Utf8,
            "symbol_id": DIM_SYMBOL.to_polars()["symbol_id"],
            "ts_spine_us": pl.Int64,
        }
    )


def _enforce_max_rows(df: pl.DataFrame, *, max_rows: int) -> None:
    if df.height > max_rows:
        raise RuntimeError(f"Spine would generate too many rows: {df.height} > {max_rows}")
