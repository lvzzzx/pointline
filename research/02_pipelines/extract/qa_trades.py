"""QA checks for trade streams (Binance tick data)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pointline import research
from pointline.tables.trades import SIDE_BUY, SIDE_SELL


@dataclass
class QaResult:
    symbol_id: int
    start_ts_us: int
    end_ts_us: int
    rows: int
    dup_trade_id: int | None
    non_monotonic_ts: int
    mean_next_ret_buy: float | None
    mean_next_ret_sell: float | None
    order_by: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QA checks for trades")
    parser.add_argument("--symbol-id", type=int, required=True)
    parser.add_argument("--start-ts-us", type=int, required=True)
    parser.add_argument("--end-ts-us", type=int, required=True)
    parser.add_argument("--ts-col", type=str, default="ts_local_us")
    parser.add_argument("--out", type=Path)
    return parser.parse_args()


def _load_trades(symbol_id: int, start_ts_us: int, end_ts_us: int, ts_col: str) -> pl.DataFrame:
    columns = [
        "symbol_id",
        ts_col,
        "side",
        "price_int",
        "qty_int",
        "trade_id",
        "file_id",
        "file_line_number",
    ]
    return research.load_trades(
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        ts_col=ts_col,
        columns=columns,
        lazy=False,
    )


def _dedup_count(trades: pl.DataFrame) -> int | None:
    if "trade_id" not in trades.columns:
        return None
    if trades["trade_id"].null_count() == trades.height:
        return None
    dupes = trades.group_by("trade_id").len().filter(pl.col("len") > 1)
    return int(dupes.select(pl.col("len") - 1).sum()[0, 0])


def _non_monotonic_ts(trades: pl.DataFrame, ts_col: str) -> tuple[int, str]:
    if trades.height < 2:
        return 0, ts_col
    if (
        "file_id" in trades.columns
        and "file_line_number" in trades.columns
        and trades["file_id"].null_count() < trades.height
    ):
        # Check monotonicity within each file; avoid cross-file ordering assumptions.
        diffs = trades.select(
            pl.col(ts_col)
            .sort_by("file_line_number")
            .diff()
            .over("file_id")
            .alias("ts_diff")
        )["ts_diff"]
        return int(diffs.lt(0).fill_null(False).sum()), "per-file file_line_number"
    ordered = trades.sort(ts_col)
    diffs = ordered.select(pl.col(ts_col).diff()).to_series()
    return int(diffs.lt(0).fill_null(False).sum()), ts_col


def _side_sanity(trades: pl.DataFrame) -> tuple[float | None, float | None]:
    if trades.height < 3:
        return None, None
    if "price_int" not in trades.columns or "qty_int" not in trades.columns:
        return None, None
    # Use price changes between adjacent trades as a rough sanity check.
    df = trades.select([
        pl.col("side"),
        pl.col("price_int").cast(pl.Float64).alias("price"),
    ]).with_columns(pl.col("price").shift(-1).alias("price_next"))
    df = df.with_columns(((pl.col("price_next") - pl.col("price")) / pl.col("price")).alias("ret"))
    mean_buy = (
        df.filter(pl.col("side") == SIDE_BUY)
        .select(pl.col("ret").mean())
        .item()
    )
    mean_sell = (
        df.filter(pl.col("side") == SIDE_SELL)
        .select(pl.col("ret").mean())
        .item()
    )
    return mean_buy, mean_sell


def run_qa(symbol_id: int, start_ts_us: int, end_ts_us: int, ts_col: str) -> QaResult:
    trades = _load_trades(symbol_id, start_ts_us, end_ts_us, ts_col)
    non_monotonic, order_by = _non_monotonic_ts(trades, ts_col)
    return QaResult(
        symbol_id=symbol_id,
        start_ts_us=start_ts_us,
        end_ts_us=end_ts_us,
        rows=trades.height,
        dup_trade_id=_dedup_count(trades),
        non_monotonic_ts=non_monotonic,
        mean_next_ret_buy=_side_sanity(trades)[0],
        mean_next_ret_sell=_side_sanity(trades)[1],
        order_by=order_by,
    )


def _format_report(result: QaResult) -> str:
    start = datetime.fromtimestamp(result.start_ts_us / 1_000_000, tz=timezone.utc)
    end = datetime.fromtimestamp(result.end_ts_us / 1_000_000, tz=timezone.utc)
    return (
        "qa_report\n"
        f"symbol_id: {result.symbol_id}\n"
        f"start: {start.isoformat()}\n"
        f"end: {end.isoformat()}\n"
        f"rows: {result.rows}\n"
        f"dup_trade_id: {result.dup_trade_id}\n"
        f"order_by: {result.order_by}\n"
        f"non_monotonic_ts: {result.non_monotonic_ts}\n"
        f"mean_next_ret_buy: {result.mean_next_ret_buy}\n"
        f"mean_next_ret_sell: {result.mean_next_ret_sell}\n"
    )


def main() -> None:
    args = _parse_args()
    result = run_qa(args.symbol_id, args.start_ts_us, args.end_ts_us, args.ts_col)
    report = _format_report(result)
    if args.out:
        args.out.write_text(report)
    else:
        print(report)


if __name__ == "__main__":
    main()
