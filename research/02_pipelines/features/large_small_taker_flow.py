"""Build large/small taker flow factors and 3h forward returns from trade prints."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pointline import research
from pointline.dim_symbol import read_dim_symbol_table
from pointline.tables.trades import SIDE_BUY, SIDE_SELL


@dataclass
class PipelineConfig:
    symbol_id: int
    start_ts_us: int
    end_ts_us: int
    ts_col: str
    bar_size: str
    quantile_window: str
    large_q: float
    small_q: float
    min_trades: int
    eps: float
    output: Path | None
    log_path: Path | None


def _parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Large vs small taker flow feature builder")
    parser.add_argument("--symbol-id", type=int, required=True)
    parser.add_argument("--start-ts-us", type=int, required=True)
    parser.add_argument("--end-ts-us", type=int, required=True)
    parser.add_argument("--ts-col", type=str, default="ts_local_us")
    parser.add_argument("--bar-size", type=str, default="5m")
    parser.add_argument("--quantile-window", type=str, default="72h")
    parser.add_argument("--large-q", type=float, default=0.85)
    parser.add_argument("--small-q", type=float, default=0.50)
    parser.add_argument("--min-trades", type=int, default=5000)
    parser.add_argument("--eps", type=float, default=1e-12)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-path", type=Path)
    args = parser.parse_args()
    return PipelineConfig(
        symbol_id=args.symbol_id,
        start_ts_us=args.start_ts_us,
        end_ts_us=args.end_ts_us,
        ts_col=args.ts_col,
        bar_size=args.bar_size,
        quantile_window=args.quantile_window,
        large_q=args.large_q,
        small_q=args.small_q,
        min_trades=args.min_trades,
        eps=args.eps,
        output=args.output,
        log_path=args.log_path,
    )


def _parse_bar_minutes(value: str) -> int:
    value = value.strip().lower()
    if value.endswith("m"):
        return int(value[:-1])
    if value.endswith("h"):
        return int(value[:-1]) * 60
    raise ValueError("bar-size must end with 'm' or 'h'")


def _load_trades_lazy(config: PipelineConfig) -> pl.LazyFrame:
    columns = [
        "symbol_id",
        "exchange_id",
        config.ts_col,
        "side",
        "price_int",
        "qty_int",
        "trade_id",
    ]
    return research.scan_table(
        "trades",
        symbol_id=config.symbol_id,
        start_ts_us=config.start_ts_us,
        end_ts_us=config.end_ts_us,
        ts_col=config.ts_col,
        columns=columns,
    )


def _attach_notional(lf: pl.LazyFrame, ts_col: str) -> pl.LazyFrame:
    dim_symbol = read_dim_symbol_table(
        columns=["symbol_id", "price_increment", "amount_increment"],
        unique_by=["symbol_id"],
    ).lazy()

    lf = lf.join(dim_symbol, on="symbol_id", how="left")
    lf = lf.with_columns(
        [
            (pl.col("price_int") * pl.col("price_increment")).alias("price"),
            (pl.col("qty_int") * pl.col("amount_increment")).alias("qty"),
        ]
    )
    lf = lf.with_columns(
        [
            pl.from_epoch(pl.col(ts_col), time_unit="us").alias("ts"),
            pl.when(pl.col("side") == SIDE_BUY)
            .then(1)
            .when(pl.col("side") == SIDE_SELL)
            .then(-1)
            .otherwise(0)
            .alias("sign"),
            (pl.col("price") * pl.col("qty")).alias("notional"),
            (pl.col("price") * pl.col("qty") * pl.col("sign")).alias("signed_notional"),
        ]
    )
    return lf.drop(["price_increment", "amount_increment"])


def _thresholds(
    trades: pl.LazyFrame,
    bar_size: str,
    window: str,
    large_q: float,
    small_q: float,
) -> pl.LazyFrame:
    thresholds = (
        trades.groupby_dynamic("ts", every=bar_size, period=window, closed="left", label="left")
        .agg(
            [
                pl.col("notional").quantile(large_q).alias("q_large"),
                pl.col("notional").quantile(small_q).alias("q_small"),
                pl.len().alias("trade_count"),
            ]
        )
        .sort("ts")
        # Shift to ensure thresholds come strictly from prior data (avoid lookahead).
        .with_columns(
            [
                pl.col("q_large").shift(1).alias("q_large"),
                pl.col("q_small").shift(1).alias("q_small"),
                pl.col("trade_count").shift(1).alias("trade_count"),
            ]
        )
        .rename({"ts": "bar_start"})
    )
    return thresholds


def _build_bars(config: PipelineConfig) -> pl.DataFrame:
    trades = _load_trades_lazy(config)
    trades = _attach_notional(trades, config.ts_col)
    trades = trades.with_columns(pl.col("ts").dt.truncate(config.bar_size).alias("bar_start"))

    thresholds = _thresholds(
        trades,
        config.bar_size,
        config.quantile_window,
        config.large_q,
        config.small_q,
    )

    trades = trades.join(thresholds, on="bar_start", how="left")
    trades = trades.filter(pl.col("trade_count") >= config.min_trades)
    trades = trades.filter(pl.col("q_large").is_not_null() & pl.col("q_small").is_not_null())

    trades = trades.with_columns(
        [
            (pl.col("notional") >= pl.col("q_large")).alias("is_large"),
            (pl.col("notional") <= pl.col("q_small")).alias("is_small"),
        ]
    )

    bars = trades.groupby("bar_start").agg(
        [
            pl.col("notional").sum().alias("A"),
            pl.col("signed_notional").sum().alias("N"),
            pl.col("notional").filter(pl.col("sign") > 0).sum().alias("B"),
            pl.col("notional").filter(pl.col("sign") < 0).sum().alias("S"),
            pl.len().alias("K"),
            pl.col("price").last().alias("close"),
            pl.col("notional").filter(pl.col("is_large")).sum().alias("A_L"),
            pl.col("signed_notional").filter(pl.col("is_large")).sum().alias("N_L"),
            pl.col("notional")
            .filter(pl.col("is_large") & (pl.col("sign") > 0))
            .sum()
            .alias("B_L"),
            pl.col("notional")
            .filter(pl.col("is_large") & (pl.col("sign") < 0))
            .sum()
            .alias("S_L"),
            pl.col("notional").filter(pl.col("is_small")).sum().alias("A_S"),
            pl.col("signed_notional").filter(pl.col("is_small")).sum().alias("N_S"),
            pl.col("notional")
            .filter(pl.col("is_small") & (pl.col("sign") > 0))
            .sum()
            .alias("B_S"),
            pl.col("notional")
            .filter(pl.col("is_small") & (pl.col("sign") < 0))
            .sum()
            .alias("S_S"),
            pl.col("notional").top_k(1).sum().alias("top1_notional"),
            pl.col("notional").top_k(3).sum().alias("top3_notional"),
            pl.col("notional").top_k(5).sum().alias("top5_notional"),
        ]
    )

    return bars.sort("bar_start").collect()


def _add_features(df: pl.DataFrame, config: PipelineConfig) -> pl.DataFrame:
    eps = config.eps
    bar_minutes = _parse_bar_minutes(config.bar_size)

    if 180 % bar_minutes != 0:
        raise ValueError("bar-size must divide 180 minutes for 3h horizon")

    w_30m = 30 // bar_minutes
    w_1h = 60 // bar_minutes
    w_3h = 180 // bar_minutes

    df = df.with_columns(
        [
            (pl.col("A").fill_null(0.0)).alias("A"),
            (pl.col("N").fill_null(0.0)).alias("N"),
            (pl.col("B").fill_null(0.0)).alias("B"),
            (pl.col("S").fill_null(0.0)).alias("S"),
            (pl.col("A_L").fill_null(0.0)).alias("A_L"),
            (pl.col("N_L").fill_null(0.0)).alias("N_L"),
            (pl.col("B_L").fill_null(0.0)).alias("B_L"),
            (pl.col("S_L").fill_null(0.0)).alias("S_L"),
            (pl.col("A_S").fill_null(0.0)).alias("A_S"),
            (pl.col("N_S").fill_null(0.0)).alias("N_S"),
            (pl.col("B_S").fill_null(0.0)).alias("B_S"),
            (pl.col("S_S").fill_null(0.0)).alias("S_S"),
        ]
    )

    df = df.with_columns(
        [
            (pl.col("N_L") / (pl.col("A") + eps)).alias("F_L1"),
            ((pl.col("B_L") - pl.col("S_L")) / (pl.col("B_L") + pl.col("S_L") + eps)).alias(
                "F_L2"
            ),
            ((pl.col("N_L") - pl.col("N_S")) / (pl.col("A") + eps)).alias("F_D"),
            (pl.col("N").abs() / (pl.col("A") + eps)).alias("F_T"),
            (pl.col("A_L") / (pl.col("A") + eps)).alias("F_P"),
            (pl.col("top1_notional") / (pl.col("A") + eps)).alias("F_C1"),
            (pl.col("top3_notional") / (pl.col("A") + eps)).alias("F_C3"),
            (pl.col("top5_notional") / (pl.col("A") + eps)).alias("F_C5"),
        ]
    )

    df = df.with_columns(
        [
            (pl.col("close").log() - pl.col("close").shift(1).log()).alias("ret_5m"),
            (pl.col("close").log() - pl.col("close").shift(w_1h).log()).alias("ret_1h"),
            (pl.col("close").log() - pl.col("close").shift(w_3h).log()).alias("ret_3h"),
            (pl.col("close").shift(-w_3h).log() - pl.col("close").log()).alias("ret_fwd_3h"),
        ]
    )

    df = df.with_columns(
        [
            (pl.col("ret_5m") ** 2).rolling_sum(w_1h).sqrt().alias("rv_1h"),
            (pl.col("ret_5m") ** 2).rolling_sum(w_3h).sqrt().alias("rv_3h"),
            (pl.col("A") + eps).log().alias("log_A"),
            pl.col("K").alias("trade_count"),
        ]
    )

    df = df.with_columns(pl.col("log_A").rolling_mean(w_1h).alias("log_A_mean_1h"))

    df = df.with_columns(
        [
            pl.col("F_L1").rolling_sum(w_30m).alias("F_L1_sum_30m"),
            pl.col("F_L1").rolling_sum(w_1h).alias("F_L1_sum_1h"),
            pl.col("F_L1").rolling_sum(w_3h).alias("F_L1_sum_3h"),
        ]
    )

    df = df.with_columns(
        [
            (pl.col("F_L1_sum_30m") - (pl.col("F_L1_sum_3h") / 6.0)).alias("F_L1_acc"),
        ]
    )

    return df


def _write_output(df: pl.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".csv":
        df.write_csv(output)
    else:
        df.write_parquet(output)


def _log_run(config: PipelineConfig, output: Path, rows: int) -> None:
    if config.log_path is None:
        return
    log_entry = {
        "run_id": datetime.now(tz=timezone.utc).isoformat(),
        "symbol_id": config.symbol_id,
        "start_ts_us": config.start_ts_us,
        "end_ts_us": config.end_ts_us,
        "ts_col": config.ts_col,
        "bar_size": config.bar_size,
        "quantile_window": config.quantile_window,
        "large_q": config.large_q,
        "small_q": config.small_q,
        "min_trades": config.min_trades,
        "output": str(output),
        "rows": rows,
    }
    config.log_path.parent.mkdir(parents=True, exist_ok=True)
    with config.log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(log_entry) + "\n")


def main() -> None:
    config = _parse_args()
    bars = _build_bars(config)
    features = _add_features(bars, config)
    if config.output:
        _write_output(features, config.output)
        _log_run(config, config.output, features.height)
    else:
        print(features.head(5))


if __name__ == "__main__":
    main()
