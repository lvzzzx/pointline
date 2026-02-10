"""Trading phase derivation utilities for CN exchange market microstructure tables."""

from __future__ import annotations

import polars as pl

TRADING_PHASE_UNKNOWN = 0
TRADING_PHASE_OPENING_CALL = 1
TRADING_PHASE_CONTINUOUS = 2
TRADING_PHASE_CLOSING_CALL = 3

_HOUR_US = 3_600_000_000
_MIN_US = 60_000_000
_DAY_US = 86_400_000_000
_CST_OFFSET_US = 8 * _HOUR_US


def derive_cn_trading_phase_expr(
    *, ts_col: str = "ts_local_us", exchange_col: str = "exchange"
) -> pl.Expr:
    """Return Polars expression deriving trading phase from UTC timestamp + exchange.

    Notes:
    - Quant360 CN L3 feed does not provide explicit session phase.
    - We derive from local exchange time in Asia/Shanghai (UTC+8, no DST).
    - Rules are intentionally conservative; unmatched timestamps become UNKNOWN.
    """

    tod_us = ((pl.col(ts_col) + _CST_OFFSET_US) % _DAY_US).alias("_tod_us")

    open_start = 9 * _HOUR_US + 15 * _MIN_US
    open_end = 9 * _HOUR_US + 25 * _MIN_US

    am_cont_start = 9 * _HOUR_US + 30 * _MIN_US
    am_cont_end = 11 * _HOUR_US + 30 * _MIN_US
    pm_cont_start = 13 * _HOUR_US
    pm_cont_end = 14 * _HOUR_US + 57 * _MIN_US

    close_start = 14 * _HOUR_US + 57 * _MIN_US
    close_end = 15 * _HOUR_US

    cn_exchanges = ["szse", "sse"]
    return (
        pl.when(pl.col(exchange_col).is_in(cn_exchanges))
        .then(
            pl.when((tod_us >= open_start) & (tod_us < open_end))
            .then(pl.lit(TRADING_PHASE_OPENING_CALL, dtype=pl.UInt8))
            .when(
                ((tod_us >= am_cont_start) & (tod_us < am_cont_end))
                | ((tod_us >= pm_cont_start) & (tod_us < pm_cont_end))
            )
            .then(pl.lit(TRADING_PHASE_CONTINUOUS, dtype=pl.UInt8))
            .when((tod_us >= close_start) & (tod_us < close_end))
            .then(pl.lit(TRADING_PHASE_CLOSING_CALL, dtype=pl.UInt8))
            .otherwise(pl.lit(TRADING_PHASE_UNKNOWN, dtype=pl.UInt8))
        )
        .otherwise(pl.lit(TRADING_PHASE_UNKNOWN, dtype=pl.UInt8))
        .alias("trading_phase")
    )


__all__ = [
    "TRADING_PHASE_UNKNOWN",
    "TRADING_PHASE_OPENING_CALL",
    "TRADING_PHASE_CONTINUOUS",
    "TRADING_PHASE_CLOSING_CALL",
    "derive_cn_trading_phase_expr",
]
