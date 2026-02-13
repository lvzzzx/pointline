"""CN (SSE/SZSE) trading-phase classification for query-time research filtering."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from zoneinfo import ZoneInfo

import polars as pl

from pointline.ingestion.exchange import get_exchange_timezone


class TradingPhase(str, Enum):
    """Canonical CN trading phases for research filtering."""

    CLOSED = "CLOSED"
    PRE_OPEN = "PRE_OPEN"
    MORNING = "MORNING"
    NOON_BREAK = "NOON_BREAK"
    AFTERNOON = "AFTERNOON"
    CLOSING = "CLOSING"
    AFTER_HOURS = "AFTER_HOURS"


_CN_EXCHANGES: set[str] = {"sse", "szse"}
_AFTER_HOURS_MARKET_TYPES: set[str] = {
    "star_board",
    "growth_board",
    "star",
    "chinext",
}


def classify_phase(
    *,
    ts_event_us: int,
    exchange: str,
    market_type: str | None = None,
) -> TradingPhase:
    """Classify one UTC microsecond timestamp into a CN trading phase."""
    exchange_norm = _normalize_cn_exchange(exchange)
    tz = ZoneInfo(get_exchange_timezone(exchange_norm))
    dt_local = datetime.fromtimestamp(ts_event_us / 1_000_000, tz=timezone.utc).astimezone(tz)

    minute_of_day = dt_local.hour * 60 + dt_local.minute
    return _phase_from_minute(
        minute_of_day=minute_of_day,
        exchange=exchange_norm,
        market_type=market_type,
    )


def add_phase_column(
    df: pl.DataFrame,
    *,
    exchange: str,
    ts_col: str = "ts_event_us",
    market_type: str | None = None,
    out_col: str = "trading_phase",
) -> pl.DataFrame:
    """Add a CN trading-phase string column using vectorized Polars expressions."""
    exchange_norm = _normalize_cn_exchange(exchange)
    if ts_col not in df.columns:
        raise ValueError(f"missing timestamp column: {ts_col}")

    tz = get_exchange_timezone(exchange_norm)
    local_dt = (
        pl.from_epoch(pl.col(ts_col).cast(pl.Int64), time_unit="us")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone(tz)
    )
    minute_of_day = (
        local_dt.dt.hour().cast(pl.Int32) * 60 + local_dt.dt.minute().cast(pl.Int32)
    ).cast(pl.Int32)
    phase_expr = _phase_expr(
        minute_of_day=minute_of_day,
        exchange=exchange_norm,
        market_type=market_type,
    )
    return df.with_columns(phase_expr.alias(out_col))


def filter_by_phase(
    df: pl.DataFrame,
    *,
    exchange: str,
    phases: list[TradingPhase | str],
    ts_col: str = "ts_event_us",
    market_type: str | None = None,
    phase_col: str = "trading_phase",
    keep_phase_col: bool = False,
) -> pl.DataFrame:
    """Filter rows by one or more CN trading phases."""
    wanted = [_coerce_phase_name(item) for item in phases]
    if not wanted:
        raise ValueError("phases must be non-empty")

    framed = add_phase_column(
        df,
        exchange=exchange,
        ts_col=ts_col,
        market_type=market_type,
        out_col=phase_col,
    )
    out = framed.filter(pl.col(phase_col).is_in(wanted))
    if keep_phase_col:
        return out
    return out.drop(phase_col)


def _normalize_cn_exchange(exchange: str) -> str:
    normalized = exchange.strip().lower()
    if normalized not in _CN_EXCHANGES:
        supported = ", ".join(sorted(_CN_EXCHANGES))
        raise ValueError(
            f"CN trading phases only support exchanges {{{supported}}}, got {exchange!r}"
        )
    return normalized


def _supports_after_hours(market_type: str | None) -> bool:
    return bool(market_type and market_type.strip().lower() in _AFTER_HOURS_MARKET_TYPES)


def _phase_from_minute(
    *,
    minute_of_day: int,
    exchange: str,
    market_type: str | None,
) -> TradingPhase:
    if 9 * 60 + 15 <= minute_of_day < 9 * 60 + 25:
        return TradingPhase.PRE_OPEN
    if 9 * 60 + 30 <= minute_of_day < 11 * 60 + 30:
        return TradingPhase.MORNING
    if 11 * 60 + 30 <= minute_of_day < 13 * 60:
        return TradingPhase.NOON_BREAK
    if 13 * 60 <= minute_of_day < 14 * 60 + 57:
        return TradingPhase.AFTERNOON
    if exchange == "szse" and 14 * 60 + 57 <= minute_of_day < 15 * 60:
        return TradingPhase.CLOSING
    if _supports_after_hours(market_type) and 15 * 60 + 5 <= minute_of_day < 15 * 60 + 30:
        return TradingPhase.AFTER_HOURS
    return TradingPhase.CLOSED


def _phase_expr(*, minute_of_day: pl.Expr, exchange: str, market_type: str | None) -> pl.Expr:
    expr = (
        pl.when((minute_of_day >= 9 * 60 + 15) & (minute_of_day < 9 * 60 + 25))
        .then(pl.lit(TradingPhase.PRE_OPEN.value))
        .when((minute_of_day >= 9 * 60 + 30) & (minute_of_day < 11 * 60 + 30))
        .then(pl.lit(TradingPhase.MORNING.value))
        .when((minute_of_day >= 11 * 60 + 30) & (minute_of_day < 13 * 60))
        .then(pl.lit(TradingPhase.NOON_BREAK.value))
        .when((minute_of_day >= 13 * 60) & (minute_of_day < 14 * 60 + 57))
        .then(pl.lit(TradingPhase.AFTERNOON.value))
    )
    if exchange == "szse":
        expr = expr.when((minute_of_day >= 14 * 60 + 57) & (minute_of_day < 15 * 60)).then(
            pl.lit(TradingPhase.CLOSING.value)
        )
    if _supports_after_hours(market_type):
        expr = expr.when((minute_of_day >= 15 * 60 + 5) & (minute_of_day < 15 * 60 + 30)).then(
            pl.lit(TradingPhase.AFTER_HOURS.value)
        )
    return expr.otherwise(pl.lit(TradingPhase.CLOSED.value))


def _coerce_phase_name(value: TradingPhase | str) -> str:
    if isinstance(value, TradingPhase):
        return value.value
    if isinstance(value, str):
        token = value.strip().upper()
        try:
            return TradingPhase[token].value
        except KeyError as exc:
            known = ", ".join(phase.name for phase in TradingPhase)
            raise ValueError(f"Unknown trading phase {value!r}. Known: {known}") from exc
    raise TypeError(f"phase must be TradingPhase|str, got {type(value).__name__}")
