"""Shared time parsing and exchange-local date helpers for v2 research."""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from pointline.v2.ingestion.exchange import get_exchange_timezone

TimestampInput = int | str | date | datetime


def normalize_ts_us(value: TimestampInput, *, param_name: str) -> int:
    """Normalize supported time inputs to UTC microseconds."""
    if isinstance(value, int):
        return value

    dt: datetime
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{param_name}: invalid timestamp string {value!r}") from exc
    else:
        raise TypeError(f"{param_name} must be int|str|date|datetime, got {type(value).__name__}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def validate_time_window(start_ts_us: int, end_ts_us: int) -> None:
    """Validate [start, end) time window."""
    if end_ts_us <= start_ts_us:
        raise ValueError(f"end must be > start, got start={start_ts_us}, end={end_ts_us}")


def derive_trading_date_bounds(
    *,
    exchange: str,
    start_ts_us: int,
    end_ts_us: int,
) -> tuple[date, date]:
    """Derive inclusive local-date bounds for [start_ts_us, end_ts_us)."""
    validate_time_window(start_ts_us, end_ts_us)
    tz = ZoneInfo(get_exchange_timezone(exchange))

    start_local = datetime.fromtimestamp(start_ts_us / 1_000_000, tz=timezone.utc).astimezone(tz)
    end_local = datetime.fromtimestamp((end_ts_us - 1) / 1_000_000, tz=timezone.utc).astimezone(tz)
    return start_local.date(), end_local.date()
