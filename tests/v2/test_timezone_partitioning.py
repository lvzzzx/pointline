from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl

from pointline.v2.ingestion.timezone import derive_trading_date, derive_trading_date_frame


def _ts_us(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def test_derive_trading_date_uses_exchange_timezone() -> None:
    ts_utc = _ts_us(datetime(2024, 9, 29, 16, 30, 0, tzinfo=ZoneInfo("UTC")))
    assert derive_trading_date(ts_utc, "binance-futures") == date(2024, 9, 29)
    assert derive_trading_date(ts_utc, "szse") == date(2024, 9, 30)


def test_derive_trading_date_frame_adds_column_when_missing() -> None:
    df = pl.DataFrame(
        {
            "exchange": ["szse", "binance-futures"],
            "ts_event_us": [
                _ts_us(datetime(2024, 9, 29, 16, 30, 0, tzinfo=ZoneInfo("UTC"))),
                _ts_us(datetime(2024, 9, 29, 16, 30, 0, tzinfo=ZoneInfo("UTC"))),
            ],
        }
    )

    result = derive_trading_date_frame(df)

    assert result["trading_date"].to_list() == [date(2024, 9, 30), date(2024, 9, 29)]
