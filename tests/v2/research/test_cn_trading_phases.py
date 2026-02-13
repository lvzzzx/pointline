from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from deltalake import write_deltalake

from pointline.schemas.events import TRADES
from pointline.v2.research.cn_trading_phases import (
    TradingPhase,
    add_phase_column,
    classify_phase,
    filter_by_phase,
)
from pointline.v2.research.query import load_events
from pointline.v2.storage.delta.layout import table_path


def _cn_ts_us(hour: int, minute: int) -> int:
    dt_local = datetime(2024, 1, 2, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai"))
    return int(dt_local.astimezone(timezone.utc).timestamp() * 1_000_000)


def _seed_szse_phase_trades(silver_root: Path) -> dict[str, int]:
    phase_ts = {
        "pre_open": _cn_ts_us(9, 20),
        "morning": _cn_ts_us(9, 31),
        "noon_break": _cn_ts_us(11, 35),
        "afternoon": _cn_ts_us(13, 10),
        "closing": _cn_ts_us(14, 58),
        "after_hours": _cn_ts_us(15, 10),
    }
    ts_values = list(phase_ts.values())
    trades = pl.DataFrame(
        {
            "exchange": ["szse"] * len(ts_values),
            "trading_date": [date(2024, 1, 2)] * len(ts_values),
            "symbol": ["300001"] * len(ts_values),
            "symbol_id": [300001] * len(ts_values),
            "ts_event_us": ts_values,
            "ts_local_us": ts_values,
            "file_id": [77] * len(ts_values),
            "file_seq": list(range(1, len(ts_values) + 1)),
            "side": ["buy"] * len(ts_values),
            "is_buyer_maker": [False] * len(ts_values),
            "price": [100_000_000_000] * len(ts_values),
            "qty": [10_000_000_000] * len(ts_values),
        },
        schema=TRADES.to_polars(),
    )
    path = table_path(silver_root=silver_root, table_name="trades")
    write_deltalake(
        str(path), trades.to_arrow(), mode="overwrite", partition_by=["exchange", "trading_date"]
    )
    return phase_ts


def test_classify_phase_sse_szse_boundaries() -> None:
    ts_closing = _cn_ts_us(14, 58)
    ts_after_hours = _cn_ts_us(15, 10)

    assert classify_phase(ts_event_us=ts_closing, exchange="sse") == TradingPhase.CLOSED
    assert classify_phase(ts_event_us=ts_closing, exchange="szse") == TradingPhase.CLOSING
    assert (
        classify_phase(ts_event_us=ts_after_hours, exchange="szse", market_type="growth_board")
        == TradingPhase.AFTER_HOURS
    )
    assert classify_phase(ts_event_us=ts_after_hours, exchange="szse") == TradingPhase.CLOSED


def test_add_phase_column_and_filter_by_phase_contract() -> None:
    frame = pl.DataFrame(
        {
            "ts_event_us": [_cn_ts_us(9, 20), _cn_ts_us(9, 31), _cn_ts_us(14, 58)],
            "value": [1, 2, 3],
        }
    )

    with_phase = add_phase_column(frame, exchange="szse")
    assert with_phase["trading_phase"].to_list() == [
        TradingPhase.PRE_OPEN.value,
        TradingPhase.MORNING.value,
        TradingPhase.CLOSING.value,
    ]

    filtered = filter_by_phase(with_phase, exchange="szse", phases=[TradingPhase.CLOSING])
    assert filtered.columns == ["ts_event_us", "value"]
    assert filtered.height == 1
    assert filtered.item(0, "value") == 3

    kept = filter_by_phase(
        frame,
        exchange="szse",
        phases=["morning"],
        keep_phase_col=True,
    )
    assert kept.columns == ["ts_event_us", "value", "trading_phase"]
    assert kept.height == 1
    assert kept.item(0, "trading_phase") == TradingPhase.MORNING.value


def test_load_events_then_filter_by_phase_explicitly(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    phase_ts = _seed_szse_phase_trades(silver_root)

    start = _cn_ts_us(9, 0)
    end = _cn_ts_us(15, 30)

    loaded = load_events(
        silver_root=silver_root,
        table="trades",
        exchange="szse",
        symbol="300001",
        start=start,
        end=end,
    )
    assert loaded.height == len(phase_ts)

    closing = filter_by_phase(
        loaded,
        exchange="szse",
        phases=[TradingPhase.CLOSING],
    )
    assert closing.height == 1
    assert closing.item(0, "ts_event_us") == phase_ts["closing"]

    after_hours = filter_by_phase(
        loaded,
        exchange="szse",
        phases=[TradingPhase.AFTER_HOURS],
        market_type="growth_board",
    )
    assert after_hours.height == 1
    assert after_hours.item(0, "ts_event_us") == phase_ts["after_hours"]

    after_hours_without_market_type = filter_by_phase(
        loaded,
        exchange="szse",
        phases=[TradingPhase.AFTER_HOURS],
    )
    assert after_hours_without_market_type.is_empty()
