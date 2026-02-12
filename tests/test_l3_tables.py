from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from pointline.tables.l3_orders import (
    ALLOWED_EXCHANGES as L3_ORDERS_ALLOWED,
)
from pointline.tables.l3_orders import (
    L3_ORDERS_SCHEMA,
    normalize_l3_orders_schema,
    validate_l3_orders,
)
from pointline.tables.l3_ticks import (
    ALLOWED_EXCHANGES as L3_TICKS_ALLOWED,
)
from pointline.tables.l3_ticks import (
    L3_TICKS_SCHEMA,
    normalize_l3_ticks_schema,
    validate_l3_ticks,
)


def test_normalize_l3_orders_derives_trading_phase() -> None:
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["szse"],
            "symbol": ["000001.SZ"],
            # 2024-09-30 09:20:00 Asia/Shanghai in UTC microseconds
            "ts_local_us": [1727659200000000],
            "appl_seq_num": [1],
            "side": [0],
            "ord_type": [1],
            "px_int": [10000],
            "order_qty_int": [1],
            "channel_no": [1],
            "file_id": [1],
            "file_line_number": [1],
        }
    )
    normalized = normalize_l3_orders_schema(df)
    assert list(normalized.schema.keys()) == list(L3_ORDERS_SCHEMA.keys())
    assert normalized["trading_phase"][0] == 1


def test_validate_l3_orders_accepts_valid() -> None:
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["szse"],
            "symbol": ["000001.SZ"],
            "ts_local_us": [1727659800000000],
            "appl_seq_num": [2],
            "side": [1],
            "ord_type": [1],
            "px_int": [10000],
            "order_qty_int": [1],
            "channel_no": [1],
            "trading_phase": [2],
            "file_id": [1],
            "file_line_number": [1],
        },
        schema=L3_ORDERS_SCHEMA,
    )
    validated = validate_l3_orders(df)
    assert validated.height == 1


def test_normalize_l3_ticks_derives_trading_phase() -> None:
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["sse"],
            "symbol": ["600000.SH"],
            # 2024-09-30 14:58:00 Asia/Shanghai in UTC microseconds
            "ts_local_us": [1727679480000000],
            "appl_seq_num": [1],
            "bid_appl_seq_num": [123],
            "offer_appl_seq_num": [456],
            "exec_type": [0],
            "px_int": [3000],
            "qty_int": [1],
            "channel_no": [3],
            "file_id": [1],
            "file_line_number": [1],
        }
    )
    normalized = normalize_l3_ticks_schema(df)
    assert list(normalized.schema.keys()) == list(L3_TICKS_SCHEMA.keys())
    assert normalized["trading_phase"][0] == 3


def test_validate_l3_ticks_filters_invalid_tick_semantics() -> None:
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["szse"],
            "symbol": ["000001.SZ"],
            "ts_local_us": [1727659800000000],
            "appl_seq_num": [1],
            "bid_appl_seq_num": [1],
            "offer_appl_seq_num": [2],
            "exec_type": [1],  # cancel
            "px_int": [10],  # invalid for cancel; must be 0
            "qty_int": [1],
            "channel_no": [1],
            "trading_phase": [2],
            "file_id": [1],
            "file_line_number": [1],
        },
        schema=L3_TICKS_SCHEMA,
    )
    validated = validate_l3_ticks(df)
    assert validated.height == 0


def test_l3_allowed_exchanges_constants() -> None:
    """ALLOWED_EXCHANGES must be szse and sse only."""
    assert frozenset({"szse", "sse"}) == L3_ORDERS_ALLOWED
    assert frozenset({"szse", "sse"}) == L3_TICKS_ALLOWED


def test_validate_l3_orders_rejects_non_cn_exchange() -> None:
    """l3_orders validation must raise for exchanges outside SZSE/SSE."""
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "ts_local_us": [1727659800000000],
            "appl_seq_num": [1],
            "side": [0],
            "ord_type": [1],
            "px_int": [10000],
            "order_qty_int": [1],
            "channel_no": [1],
            "trading_phase": [2],
            "file_id": [1],
            "file_line_number": [1],
        },
        schema=L3_ORDERS_SCHEMA,
    )
    with pytest.raises(ValueError, match="not allowed"):
        validate_l3_orders(df)


def test_validate_l3_ticks_rejects_non_cn_exchange() -> None:
    """l3_ticks validation must raise for exchanges outside SZSE/SSE."""
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
            "ts_local_us": [1727659800000000],
            "appl_seq_num": [1],
            "bid_appl_seq_num": [1],
            "offer_appl_seq_num": [2],
            "exec_type": [0],
            "px_int": [10000],
            "qty_int": [1],
            "channel_no": [1],
            "trading_phase": [2],
            "file_id": [1],
            "file_line_number": [1],
        },
        schema=L3_TICKS_SCHEMA,
    )
    with pytest.raises(ValueError, match="not allowed"):
        validate_l3_ticks(df)
