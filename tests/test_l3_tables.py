from __future__ import annotations

from datetime import date

import polars as pl

from pointline.tables.l3_orders import (
    L3_ORDERS_SCHEMA,
    normalize_l3_orders_schema,
    validate_l3_orders,
)
from pointline.tables.l3_ticks import L3_TICKS_SCHEMA, normalize_l3_ticks_schema, validate_l3_ticks


def test_normalize_l3_orders_derives_trading_phase() -> None:
    df = pl.DataFrame(
        {
            "date": [date(2024, 9, 30)],
            "exchange": ["szse"],
            "exchange_id": [30],
            "symbol_id": [1001],
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
            "exchange_id": [30],
            "symbol_id": [1001],
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
            "exchange_id": [31],
            # 2024-09-30 14:58:00 Asia/Shanghai in UTC microseconds
            "ts_local_us": [1727679480000000],
            "symbol_id": [2001],
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
            "exchange_id": [30],
            "symbol_id": [1001],
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
