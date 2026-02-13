from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE
from pointline.vendors.tardis import (
    parse_tardis_incremental_l2,
    parse_tardis_quotes,
    parse_tardis_trades,
)


def test_parse_tardis_trades_scales_and_maps_fields() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1_700_000_000_000_100],
            "local_timestamp": [1_700_000_000_000_200],
            "id": ["12345"],
            "side": ["BUY"],
            "price": [43_210.123456789],
            "amount": [0.005],
        }
    )

    out = parse_tardis_trades(raw)
    assert out.columns == [
        "symbol",
        "exchange",
        "ts_event_us",
        "ts_local_us",
        "trade_id",
        "side",
        "is_buyer_maker",
        "price",
        "qty",
    ]
    assert out["symbol"][0] == "BTCUSDT"
    assert out["exchange"][0] == "binance-futures"
    assert out["ts_event_us"][0] == 1_700_000_000_000_100
    assert out["ts_local_us"][0] == 1_700_000_000_000_200
    assert out["trade_id"][0] == "12345"
    assert out["side"][0] == "buy"
    assert out["is_buyer_maker"][0] is None
    assert out["price"][0] == int(round(43_210.123456789 * PRICE_SCALE))
    assert out["qty"][0] == int(round(0.005 * QTY_SCALE))


def test_parse_tardis_trades_falls_back_to_local_timestamp() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "side": ["unknown"],
            "local_timestamp": [1_700_000_000_000_999],
            "price": [10.0],
            "amount": [1.0],
        }
    )

    out = parse_tardis_trades(raw)
    assert out["ts_event_us"][0] == 1_700_000_000_000_999
    assert out["ts_local_us"][0] == 1_700_000_000_000_999
    assert out["trade_id"][0] is None


def test_parse_tardis_trades_multi_symbol() -> None:
    """Grouped-symbol file: one DataFrame with multiple instruments."""
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "timestamp": [1_700_000_000_000_100, 1_700_000_000_000_200],
            "local_timestamp": [1_700_000_000_000_150, 1_700_000_000_000_250],
            "id": ["t-1", "t-2"],
            "side": ["buy", "sell"],
            "price": [42_000.0, 2_200.0],
            "amount": [0.25, 1.5],
        }
    )

    out = parse_tardis_trades(raw)
    assert out["symbol"].to_list() == ["BTCUSDT", "ETHUSDT"]
    assert out["exchange"].to_list() == ["binance-futures", "binance-futures"]
    assert out["price"].to_list() == [
        int(round(42_000.0 * PRICE_SCALE)),
        int(round(2_200.0 * PRICE_SCALE)),
    ]


def test_parse_tardis_quotes_scales_and_maps_sequence() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1_700_000_000_010_000],
            "local_timestamp": [1_700_000_000_020_000],
            "bid_price": [100.25],
            "bid_amount": [2.0],
            "ask_price": [100.30],
            "ask_amount": [3.5],
            "sequence_number": [333],
        }
    )

    out = parse_tardis_quotes(raw)
    assert out["exchange"][0] == "binance-futures"
    assert out["symbol"][0] == "BTCUSDT"
    assert out["bid_price"][0] == int(round(100.25 * PRICE_SCALE))
    assert out["bid_qty"][0] == int(round(2.0 * QTY_SCALE))
    assert out["ask_price"][0] == int(round(100.30 * PRICE_SCALE))
    assert out["ask_qty"][0] == int(round(3.5 * QTY_SCALE))
    assert out["seq_num"][0] == 333


def test_parse_tardis_quotes_keeps_row_level_exchange_and_symbol() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures", "okx-futures"],
            "symbol": ["BTCUSDT", "ETH-USDT-SWAP"],
            "timestamp": [1_700_000_000_010_000, 1_700_000_000_020_000],
            "bid_price": [100.0, 200.0],
            "bid_amount": [1.0, 2.0],
            "ask_price": [101.0, 201.0],
            "ask_amount": [1.5, 2.5],
        }
    )

    out = parse_tardis_quotes(raw)
    assert out["exchange"].to_list() == ["binance-futures", "okx-futures"]
    assert out["symbol"].to_list() == ["BTCUSDT", "ETH-USDT-SWAP"]


def test_parse_tardis_incremental_l2_scales_and_maps_book_seq() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1_700_000_000_100_000],
            "local_timestamp": [1_700_000_000_100_500],
            "is_snapshot": [False],
            "side": ["ASK"],
            "price": [101.5],
            "amount": [0.0],
            "update_id": [987],
        }
    )

    out = parse_tardis_incremental_l2(raw)
    assert out.columns == [
        "symbol",
        "exchange",
        "ts_event_us",
        "ts_local_us",
        "book_seq",
        "side",
        "price",
        "qty",
        "is_snapshot",
    ]
    assert out["exchange"][0] == "binance-futures"
    assert out["symbol"][0] == "BTCUSDT"
    assert out["book_seq"][0] == 987
    assert out["side"][0] == "ask"
    assert out["price"][0] == int(round(101.5 * PRICE_SCALE))
    assert out["qty"][0] == 0
    assert out["is_snapshot"][0] is False


def test_parse_tardis_incremental_l2_multi_symbol() -> None:
    """Grouped-symbol file: one DataFrame with multiple instruments."""
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "timestamp": [1_700_000_000_100_000, 1_700_000_000_200_000],
            "local_timestamp": [1_700_000_000_100_500, 1_700_000_000_200_500],
            "is_snapshot": [False, True],
            "side": ["bid", "ask"],
            "price": [42_000.0, 2_200.0],
            "amount": [1.0, 0.5],
            "update_id": [100, 101],
        }
    )

    out = parse_tardis_incremental_l2(raw)
    assert out["symbol"].to_list() == ["BTCUSDT", "ETHUSDT"]
    assert out["exchange"].to_list() == ["binance-futures", "binance-futures"]
    assert out["is_snapshot"].to_list() == [False, True]
