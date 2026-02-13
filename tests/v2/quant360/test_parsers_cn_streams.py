from __future__ import annotations

import polars as pl
import pytest

from pointline.v2.vendors.quant360 import (
    parse_l2_snapshot_stream,
    parse_order_stream,
    parse_tick_stream,
)


def test_parse_order_stream_szse() -> None:
    raw = pl.DataFrame(
        {
            "ApplSeqNum": [10],
            "Side": [1],
            "OrdType": [2],
            "Price": [10.23],
            "OrderQty": [100],
            "TransactTime": [20240102093000123],
            "ChannelNo": [3],
        }
    )
    out = parse_order_stream(raw, exchange="szse", symbol="000001")

    assert out["symbol"][0] == "000001"
    assert out["appl_seq_num"][0] == 10
    assert out["side_raw"][0] == "1"
    assert out["ord_type_raw"][0] == "2"
    assert out["qty_raw"][0] == 100


def test_parse_order_stream_sse_symbol_mismatch_raises() -> None:
    raw = pl.DataFrame(
        {
            "SecurityID": ["600000"],
            "TransactTime": [20240102093000123],
            "OrderNo": [1],
            "Price": [10.0],
            "Balance": [100],
            "OrderBSFlag": ["B"],
            "OrdType": ["A"],
            "OrderIndex": [1],
            "ChannelNo": [1],
            "BizIndex": [1],
        }
    )

    with pytest.raises(ValueError, match="Symbol mismatch"):
        parse_order_stream(raw, exchange="sse", symbol="600001")


def test_parse_tick_stream_sse_sets_exec_type_fill() -> None:
    raw = pl.DataFrame(
        {
            "SecurityID": ["600000"],
            "TradeTime": [20240102093000123],
            "TradePrice": [10.01],
            "TradeQty": [200],
            "BuyNo": [1001],
            "SellNo": [1002],
            "TradeIndex": [9],
            "ChannelNo": [2],
            "TradeBSFlag": ["B"],
            "BizIndex": [88],
            "ApplSeqNum": [8899],
        }
    )
    out = parse_tick_stream(raw, exchange="sse", symbol="600000")

    assert out["exec_type_raw"][0] == "F"
    assert out["trade_bs_flag_raw"][0] == "B"
    assert out["bid_appl_seq_num"][0] == 1001
    assert out["offer_appl_seq_num"][0] == 1002


def test_parse_tick_stream_szse_uses_exec_type_field() -> None:
    raw = pl.DataFrame(
        {
            "ApplSeqNum": [123],
            "BidApplSeqNum": [11],
            "OfferApplSeqNum": [12],
            "Price": [0.0],
            "Qty": [50],
            "ExecType": ["4"],
            "TransactTime": [20240102093000123],
            "ChannelNo": [6],
        }
    )
    out = parse_tick_stream(raw, exchange="szse", symbol="000001")
    assert out["exec_type_raw"][0] == "4"
    assert out["qty_raw"][0] == 50


def test_parse_l2_snapshot_stream_parses_levels() -> None:
    raw = pl.DataFrame(
        {
            "MsgSeqNum": [999],
            "SendingTime": [20240102093000123],
            "QuotTime": [20240102093000123],
            "ImageStatus": ["0"],
            "TradingPhaseCode": ["T0"],
            "BidPrice": ["[11.63,11.62,11.61,11.60,11.59,11.58,11.57,11.56,11.55,11.54]"],
            "BidOrderQty": [
                "[254100,476700,492500,1323400,283700,332700,243200,624700,484400,187400]"
            ],
            "OfferPrice": ["[11.64,11.65,11.66,11.67,11.68,11.69,11.70,11.71,11.72,11.73]"],
            "OfferOrderQty": ["[111,222,333,444,555,666,777,888,999,1111]"],
        }
    )

    out = parse_l2_snapshot_stream(raw, exchange="szse", symbol="000001")
    assert out["msg_seq_num"][0] == 999
    assert len(out["bid_price_levels"][0]) == 10
    assert len(out["ask_qty_levels"][0]) == 10


def test_parse_l2_snapshot_stream_rejects_bad_depth_vector() -> None:
    raw = pl.DataFrame(
        {
            "MsgSeqNum": [1],
            "QuotTime": [20240102093000123],
            "BidPrice": ["[11.63,11.62]"],
            "BidOrderQty": ["[1,2,3]"],
            "OfferPrice": ["[11.64,11.65]"],
            "OfferOrderQty": ["[1,2]"],
        }
    )

    with pytest.raises(ValueError, match="expected 10 levels"):
        parse_l2_snapshot_stream(raw, exchange="szse", symbol="000001")
