from __future__ import annotations

import polars as pl

from pointline.schemas.types import PRICE_SCALE, QTY_SCALE
from pointline.vendors.tardis import (
    parse_tardis_derivative_ticker,
    parse_tardis_incremental_l2,
    parse_tardis_liquidations,
    parse_tardis_options_chain,
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


# --- derivative_ticker ---


def test_parse_tardis_derivative_ticker_scales_prices_and_keeps_rates() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1_700_000_000_000_100],
            "local_timestamp": [1_700_000_000_000_200],
            "mark_price": [43_210.5],
            "index_price": [43_200.0],
            "last_price": [43_215.0],
            "open_interest": [1234.5],
            "funding_rate": [0.0001],
            "predicted_funding_rate": [0.00012],
            "funding_timestamp": [1_700_003_600_000_000],
        }
    )

    out = parse_tardis_derivative_ticker(raw)
    assert out.columns == [
        "symbol",
        "exchange",
        "ts_event_us",
        "ts_local_us",
        "mark_price",
        "index_price",
        "last_price",
        "open_interest",
        "funding_rate",
        "predicted_funding_rate",
        "funding_timestamp",
    ]
    assert out["exchange"][0] == "binance-futures"
    assert out["symbol"][0] == "BTCUSDT"
    assert out["ts_event_us"][0] == 1_700_000_000_000_100
    assert out["ts_local_us"][0] == 1_700_000_000_000_200
    assert out["mark_price"][0] == int(round(43_210.5 * PRICE_SCALE))
    assert out["index_price"][0] == int(round(43_200.0 * PRICE_SCALE))
    assert out["last_price"][0] == int(round(43_215.0 * PRICE_SCALE))
    assert out["open_interest"][0] == int(round(1234.5 * QTY_SCALE))
    assert out["funding_rate"][0] == 0.0001
    assert out["predicted_funding_rate"][0] == 0.00012
    assert out["funding_timestamp"][0] == 1_700_003_600_000_000


def test_parse_tardis_derivative_ticker_handles_optional_columns() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["deribit"],
            "symbol": ["BTC-PERPETUAL"],
            "timestamp": [1_700_000_000_000_100],
            "mark_price": [43_000.0],
        }
    )

    out = parse_tardis_derivative_ticker(raw)
    assert out["mark_price"][0] == int(round(43_000.0 * PRICE_SCALE))
    assert out["index_price"][0] is None
    assert out["last_price"][0] is None
    assert out["open_interest"][0] is None
    assert out["funding_rate"][0] is None
    assert out["predicted_funding_rate"][0] is None
    assert out["funding_timestamp"][0] is None
    assert out["ts_local_us"][0] is None


# --- liquidations ---


def test_parse_tardis_liquidations_scales_and_maps_fields() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["binance-futures"],
            "symbol": ["BTCUSDT"],
            "timestamp": [1_700_000_000_000_100],
            "local_timestamp": [1_700_000_000_000_200],
            "id": ["liq-1"],
            "side": ["SELL"],
            "price": [42_500.0],
            "amount": [0.1],
        }
    )

    out = parse_tardis_liquidations(raw)
    assert out.columns == [
        "symbol",
        "exchange",
        "ts_event_us",
        "ts_local_us",
        "liquidation_id",
        "side",
        "price",
        "qty",
    ]
    assert out["exchange"][0] == "binance-futures"
    assert out["symbol"][0] == "BTCUSDT"
    assert out["ts_event_us"][0] == 1_700_000_000_000_100
    assert out["ts_local_us"][0] == 1_700_000_000_000_200
    assert out["liquidation_id"][0] == "liq-1"
    assert out["side"][0] == "sell"
    assert out["price"][0] == int(round(42_500.0 * PRICE_SCALE))
    assert out["qty"][0] == int(round(0.1 * QTY_SCALE))


def test_parse_tardis_liquidations_handles_optional_id() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["okx"],
            "symbol": ["BTC-USDT-SWAP"],
            "timestamp": [1_700_000_000_000_100],
            "side": ["buy"],
            "price": [100.0],
            "amount": [1.0],
        }
    )

    out = parse_tardis_liquidations(raw)
    assert out["liquidation_id"][0] is None


# --- options_chain ---


def test_parse_tardis_options_chain_scales_and_maps_fields() -> None:
    """
    Validate that parse_tardis_options_chain maps input columns to the expected output schema and applies price/quantity scaling.
    
    Asserts that the output contains the expected columns and that:
    - exchange and symbol values are passed through,
    - `type` is normalized to lowercase as `option_type`,
    - strike and all price-like fields are scaled by PRICE_SCALE and converted to integers,
    - quantity-like fields (open_interest, bid_amount, ask_amount) are scaled by QTY_SCALE and converted to integers,
    - expiration is preserved as `expiration_ts_us`,
    - implied volatility fields (`bid_iv`, `ask_iv`, `mark_iv`) and greeks (`delta`, `gamma`, `vega`, `theta`, `rho`) are preserved.
    """
    raw = pl.DataFrame(
        {
            "exchange": ["deribit"],
            "symbol": ["BTC-20240126-42000-C"],
            "timestamp": [1_700_000_000_000_100],
            "local_timestamp": [1_700_000_000_000_200],
            "type": ["Call"],
            "strike_price": [42_000.0],
            "expiration": [1_706_284_800_000_000],
            "open_interest": [500.0],
            "last_price": [0.05],
            "bid_price": [0.045],
            "bid_amount": [10.0],
            "bid_iv": [0.55],
            "ask_price": [0.055],
            "ask_amount": [8.0],
            "ask_iv": [0.58],
            "mark_price": [0.05],
            "mark_iv": [0.56],
            "underlying_index": ["BTC"],
            "underlying_price": [43_000.0],
            "delta": [0.45],
            "gamma": [0.0001],
            "vega": [15.5],
            "theta": [-20.3],
            "rho": [0.5],
        }
    )

    out = parse_tardis_options_chain(raw)
    assert out.columns == [
        "symbol",
        "exchange",
        "ts_event_us",
        "ts_local_us",
        "option_type",
        "strike",
        "expiration_ts_us",
        "open_interest",
        "last_price",
        "bid_price",
        "bid_qty",
        "bid_iv",
        "ask_price",
        "ask_qty",
        "ask_iv",
        "mark_price",
        "mark_iv",
        "underlying_index",
        "underlying_price",
        "delta",
        "gamma",
        "vega",
        "theta",
        "rho",
    ]
    assert out["exchange"][0] == "deribit"
    assert out["option_type"][0] == "call"
    assert out["strike"][0] == int(round(42_000.0 * PRICE_SCALE))
    assert out["expiration_ts_us"][0] == 1_706_284_800_000_000
    assert out["open_interest"][0] == int(round(500.0 * QTY_SCALE))
    assert out["last_price"][0] == int(round(0.05 * PRICE_SCALE))
    assert out["bid_price"][0] == int(round(0.045 * PRICE_SCALE))
    assert out["bid_qty"][0] == int(round(10.0 * QTY_SCALE))
    assert out["bid_iv"][0] == 0.55
    assert out["ask_price"][0] == int(round(0.055 * PRICE_SCALE))
    assert out["ask_qty"][0] == int(round(8.0 * QTY_SCALE))
    assert out["ask_iv"][0] == 0.58
    assert out["mark_price"][0] == int(round(0.05 * PRICE_SCALE))
    assert out["mark_iv"][0] == 0.56
    assert out["underlying_index"][0] == "BTC"
    assert out["underlying_price"][0] == int(round(43_000.0 * PRICE_SCALE))
    assert out["delta"][0] == 0.45
    assert out["gamma"][0] == 0.0001
    assert out["vega"][0] == 15.5
    assert out["theta"][0] == -20.3
    assert out["rho"][0] == 0.5


def test_parse_tardis_options_chain_handles_minimal_columns() -> None:
    raw = pl.DataFrame(
        {
            "exchange": ["deribit"],
            "symbol": ["BTC-20240126-42000-P"],
            "timestamp": [1_700_000_000_000_100],
            "type": ["Put"],
            "strike_price": [42_000.0],
            "expiration": [1_706_284_800_000_000],
        }
    )

    out = parse_tardis_options_chain(raw)
    assert out["option_type"][0] == "put"
    assert out["strike"][0] == int(round(42_000.0 * PRICE_SCALE))
    assert out["open_interest"][0] is None
    assert out["bid_price"][0] is None
    assert out["delta"][0] is None
    assert out["underlying_index"][0] is None