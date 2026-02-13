from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.schemas.events import TRADES
from pointline.v2.research.discovery import discover_symbols
from pointline.v2.research.query import load_events
from pointline.v2.storage.delta.dimension_store import DeltaDimensionStore
from pointline.v2.storage.delta.layout import table_path


def _seed_dim_symbol(silver_root: Path) -> None:
    max_until = 2**63 - 1
    dim = pl.DataFrame(
        {
            "symbol_id": [11, 22, 33],
            "exchange": ["binance-futures", "binance-futures", "binance-futures"],
            "exchange_symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "canonical_symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "market_type": ["perpetual", "perpetual", "perpetual"],
            "base_asset": ["BTC", "BTC", "ETH"],
            "quote_asset": ["USDT", "USDT", "USDT"],
            "valid_from_ts_us": [1_000_000, 2_000_000, 2_000_000],
            "valid_until_ts_us": [2_000_000, max_until, max_until],
            "is_current": [False, True, True],
            "tick_size": [100, 200, 300],
            "lot_size": [1_000, 2_000, 3_000],
            "contract_size": [None, None, None],
            "updated_at_ts_us": [2_000_000, 2_000_000, 2_000_000],
        },
        schema=DIM_SYMBOL.to_polars(),
    )
    DeltaDimensionStore(silver_root=silver_root).save_dim_symbol(dim)


def _seed_trades_table(silver_root: Path) -> None:
    trades = pl.DataFrame(
        {
            "exchange": ["binance-futures", "binance-futures", "binance-futures"],
            "trading_date": [date(1970, 1, 1), date(1970, 1, 1), date(1970, 1, 1)],
            "symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "symbol_id": [22, 22, 33],
            "ts_event_us": [2_100_000, 2_100_000, 2_100_000],
            "ts_local_us": [2_100_000, 2_100_000, 2_100_000],
            "file_id": [1, 1, 1],
            "file_seq": [2, 1, 3],
            "side": ["buy", "buy", "sell"],
            "is_buyer_maker": [False, False, True],
            "price": [100_000_000_000, 100_000_000_000, 50_000_000_000],
            "qty": [10_000_000_000, 20_000_000_000, 30_000_000_000],
        },
        schema=TRADES.to_polars(),
    )
    path = table_path(silver_root=silver_root, table_name="trades")
    write_deltalake(
        str(path), trades.to_arrow(), mode="overwrite", partition_by=["exchange", "trading_date"]
    )


def test_discover_symbols_current_without_meta(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_symbol(silver_root)

    out = discover_symbols(
        silver_root=silver_root,
        exchange="binance-futures",
        q="btc",
        limit=10,
    )

    assert out.height == 1
    assert out.item(0, "symbol_id") == 22
    assert "tick_size" not in out.columns


def test_discover_symbols_as_of_with_meta(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_symbol(silver_root)

    out = discover_symbols(
        silver_root=silver_root,
        exchange="binance-futures",
        as_of=1_500_000,
        include_meta=True,
        limit=10,
    )

    assert out.height == 1
    assert out.item(0, "symbol_id") == 11
    assert out.item(0, "tick_size") == 100


def test_load_events_no_implicit_symbol_meta_join(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_symbol(silver_root)
    _seed_trades_table(silver_root)

    out = load_events(
        silver_root=silver_root,
        table="trades",
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=2_000_000,
        end=2_200_000,
    )

    assert out.height == 2
    assert out["qty"].to_list() == [20_000_000_000, 10_000_000_000]
    assert "tick_size" not in out.columns
    assert "lot_size" not in out.columns


def test_load_events_with_explicit_symbol_meta_columns(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_symbol(silver_root)
    _seed_trades_table(silver_root)

    out = load_events(
        silver_root=silver_root,
        table="trades",
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=2_000_000,
        end=2_200_000,
        include_lineage=True,
        symbol_meta_columns=["tick_size", "lot_size"],
    )

    assert out.height == 2
    assert "file_id" in out.columns
    assert "file_seq" in out.columns
    assert "tick_size" in out.columns
    assert "lot_size" in out.columns
    assert out["tick_size"].to_list() == [200, 200]
    assert out["lot_size"].to_list() == [2_000, 2_000]


def test_load_events_rejects_non_event_table(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_symbol(silver_root)

    with pytest.raises(ValueError, match="event tables"):
        load_events(
            silver_root=silver_root,
            table="dim_symbol",
            exchange="binance-futures",
            symbol="BTCUSDT",
            start=2_000_000,
            end=2_200_000,
        )
