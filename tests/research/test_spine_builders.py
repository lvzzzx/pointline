from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake

from pointline.research.spine import (
    DollarSpineConfig,
    TradesSpineConfig,
    VolumeSpineConfig,
    build_spine,
)
from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.schemas.events import TRADES
from pointline.schemas.types import QTY_SCALE
from pointline.storage.delta.dimension_store import DeltaDimensionStore
from pointline.storage.delta.layout import table_path


def _seed_dim_current(silver_root: Path) -> None:
    max_until = 2**63 - 1
    dim = pl.DataFrame(
        {
            "symbol_id": [2001, 3001],
            "exchange": ["binance-futures", "binance-futures"],
            "exchange_symbol": ["BTCUSDT", "ETHUSDT"],
            "canonical_symbol": ["BTCUSDT", "ETHUSDT"],
            "market_type": ["perpetual", "perpetual"],
            "base_asset": ["BTC", "ETH"],
            "quote_asset": ["USDT", "USDT"],
            "valid_from_ts_us": [0, 0],
            "valid_until_ts_us": [max_until, max_until],
            "is_current": [True, True],
            "tick_size": [100, 100],
            "lot_size": [1_000, 1_000],
            "contract_size": [None, None],
            "updated_at_ts_us": [0, 0],
        },
        schema=DIM_SYMBOL.to_polars(),
    )
    DeltaDimensionStore(silver_root=silver_root).save_dim_symbol(dim)


def _seed_trades_for_bars(silver_root: Path) -> None:
    # price/qty are canonical scaled ints (1e9)
    trades = pl.DataFrame(
        {
            "exchange": [
                "binance-futures",
                "binance-futures",
                "binance-futures",
                "binance-futures",
                "binance-futures",
            ],
            "trading_date": [
                date(1970, 1, 1),
                date(1970, 1, 1),
                date(1970, 1, 1),
                date(1970, 1, 1),
                date(1970, 1, 1),
            ],
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT", "ETHUSDT"],
            "symbol_id": [2001, 2001, 2001, 2001, 3001],
            "ts_event_us": [10, 20, 20, 30, 15],
            "ts_local_us": [10, 20, 20, 30, 15],
            "file_id": [1, 1, 1, 1, 1],
            "file_seq": [1, 1, 2, 3, 1],
            "trade_id": [None, None, None, None, None],
            "side": ["buy", "buy", "buy", "sell", "buy"],
            "is_buyer_maker": [False, False, False, True, False],
            "price": [
                10 * QTY_SCALE,
                10 * QTY_SCALE,
                10 * QTY_SCALE,
                20 * QTY_SCALE,
                30 * QTY_SCALE,
            ],
            "qty": [
                40 * QTY_SCALE,
                70 * QTY_SCALE,
                10 * QTY_SCALE,
                80 * QTY_SCALE,
                5 * QTY_SCALE,
            ],
        },
        schema=TRADES.to_polars(),
    )
    path = table_path(silver_root=silver_root, table_name="trades")
    write_deltalake(
        str(path), trades.to_arrow(), mode="overwrite", partition_by=["exchange", "trading_date"]
    )


def test_trades_spine_deduplicates_same_timestamp(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_current(silver_root)
    _seed_trades_for_bars(silver_root)

    out = build_spine(
        silver_root=silver_root,
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=0,
        end=100,
        builder="trades",
        config=TradesSpineConfig(),
    )

    assert out["ts_spine_us"].to_list() == [10, 20, 30]


def test_volume_spine_threshold_crossings(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_current(silver_root)
    _seed_trades_for_bars(silver_root)

    out = build_spine(
        silver_root=silver_root,
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=0,
        end=100,
        builder="volume",
        config=VolumeSpineConfig(volume_threshold_scaled=100 * QTY_SCALE),
    )

    # cum qty crosses 100 at ts=20, then 200 at ts=30
    assert out["ts_spine_us"].to_list() == [20, 30]


def test_dollar_spine_threshold_crossings(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_current(silver_root)
    _seed_trades_for_bars(silver_root)

    out = build_spine(
        silver_root=silver_root,
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=0,
        end=100,
        builder="dollar",
        config=DollarSpineConfig(dollar_threshold_scaled=1_000 * QTY_SCALE),
    )

    # notionals (scaled): 400, 700, 100, 400 => crosses 1000 at ts=20, 2000 at ts=30
    assert out["ts_spine_us"].to_list() == [20, 30]


def test_volume_and_dollar_require_positive_thresholds(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_current(silver_root)
    _seed_trades_for_bars(silver_root)

    with pytest.raises(ValueError, match="volume_threshold_scaled must be > 0"):
        build_spine(
            silver_root=silver_root,
            exchange="binance-futures",
            symbol="BTCUSDT",
            start=0,
            end=100,
            builder="volume",
            config=VolumeSpineConfig(volume_threshold_scaled=0),
        )

    with pytest.raises(ValueError, match="dollar_threshold_scaled must be > 0"):
        build_spine(
            silver_root=silver_root,
            exchange="binance-futures",
            symbol="BTCUSDT",
            start=0,
            end=100,
            builder="dollar",
            config=DollarSpineConfig(dollar_threshold_scaled=0),
        )
