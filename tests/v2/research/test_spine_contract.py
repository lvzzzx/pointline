from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.v2.research.spine import ClockSpineConfig, build_spine
from pointline.v2.storage.delta.dimension_store import DeltaDimensionStore


def _seed_dim_for_clock(silver_root: Path) -> None:
    max_until = 2**63 - 1
    dim = pl.DataFrame(
        {
            "symbol_id": [1001, 1002],
            "exchange": ["binance-futures", "binance-futures"],
            "exchange_symbol": ["BTCUSDT", "BTCUSDT"],
            "canonical_symbol": ["BTCUSDT", "BTCUSDT"],
            "market_type": ["perpetual", "perpetual"],
            "base_asset": ["BTC", "BTC"],
            "quote_asset": ["USDT", "USDT"],
            "valid_from_ts_us": [0, 100],
            "valid_until_ts_us": [100, max_until],
            "is_current": [False, True],
            "tick_size": [100, 200],
            "lot_size": [1_000, 2_000],
            "contract_size": [None, None],
            "updated_at_ts_us": [100, 100],
        },
        schema=DIM_SYMBOL.to_polars(),
    )
    DeltaDimensionStore(silver_root=silver_root).save_dim_symbol(dim)


def test_build_spine_clock_contract_and_symbol_id_windows(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_for_clock(silver_root)

    out = build_spine(
        silver_root=silver_root,
        exchange="binance-futures",
        symbol="BTCUSDT",
        start=0,
        end=200,
        builder="clock",
        config=ClockSpineConfig(step_us=50),
    )

    assert out.columns == ["exchange", "symbol", "symbol_id", "ts_spine_us"]
    assert out["ts_spine_us"].to_list() == [50, 100, 150, 200]
    assert out["symbol_id"].to_list() == [1001, 1001, 1002, 1002]


def test_build_spine_clock_enforces_max_rows(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_for_clock(silver_root)

    with pytest.raises(RuntimeError, match="too many rows"):
        build_spine(
            silver_root=silver_root,
            exchange="binance-futures",
            symbol="BTCUSDT",
            start=0,
            end=200,
            builder="clock",
            config=ClockSpineConfig(step_us=1, max_rows=10),
        )


def test_build_spine_rejects_unknown_builder(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    _seed_dim_for_clock(silver_root)

    with pytest.raises(ValueError, match="Unknown spine builder"):
        build_spine(
            silver_root=silver_root,
            exchange="binance-futures",
            symbol="BTCUSDT",
            start=date(1970, 1, 1),
            end=date(1970, 1, 2),
            builder="unknown",
            config=ClockSpineConfig(step_us=1_000_000),
        )
