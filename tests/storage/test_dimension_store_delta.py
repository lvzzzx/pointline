from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.storage.delta.dimension_store import DeltaDimensionStore


def _dim_symbol_row() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol_id": [1],
            "exchange": ["binance-futures"],
            "exchange_symbol": ["BTCUSDT"],
            "canonical_symbol": ["BTCUSDT"],
            "market_type": ["perpetual"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
            "valid_from_ts_us": [1_600_000_000_000_000],
            "valid_until_ts_us": [1_900_000_000_000_000],
            "is_current": [True],
            "tick_size": [None],
            "lot_size": [None],
            "contract_size": [None],
            "updated_at_ts_us": [1_700_000_000_000_000],
        },
        schema=DIM_SYMBOL.to_polars(),
    )


def test_dimension_store_returns_empty_when_missing(tmp_path: Path) -> None:
    store = DeltaDimensionStore(silver_root=tmp_path / "silver")
    df = store.load_dim_symbol()
    assert df.is_empty()
    assert dict(df.schema) == DIM_SYMBOL.to_polars()


def test_dimension_store_reads_dim_symbol(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    table_path = silver_root / "dim_symbol"
    table_path.parent.mkdir(parents=True, exist_ok=True)

    write_deltalake(str(table_path), _dim_symbol_row().to_arrow(), mode="overwrite")

    store = DeltaDimensionStore(silver_root=silver_root)
    df = store.load_dim_symbol()
    assert df.height == 1
    assert df.item(0, "exchange_symbol") == "BTCUSDT"


def test_dimension_store_save_dim_symbol_roundtrip(tmp_path: Path) -> None:
    store = DeltaDimensionStore(silver_root=tmp_path / "silver")
    version = store.save_dim_symbol(_dim_symbol_row())

    assert version == 0
    assert store.current_version() == 0

    loaded = store.load_dim_symbol()
    assert loaded.height == 1
    assert loaded.item(0, "exchange_symbol") == "BTCUSDT"


def test_dimension_store_save_dim_symbol_checks_expected_version(tmp_path: Path) -> None:
    store = DeltaDimensionStore(silver_root=tmp_path / "silver")
    store.save_dim_symbol(_dim_symbol_row())

    with pytest.raises(ValueError, match="version mismatch"):
        store.save_dim_symbol(_dim_symbol_row(), expected_version=7)


def test_dimension_store_save_dim_symbol_validates_invariants(tmp_path: Path) -> None:
    store = DeltaDimensionStore(silver_root=tmp_path / "silver")
    invalid = _dim_symbol_row().with_columns(
        pl.lit(1_600_000_000_000_000).cast(pl.Int64).alias("valid_until_ts_us")
    )

    with pytest.raises(ValueError, match="valid_until_ts_us must be > valid_from_ts_us"):
        store.save_dim_symbol(invalid)
