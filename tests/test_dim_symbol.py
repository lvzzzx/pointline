import polars as pl

from src.dim_symbol import (
    DEFAULT_VALID_UNTIL_TS_US,
    required_dim_symbol_columns,
    required_update_columns,
    scd2_bootstrap,
    scd2_upsert,
)


def _base_updates(valid_from_ts: int, tick_size: float = 0.5) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "exchange_id": [1],
            "exchange_symbol": ["BTC-PERPETUAL"],
            "base_asset": ["BTC"],
            "quote_asset": ["USD"],
            "asset_type": [1],
            "tick_size": [tick_size],
            "lot_size": [1.0],
            "price_increment": [tick_size],
            "amount_increment": [0.1],
            "contract_size": [1.0],
            "valid_from_ts": [valid_from_ts],
        }
    )


def test_bootstrap_schema_and_flags():
    updates = _base_updates(100)
    dim = scd2_bootstrap(updates)

    assert set(required_dim_symbol_columns()) == set(dim.columns)
    assert dim.height == 1
    assert dim.select("is_current").item() is True
    assert dim.select("valid_until_ts").item() == DEFAULT_VALID_UNTIL_TS_US


def test_upsert_no_change_returns_same():
    updates = _base_updates(100)
    dim = scd2_bootstrap(updates)
    dim2 = scd2_upsert(dim, updates)

    assert dim2.height == 1
    assert dim2.select("is_current").item() is True


def test_upsert_change_closes_current_and_adds_new():
    dim = scd2_bootstrap(_base_updates(100))

    updates = _base_updates(200, tick_size=1.0)
    dim2 = scd2_upsert(dim, updates)

    assert dim2.height == 2

    current = dim2.filter(pl.col("is_current") == True)  # noqa: E712
    history = dim2.filter(pl.col("is_current") == False)  # noqa: E712

    assert current.height == 1
    assert history.height == 1

    assert history.select("valid_until_ts").item() == 200
    assert current.select("valid_from_ts").item() == 200


def test_required_update_columns_contract():
    cols = required_update_columns()
    assert "exchange_id" in cols
    assert "exchange_symbol" in cols
    assert "valid_from_ts" in cols


def test_resolve_symbol_ids_asof():
    from src.dim_symbol import resolve_symbol_ids

    # Setup dim with two versions of the same symbol
    dim = scd2_bootstrap(_base_updates(100, tick_size=0.5))
    dim = scd2_upsert(dim, _base_updates(200, tick_size=1.0))

    # Data to resolve
    data = pl.DataFrame(
        {
            "exchange_id": [1, 1, 1],
            "exchange_symbol": ["BTC-PERPETUAL", "BTC-PERPETUAL", "BTC-PERPETUAL"],
            "ts_local_us": [150, 250, 50],  # Middle, After, Before
        }
    )

    resolved = resolve_symbol_ids(data, dim)

    assert resolved.height == 3
    # ts=150 should match version starting at 100
    assert resolved.filter(pl.col("ts_local_us") == 150)["tick_size"][0] == 0.5
    # ts=250 should match version starting at 200
    assert resolved.filter(pl.col("ts_local_us") == 250)["tick_size"][0] == 1.0
    # ts=50 should have null symbol_id (or no match)
    assert resolved.filter(pl.col("ts_local_us") == 50)["symbol_id"][0] is None


def test_assign_symbol_id_hash_determinism():
    df = _base_updates(100)
    # The hash for (1, "BTC-PERPETUAL", 100) with blake2b(4 bytes) should be stable
    dim = scd2_bootstrap(df)
    symbol_id = dim["symbol_id"][0]
    
    # Asserting the specific value to ensure we don't break existing IDs
    assert symbol_id == 3019004731
    
    # Test with multiple rows
    df_multi = pl.concat([
        _base_updates(100),
        _base_updates(200),
    ])
    dim_multi = scd2_bootstrap(df_multi)
    assert dim_multi.height == 2
    assert dim_multi["symbol_id"][0] == 3019004731
    assert dim_multi["symbol_id"][1] != 3019004731


def test_upsert_empty_updates_returns_original():
    dim = scd2_bootstrap(_base_updates(100))
    # Empty updates with correct schema
    empty_updates = pl.DataFrame(schema=dim.select(required_update_columns()).schema)
    dim2 = scd2_upsert(dim, empty_updates)
    assert dim2.equals(dim)


def test_upsert_multiple_symbols_mixed_changes():
    # Symbol 1: change, Symbol 2: new, Symbol 3: no change
    dim = pl.concat([
        _base_updates(100),  # Symbol 1 (ID 1)
        _base_updates(100).with_columns(pl.lit("ETH-PERPETUAL").alias("exchange_symbol")),  # Symbol 3
    ])
    dim = scd2_bootstrap(dim)

    updates = pl.concat([
        _base_updates(200, tick_size=1.0),  # Change Symbol 1
        _base_updates(200).with_columns(pl.lit("SOL-PERPETUAL").alias("exchange_symbol")),  # New Symbol 2
        _base_updates(200).with_columns(pl.lit("ETH-PERPETUAL").alias("exchange_symbol")),  # No change Symbol 3
    ])

    dim2 = scd2_upsert(dim, updates)

    # Symbol 1 should have 2 rows (history + current)
    # Symbol 2 should have 1 row (current)
    # Symbol 3 should have 1 row (current)
    assert dim2.height == 4

    # Verify Symbol 1
    s1 = dim2.filter(pl.col("exchange_symbol") == "BTC-PERPETUAL")
    assert s1.height == 2
    assert s1.filter(pl.col("is_current"))["tick_size"][0] == 1.0

    # Verify Symbol 2
    s2 = dim2.filter(pl.col("exchange_symbol") == "SOL-PERPETUAL")
    assert s2.height == 1
    assert s2.select("is_current").item() is True

    # Verify Symbol 3
    s3 = dim2.filter(pl.col("exchange_symbol") == "ETH-PERPETUAL")
    assert s3.height == 1


def test_upsert_custom_valid_from_col():
    dim = scd2_bootstrap(_base_updates(100))
    updates = _base_updates(200, tick_size=1.0).rename({"valid_from_ts": "event_time"})
    
    dim2 = scd2_upsert(dim, updates, valid_from_col="event_time")
    assert dim2.height == 2
    assert dim2.filter(pl.col("is_current"))["valid_from_ts"][0] == 200


def test_upsert_missing_columns_raises_error():
    import pytest
    dim = scd2_bootstrap(_base_updates(100))
    bad_updates = pl.DataFrame({"exchange_id": [1]})
    with pytest.raises(ValueError, match="missing required columns"):
        scd2_upsert(dim, bad_updates)


def test_normalize_schema_missing_columns_raises_error():
    from src.dim_symbol import normalize_dim_symbol_schema
    import pytest
    bad_df = pl.DataFrame({"exchange_id": [1]})
    with pytest.raises(ValueError, match="missing required columns"):
        normalize_dim_symbol_schema(bad_df)
