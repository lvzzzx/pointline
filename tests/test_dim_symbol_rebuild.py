
import polars as pl
import pytest
from pointline.dim_symbol import (
    rebuild_from_history,
    check_coverage,
    DEFAULT_VALID_UNTIL_TS_US,
    required_dim_symbol_columns,
)

def _base_row(ts: int, tick_size: float = 0.5, symbol: str = "BTC-PERPETUAL"):
    return pl.DataFrame(
        {
            "exchange_id": [1],
            "exchange_symbol": [symbol],
            "base_asset": ["BTC"],
            "quote_asset": ["USD"],
            "asset_type": [1],
            "tick_size": [tick_size],
            "lot_size": [1.0],
            "price_increment": [tick_size],
            "amount_increment": [0.1],
            "contract_size": [1.0],
            "valid_from_ts": [ts],
        }
    )

def test_rebuild_from_history_single_chain():
    # History: t=100 (0.5), t=200 (1.0), t=300 (1.5)
    hist = pl.concat([
        _base_row(100, 0.5),
        _base_row(200, 1.0),
        _base_row(300, 1.5),
    ])
    
    result = rebuild_from_history(hist)
    
    assert result.height == 3
    assert set(result.columns) == set(required_dim_symbol_columns())
    
    # Check Row 1
    r1 = result.row(0, named=True)
    assert r1["valid_from_ts"] == 100
    assert r1["valid_until_ts"] == 200
    assert r1["is_current"] is False
    assert r1["tick_size"] == 0.5
    
    # Check Row 2
    r2 = result.row(1, named=True)
    assert r2["valid_from_ts"] == 200
    assert r2["valid_until_ts"] == 300
    assert r2["is_current"] is False
    
    # Check Row 3 (Current)
    r3 = result.row(2, named=True)
    assert r3["valid_from_ts"] == 300
    assert r3["valid_until_ts"] == DEFAULT_VALID_UNTIL_TS_US
    assert r3["is_current"] is True

def test_rebuild_from_history_multi_symbol():
    # Sym A: t=100
    # Sym B: t=100, t=150
    hist = pl.concat([
        _base_row(100, symbol="A"),
        _base_row(100, symbol="B"),
        _base_row(150, symbol="B"),
    ])
    
    result = rebuild_from_history(hist)
    assert result.height == 3
    
    # Verify Sym A
    a = result.filter(pl.col("exchange_symbol") == "A")
    assert a.height == 1
    assert a["is_current"][0] is True
    assert a["valid_until_ts"][0] == DEFAULT_VALID_UNTIL_TS_US
    
    # Verify Sym B
    b = result.filter(pl.col("exchange_symbol") == "B")
    assert b.height == 2
    # B1
    assert b["valid_from_ts"][0] == 100
    assert b["valid_until_ts"][0] == 150
    assert b["is_current"][0] is False
    # B2
    assert b["valid_from_ts"][1] == 150
    assert b["valid_until_ts"][1] == DEFAULT_VALID_UNTIL_TS_US
    assert b["is_current"][1] is True

def test_check_coverage_contiguous():
    # Setup: 100-200, 200-300, 300-MAX
    df = pl.DataFrame({
        "exchange_id": [1, 1, 1],
        "exchange_symbol": ["BTC", "BTC", "BTC"],
        "valid_from_ts": [100, 200, 300],
        "valid_until_ts": [200, 300, 999],
    })
    
    # Check range 150-250: Covered by Row 1 and Row 2
    assert check_coverage(df, 1, "BTC", 150, 250) is True
    
    # Check range 50-150: Fails (starts at 100)
    assert check_coverage(df, 1, "BTC", 50, 150) is False
    
    # Check range 250-400: Covered by Row 2 and Row 3
    assert check_coverage(df, 1, "BTC", 250, 400) is True

def test_check_coverage_gaps():
    # Gap: 100-200, 250-MAX (Missing 200-250)
    df = pl.DataFrame({
        "exchange_id": [1, 1],
        "exchange_symbol": ["BTC", "BTC"],
        "valid_from_ts": [100, 250],
        "valid_until_ts": [200, 999],
    })
    
    # Check range 150-300: Fails due to gap
    assert check_coverage(df, 1, "BTC", 150, 300) is False
    
    # Check range 100-200: OK
    assert check_coverage(df, 1, "BTC", 100, 200) is True
    
    # Check range 100-201: Fails
    assert check_coverage(df, 1, "BTC", 100, 201) is False

def test_check_coverage_missing_symbol():
    df = pl.DataFrame({
        "exchange_id": [1],
        "exchange_symbol": ["ETH"],
        "valid_from_ts": [100],
        "valid_until_ts": [200],
    })
    assert check_coverage(df, 1, "BTC", 100, 200) is False
