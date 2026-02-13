"""Tests for v2 dim_symbol SCD2 logic."""

from __future__ import annotations

import polars as pl
import pytest

from pointline.dim_symbol import (
    VALID_UNTIL_MAX,
    assign_symbol_ids,
    bootstrap,
    upsert,
    validate,
)
from pointline.schemas.dimensions import DIM_SYMBOL

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_DEFAULTS: dict[str, object] = {
    "exchange": "binance-futures",
    "exchange_symbol": "BTCUSDT",
    "canonical_symbol": "BTC/USDT",
    "market_type": "perpetual",
    "base_asset": "BTC",
    "quote_asset": "USDT",
    "tick_size": 100,
    "lot_size": 1000,
    "contract_size": None,
}


def _make_snapshot(**overrides: object) -> pl.DataFrame:
    """Build a 1-row snapshot DataFrame."""
    row = {**_DEFAULTS, **overrides}
    return pl.DataFrame([row])


def _make_dim(rows: list[dict]) -> pl.DataFrame:
    """Build a dim_symbol DataFrame with all schema columns."""
    schema = DIM_SYMBOL.to_polars()
    if not rows:
        return pl.DataFrame(schema=schema)
    df = pl.DataFrame(rows)
    for col, dtype in schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
    return df.select([pl.col(c).cast(dtype) for c, dtype in schema.items()])


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_bootstrap_creates_current_rows(self):
        dim = bootstrap(_make_snapshot(), effective_ts_us=1000)
        assert dim.height == 1
        row = dim.row(0, named=True)
        assert row["is_current"] is True
        assert row["valid_from_ts_us"] == 1000
        assert row["valid_until_ts_us"] == VALID_UNTIL_MAX

    def test_bootstrap_assigns_deterministic_symbol_ids(self):
        snap = _make_snapshot()
        dim1 = bootstrap(snap, effective_ts_us=1000)
        dim2 = bootstrap(snap, effective_ts_us=1000)
        assert dim1["symbol_id"].to_list() == dim2["symbol_id"].to_list()

    def test_bootstrap_schema_matches_spec(self):
        dim = bootstrap(_make_snapshot(), effective_ts_us=1000)
        expected = DIM_SYMBOL.to_polars()
        assert dict(zip(dim.columns, dim.dtypes, strict=True)) == expected


# ---------------------------------------------------------------------------
# Upsert — implicit delisting (delistings=None)
# ---------------------------------------------------------------------------


class TestUpsertImplicit:
    def test_upsert_empty_dim_delegates_to_bootstrap(self):
        snap = _make_snapshot()
        empty = _make_dim([])
        result = upsert(empty, snap, effective_ts_us=1000)
        expected = bootstrap(snap, effective_ts_us=1000)
        assert result.equals(expected)

    def test_upsert_no_changes_returns_same(self):
        snap = _make_snapshot()
        dim = bootstrap(snap, effective_ts_us=1000)
        result = upsert(dim, snap, effective_ts_us=2000)
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["is_current"] is True
        assert row["valid_from_ts_us"] == 1000  # unchanged
        assert row["updated_at_ts_us"] == 1000  # unchanged

    def test_upsert_new_listing(self):
        snap1 = _make_snapshot()
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = pl.concat(
            [
                snap1,
                _make_snapshot(
                    exchange_symbol="ETHUSDT",
                    canonical_symbol="ETH/USDT",
                    base_asset="ETH",
                ),
            ]
        )
        result = upsert(dim, snap2, effective_ts_us=2000)
        assert result.height == 2
        assert result.filter(pl.col("is_current")).height == 2
        eth = result.filter(pl.col("exchange_symbol") == "ETHUSDT")
        assert eth.height == 1
        assert eth["valid_from_ts_us"][0] == 2000

    def test_upsert_changed_tracked_col(self):
        snap1 = _make_snapshot(tick_size=100)
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(tick_size=200)
        result = upsert(dim, snap2, effective_ts_us=2000)
        assert result.height == 2  # 1 closed + 1 new
        current = result.filter(pl.col("is_current"))
        history = result.filter(~pl.col("is_current"))
        assert current.height == 1
        assert history.height == 1
        assert current["tick_size"][0] == 200
        assert current["valid_from_ts_us"][0] == 2000
        assert history["valid_until_ts_us"][0] == 2000
        assert history["tick_size"][0] == 100

    def test_upsert_implicit_delist(self):
        snap1 = _make_snapshot()
        dim = bootstrap(snap1, effective_ts_us=1000)
        # Snapshot for same exchange but different symbol → BTC missing = delisted
        snap2 = _make_snapshot(
            exchange_symbol="ETHUSDT",
            canonical_symbol="ETH/USDT",
            base_asset="ETH",
        )
        result = upsert(dim, snap2, effective_ts_us=2000)
        btc = result.filter(pl.col("exchange_symbol") == "BTCUSDT")
        assert btc.height == 1
        assert btc["is_current"][0] is False
        assert btc["valid_until_ts_us"][0] == 2000
        eth = result.filter(pl.col("exchange_symbol") == "ETHUSDT")
        assert eth.height == 1
        assert eth["is_current"][0] is True

    def test_upsert_mixed_new_changed_delisted(self):
        snap1 = pl.concat(
            [
                _make_snapshot(exchange_symbol="BTCUSDT", tick_size=100),
                _make_snapshot(
                    exchange_symbol="ETHUSDT",
                    canonical_symbol="ETH/USDT",
                    base_asset="ETH",
                    tick_size=50,
                ),
                _make_snapshot(
                    exchange_symbol="SOLUSDT",
                    canonical_symbol="SOL/USDT",
                    base_asset="SOL",
                    tick_size=10,
                ),
            ]
        )
        dim = bootstrap(snap1, effective_ts_us=1000)

        # BTC: tick_size changed; ETH: removed (delisted); SOL: unchanged; DOT: new
        snap2 = pl.concat(
            [
                _make_snapshot(exchange_symbol="BTCUSDT", tick_size=200),
                _make_snapshot(
                    exchange_symbol="SOLUSDT",
                    canonical_symbol="SOL/USDT",
                    base_asset="SOL",
                    tick_size=10,
                ),
                _make_snapshot(
                    exchange_symbol="DOTUSDT",
                    canonical_symbol="DOT/USDT",
                    base_asset="DOT",
                    tick_size=1,
                ),
            ]
        )
        result = upsert(dim, snap2, effective_ts_us=2000)

        # BTC: 1 closed + 1 current = 2
        btc = result.filter(pl.col("exchange_symbol") == "BTCUSDT").sort("valid_from_ts_us")
        assert btc.height == 2
        assert btc["is_current"][0] is False
        assert btc["is_current"][1] is True
        assert btc["tick_size"][1] == 200

        # ETH: 1 closed
        eth = result.filter(pl.col("exchange_symbol") == "ETHUSDT")
        assert eth.height == 1
        assert eth["is_current"][0] is False
        assert eth["valid_until_ts_us"][0] == 2000

        # SOL: unchanged
        sol = result.filter(pl.col("exchange_symbol") == "SOLUSDT")
        assert sol.height == 1
        assert sol["is_current"][0] is True

        # DOT: new
        dot = result.filter(pl.col("exchange_symbol") == "DOTUSDT")
        assert dot.height == 1
        assert dot["is_current"][0] is True
        assert dot["valid_from_ts_us"][0] == 2000

    def test_upsert_per_exchange_scoping(self):
        """Deribit untouched when snapshot only has binance-futures."""
        snap1 = pl.concat(
            [
                _make_snapshot(exchange="binance-futures", exchange_symbol="BTCUSDT"),
                _make_snapshot(exchange="deribit", exchange_symbol="BTC-PERPETUAL"),
            ]
        )
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(exchange="binance-futures", exchange_symbol="BTCUSDT")
        result = upsert(dim, snap2, effective_ts_us=2000)
        deribit = result.filter(pl.col("exchange") == "deribit")
        assert deribit.height == 1
        assert deribit["is_current"][0] is True
        assert deribit["valid_from_ts_us"][0] == 1000  # unchanged

    def test_upsert_null_to_value_detected_as_change(self):
        snap1 = _make_snapshot(tick_size=None)
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(tick_size=100)
        result = upsert(dim, snap2, effective_ts_us=2000)
        assert result.height == 2
        current = result.filter(pl.col("is_current"))
        assert current["tick_size"][0] == 100


# ---------------------------------------------------------------------------
# Upsert — explicit delisting (delistings=DataFrame)
# ---------------------------------------------------------------------------


class TestUpsertExplicit:
    def test_upsert_explicit_delist_with_vendor_date(self):
        snap1 = _make_snapshot()
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(
            exchange_symbol="ETHUSDT",
            canonical_symbol="ETH/USDT",
            base_asset="ETH",
        )
        delistings = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "exchange_symbol": ["BTCUSDT"],
                "delisted_at_ts_us": [1500],
            }
        )
        result = upsert(dim, snap2, effective_ts_us=2000, delistings=delistings)
        btc = result.filter(pl.col("exchange_symbol") == "BTCUSDT")
        assert btc.height == 1
        assert btc["is_current"][0] is False
        assert btc["valid_until_ts_us"][0] == 1500  # vendor date, not effective_ts_us

    def test_upsert_explicit_delist_missing_from_both_untouched(self):
        snap1 = pl.concat(
            [
                _make_snapshot(exchange_symbol="BTCUSDT"),
                _make_snapshot(
                    exchange_symbol="ETHUSDT",
                    canonical_symbol="ETH/USDT",
                    base_asset="ETH",
                ),
            ]
        )
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(exchange_symbol="BTCUSDT")
        delistings = pl.DataFrame(
            schema={"exchange": pl.Utf8, "exchange_symbol": pl.Utf8, "delisted_at_ts_us": pl.Int64}
        )
        result = upsert(dim, snap2, effective_ts_us=2000, delistings=delistings)
        # ETH not in snapshot, not in delistings → stays current
        eth = result.filter(pl.col("exchange_symbol") == "ETHUSDT")
        assert eth.height == 1
        assert eth["is_current"][0] is True

    def test_upsert_explicit_and_new_together(self):
        snap1 = _make_snapshot(exchange_symbol="BTCUSDT")
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(
            exchange_symbol="ETHUSDT",
            canonical_symbol="ETH/USDT",
            base_asset="ETH",
        )
        delistings = pl.DataFrame(
            {
                "exchange": ["binance-futures"],
                "exchange_symbol": ["BTCUSDT"],
                "delisted_at_ts_us": [1500],
            }
        )
        result = upsert(dim, snap2, effective_ts_us=2000, delistings=delistings)
        assert result.height == 2
        btc = result.filter(pl.col("exchange_symbol") == "BTCUSDT")
        assert btc["is_current"][0] is False
        assert btc["valid_until_ts_us"][0] == 1500
        eth = result.filter(pl.col("exchange_symbol") == "ETHUSDT")
        assert eth["is_current"][0] is True

    def test_upsert_explicit_delist_scoping(self):
        """Only delists within exchanges present in delistings."""
        snap1 = pl.concat(
            [
                _make_snapshot(exchange="binance-futures", exchange_symbol="BTCUSDT"),
                _make_snapshot(exchange="deribit", exchange_symbol="BTC-PERPETUAL"),
            ]
        )
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(exchange="binance-futures", exchange_symbol="BTCUSDT")
        delistings = pl.DataFrame(
            {
                "exchange": ["deribit"],
                "exchange_symbol": ["BTC-PERPETUAL"],
                "delisted_at_ts_us": [1800],
            }
        )
        result = upsert(dim, snap2, effective_ts_us=2000, delistings=delistings)
        # binance-futures BTC unchanged
        btc_bf = result.filter(
            (pl.col("exchange") == "binance-futures") & (pl.col("exchange_symbol") == "BTCUSDT")
        )
        assert btc_bf.height == 1
        assert btc_bf["is_current"][0] is True
        # deribit BTC-PERPETUAL closed at 1800
        btc_dr = result.filter(
            (pl.col("exchange") == "deribit") & (pl.col("exchange_symbol") == "BTC-PERPETUAL")
        )
        assert btc_dr.height == 1
        assert btc_dr["is_current"][0] is False
        assert btc_dr["valid_until_ts_us"][0] == 1800


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------


class TestValidate:
    def test_validate_good_dim(self):
        dim = bootstrap(_make_snapshot(), effective_ts_us=1000)
        validate(dim)  # should not raise

    def test_validate_bad_window(self):
        dim = bootstrap(_make_snapshot(), effective_ts_us=1000)
        dim = dim.with_columns(pl.lit(500).cast(pl.Int64).alias("valid_until_ts_us"))
        with pytest.raises(ValueError, match="valid_until_ts_us"):
            validate(dim)

    def test_validate_duplicate_current(self):
        row = {
            "symbol_id": 1,
            "exchange": "binance-futures",
            "exchange_symbol": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "market_type": "perpetual",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "valid_from_ts_us": 1000,
            "valid_until_ts_us": 2000,
            "is_current": True,
            "tick_size": 100,
            "lot_size": 1000,
            "contract_size": None,
            "updated_at_ts_us": 1000,
        }
        row2 = {
            **row,
            "symbol_id": 2,
            "valid_from_ts_us": 2000,
            "valid_until_ts_us": VALID_UNTIL_MAX,
        }
        dim = _make_dim([row, row2])
        with pytest.raises(ValueError, match="is_current"):
            validate(dim)

    def test_validate_overlapping_windows(self):
        row1 = {
            "symbol_id": 1,
            "exchange": "binance-futures",
            "exchange_symbol": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "market_type": "perpetual",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "valid_from_ts_us": 1000,
            "valid_until_ts_us": 3000,
            "is_current": False,
            "tick_size": 100,
            "lot_size": 1000,
            "contract_size": None,
            "updated_at_ts_us": 1000,
        }
        row2 = {
            **row1,
            "symbol_id": 2,
            "valid_from_ts_us": 2000,
            "valid_until_ts_us": VALID_UNTIL_MAX,
            "is_current": True,
        }
        dim = _make_dim([row1, row2])
        with pytest.raises(ValueError, match="Overlapping"):
            validate(dim)

    def test_validate_duplicate_symbol_id(self):
        row1 = {
            "symbol_id": 42,
            "exchange": "binance-futures",
            "exchange_symbol": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "market_type": "perpetual",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "valid_from_ts_us": 1000,
            "valid_until_ts_us": VALID_UNTIL_MAX,
            "is_current": True,
            "tick_size": 100,
            "lot_size": 1000,
            "contract_size": None,
            "updated_at_ts_us": 1000,
        }
        row2 = {
            **row1,
            "exchange_symbol": "ETHUSDT",
            "canonical_symbol": "ETH/USDT",
            "base_asset": "ETH",
        }
        dim = _make_dim([row1, row2])
        with pytest.raises(ValueError, match="symbol_id"):
            validate(dim)

    def test_validate_empty_dim(self):
        validate(_make_dim([]))  # should not raise


# ---------------------------------------------------------------------------
# History preservation
# ---------------------------------------------------------------------------


class TestHistoryPreservation:
    def test_upsert_preserves_history_rows(self):
        snap1 = _make_snapshot(tick_size=100)
        dim = bootstrap(snap1, effective_ts_us=1000)
        snap2 = _make_snapshot(tick_size=200)
        dim = upsert(dim, snap2, effective_ts_us=2000)
        # Now upsert again with same tick_size=200 → no change
        snap3 = _make_snapshot(tick_size=200)
        result = upsert(dim, snap3, effective_ts_us=3000)
        # History row from first upsert should still be there
        assert result.height == 2  # 1 history + 1 current
        history = result.filter(~pl.col("is_current"))
        assert history.height == 1
        assert history["tick_size"][0] == 100

    def test_multiple_upserts_build_history(self):
        snap1 = _make_snapshot(tick_size=100)
        dim = bootstrap(snap1, effective_ts_us=1000)

        snap2 = _make_snapshot(tick_size=200)
        dim = upsert(dim, snap2, effective_ts_us=2000)

        snap3 = _make_snapshot(tick_size=300)
        dim = upsert(dim, snap3, effective_ts_us=3000)

        # 3 versions: 2 closed + 1 current
        assert dim.height == 3
        current = dim.filter(pl.col("is_current"))
        assert current.height == 1
        assert current["tick_size"][0] == 300
        assert current["valid_from_ts_us"][0] == 3000

        history = dim.filter(~pl.col("is_current")).sort("valid_from_ts_us")
        assert history.height == 2
        assert history["tick_size"].to_list() == [100, 200]
        assert history["valid_until_ts_us"].to_list() == [2000, 3000]


# ---------------------------------------------------------------------------
# assign_symbol_ids
# ---------------------------------------------------------------------------


class TestAssignSymbolIds:
    def test_deterministic(self):
        snap = _make_snapshot()
        dim = bootstrap(snap, effective_ts_us=1000)
        sid = dim["symbol_id"][0]
        dim2 = bootstrap(snap, effective_ts_us=1000)
        assert dim2["symbol_id"][0] == sid

    def test_different_inputs_different_ids(self):
        dim1 = bootstrap(_make_snapshot(exchange_symbol="BTCUSDT"), effective_ts_us=1000)
        dim2 = bootstrap(_make_snapshot(exchange_symbol="ETHUSDT"), effective_ts_us=1000)
        assert dim1["symbol_id"][0] != dim2["symbol_id"][0]

    def test_public_api_assigns_ids(self):
        df = _make_snapshot().with_columns(
            pl.lit(1000).cast(pl.Int64).alias("valid_from_ts_us"),
        )
        result = assign_symbol_ids(df)
        assert "symbol_id" in result.columns
        assert result["symbol_id"].null_count() == 0
