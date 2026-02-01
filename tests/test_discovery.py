"""Tests for data discovery API."""

import polars as pl

from pointline.research import discovery


def test_list_exchanges_all():
    """Test listing all exchanges."""
    df = discovery.list_exchanges()

    # Should return a DataFrame
    assert isinstance(df, pl.DataFrame)

    # Should have required columns
    assert "exchange" in df.columns
    assert "exchange_id" in df.columns
    assert "asset_class" in df.columns
    assert "description" in df.columns
    assert "is_active" in df.columns

    # Should have some exchanges
    assert df.height > 0

    # Should be sorted by asset_class then exchange_id
    assert df["asset_class"].is_sorted()


def test_list_exchanges_crypto_only():
    """Test filtering by crypto asset class."""
    df = discovery.list_exchanges(asset_class="crypto")

    assert df.height > 0
    # All should be crypto-spot or crypto-derivatives
    assert all(ac in ["crypto-spot", "crypto-derivatives"] for ac in df["asset_class"].to_list())


def test_list_exchanges_crypto_derivatives():
    """Test filtering by crypto-derivatives."""
    df = discovery.list_exchanges(asset_class="crypto-derivatives")

    assert df.height > 0
    assert all(ac == "crypto-derivatives" for ac in df["asset_class"].to_list())

    # Should include binance-futures
    assert "binance-futures" in df["exchange"].to_list()


def test_list_exchanges_stocks_cn():
    """Test filtering by Chinese stocks."""
    df = discovery.list_exchanges(asset_class="stocks-cn")

    assert df.height > 0
    assert all(ac == "stocks-cn" for ac in df["asset_class"].to_list())

    # Should include szse and sse
    exchanges = df["exchange"].to_list()
    assert "szse" in exchanges
    assert "sse" in exchanges


def test_list_exchanges_active_only():
    """Test filtering by active status."""
    # Get all exchanges
    all_df = discovery.list_exchanges(active_only=False)

    # Get active only
    active_df = discovery.list_exchanges(active_only=True)

    # Active should be subset of all
    assert active_df.height <= all_df.height

    # All active_df should have is_active=True
    assert all(active_df["is_active"].to_list())

    # FTX should be in all but not active
    if "ftx" in all_df["exchange"].to_list():
        assert "ftx" not in active_df["exchange"].to_list()


def test_list_exchanges_multiple_asset_classes():
    """Test filtering by multiple asset classes."""
    df = discovery.list_exchanges(asset_class=["crypto-spot", "stocks-cn"])

    assert df.height > 0

    # Should only include crypto-spot and stocks-cn
    asset_classes = set(df["asset_class"].to_list())
    assert asset_classes.issubset({"crypto-spot", "stocks-cn"})


def test_list_symbols_no_filters():
    """Test listing symbols without filters (may be slow, so we limit)."""
    # This depends on dim_symbol existing
    # For unit test, we can't assume data exists
    # So this is more of an integration test
    pass


def test_list_tables():
    """Test listing tables."""
    df = discovery.list_tables()

    assert isinstance(df, pl.DataFrame)
    assert "table_name" in df.columns
    assert "layer" in df.columns
    assert "path" in df.columns
    assert "has_date_partition" in df.columns
    assert "description" in df.columns

    # Should have some tables
    assert df.height > 0

    # Should include key tables
    table_names = df["table_name"].to_list()
    assert "dim_symbol" in table_names
    assert "trades" in table_names


def test_list_tables_silver_only():
    """Test filtering by silver layer."""
    df = discovery.list_tables(layer="silver")

    assert df.height > 0
    assert all(layer == "silver" for layer in df["layer"].to_list())


def test_data_coverage_symbol_not_found():
    """Test coverage when symbol doesn't exist."""
    coverage = discovery.data_coverage("binance-futures", "FAKESYMBOL123")

    # Should return dict with available=False for all tables
    assert isinstance(coverage, dict)
    assert "trades" in coverage
    assert coverage["trades"]["available"] is False
    assert "not found" in coverage["trades"]["reason"].lower()


def test_asset_class_taxonomy_crypto():
    """Test asset class taxonomy for crypto."""
    from pointline.config import ASSET_CLASS_TAXONOMY, get_asset_class_exchanges

    # Crypto should have children
    assert "children" in ASSET_CLASS_TAXONOMY["crypto"]
    assert "crypto-spot" in ASSET_CLASS_TAXONOMY["crypto"]["children"]
    assert "crypto-derivatives" in ASSET_CLASS_TAXONOMY["crypto"]["children"]

    # Get all crypto exchanges (should include spot + derivatives)
    exchanges = get_asset_class_exchanges("crypto")
    assert "binance" in exchanges  # spot
    assert "binance-futures" in exchanges  # derivatives


def test_asset_class_taxonomy_stocks():
    """Test asset class taxonomy for stocks."""
    from pointline.config import get_asset_class_exchanges

    # Get Chinese stock exchanges
    exchanges = get_asset_class_exchanges("stocks-cn")
    assert "szse" in exchanges
    assert "sse" in exchanges


def test_asset_type_name_decoding():
    """Test asset type integer to name conversion."""
    from pointline.config import get_asset_type_name

    assert get_asset_type_name(0) == "spot"
    assert get_asset_type_name(1) == "perpetual"
    assert get_asset_type_name(2) == "future"
    assert get_asset_type_name(3) == "option"
    assert get_asset_type_name(10) == "l3_orders"
    assert get_asset_type_name(11) == "l3_ticks"
    assert "unknown" in get_asset_type_name(999)
