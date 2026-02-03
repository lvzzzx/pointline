"""Test that all vendor parsers are properly registered."""

from pointline.io.parsers import get_parser, list_supported_combinations


def test_all_parsers_registered():
    """Test that all expected parsers are registered."""
    combos = list_supported_combinations()

    # Expected combinations based on plan
    expected = [
        ("tardis", "trades"),
        ("tardis", "quotes"),
        ("tardis", "book_snapshot_25"),
        ("tardis", "derivative_ticker"),
        ("quant360", "l3_orders"),
        ("quant360", "l3_ticks"),
        ("binance", "klines"),
    ]

    # Check all expected parsers are registered
    for vendor, data_type in expected:
        assert (vendor, data_type) in combos, f"Parser not registered: {vendor}/{data_type}"

    # Check exact count
    assert len(combos) == len(expected), f"Expected {len(expected)} parsers, got {len(combos)}"


def test_tardis_parsers():
    """Test Tardis parsers can be retrieved."""
    assert get_parser("tardis", "trades") is not None
    assert get_parser("tardis", "quotes") is not None
    assert get_parser("tardis", "book_snapshot_25") is not None
    assert get_parser("tardis", "derivative_ticker") is not None


def test_quant360_parsers():
    """Test Quant360 parsers can be retrieved."""
    assert get_parser("quant360", "l3_orders") is not None
    assert get_parser("quant360", "l3_ticks") is not None


def test_binance_parsers():
    """Test Binance parsers can be retrieved."""
    assert get_parser("binance", "klines") is not None
