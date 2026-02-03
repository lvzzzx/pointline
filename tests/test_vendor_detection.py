"""Tests for vendor plugin-based detection."""

from pathlib import Path
from tempfile import TemporaryDirectory

from pointline.io.vendors.registry import detect_vendor


def test_detect_tardis_by_directory_name():
    """Test that Tardis is detected by directory name."""
    with TemporaryDirectory() as tmpdir:
        tardis_dir = Path(tmpdir) / "tardis"
        tardis_dir.mkdir()

        assert detect_vendor(tardis_dir) == "tardis"


def test_detect_binance_by_directory_name():
    """Test that Binance is detected by directory name."""
    with TemporaryDirectory() as tmpdir:
        binance_dir = Path(tmpdir) / "binance_vision"
        binance_dir.mkdir()

        assert detect_vendor(binance_dir) == "binance_vision"


def test_detect_quant360_by_directory_name():
    """Test that Quant360 is detected by directory name."""
    with TemporaryDirectory() as tmpdir:
        quant360_dir = Path(tmpdir) / "quant360"
        quant360_dir.mkdir()

        assert detect_vendor(quant360_dir) == "quant360"


def test_detect_quant360_by_archive_pattern():
    """Test that Quant360 is detected by .7z archive pattern."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a mock Quant360 archive file
        archive = tmpdir_path / "order_new_STK_SZ_20240930.7z"
        archive.touch()

        assert detect_vendor(tmpdir_path) == "quant360"


def test_detect_quant360_by_reorganized_structure():
    """Test that Quant360 is detected by reorganized l3_orders structure."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a mock reorganized Quant360 structure
        l3_orders_dir = (
            tmpdir_path / "exchange=szse" / "type=l3_orders" / "date=2024-09-30" / "symbol=000001"
        )
        l3_orders_dir.mkdir(parents=True)
        (l3_orders_dir / "000001.csv.gz").touch()

        assert detect_vendor(tmpdir_path) == "quant360"


def test_detect_coingecko_by_directory_name():
    """Test that CoinGecko is detected by directory name."""
    with TemporaryDirectory() as tmpdir:
        coingecko_dir = Path(tmpdir) / "coingecko"
        coingecko_dir.mkdir()

        assert detect_vendor(coingecko_dir) == "coingecko"


def test_detect_tushare_by_directory_name():
    """Test that Tushare is detected by directory name."""
    with TemporaryDirectory() as tmpdir:
        tushare_dir = Path(tmpdir) / "tushare"
        tushare_dir.mkdir()

        assert detect_vendor(tushare_dir) == "tushare"


def test_no_vendor_detected_for_unknown_directory():
    """Test that None is returned when no vendor matches."""
    with TemporaryDirectory() as tmpdir:
        unknown_dir = Path(tmpdir) / "unknown_vendor"
        unknown_dir.mkdir()

        assert detect_vendor(unknown_dir) is None


def test_priority_order_quant360_over_others():
    """Test that Quant360 is detected first when multiple vendors could match.

    Quant360 has highest priority due to specific archive patterns.
    """
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create Quant360 archive (should be detected first due to priority)
        archive = tmpdir_path / "order_new_STK_SZ_20240930.7z"
        archive.touch()

        # Even if directory could match another vendor, Quant360 wins
        assert detect_vendor(tmpdir_path) == "quant360"


def test_tardis_subdirectory_detection():
    """Test that Tardis is detected when 'tardis' appears in path components."""
    with TemporaryDirectory() as tmpdir:
        bronze_dir = Path(tmpdir) / "bronze"
        tardis_dir = bronze_dir / "tardis"
        tardis_dir.mkdir(parents=True)

        assert detect_vendor(tardis_dir) == "tardis"
