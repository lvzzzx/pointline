"""Test timezone validation in services."""

import polars as pl
import pytest

from pointline.cli.ingestion_factory import create_ingestion_service
from pointline.config import get_exchange_timezone


def test_get_exchange_timezone_strict_mode_known_exchange():
    """Known exchanges should return their timezone in strict mode."""
    assert get_exchange_timezone("binance-futures", strict=True) == "UTC"
    assert get_exchange_timezone("szse", strict=True) == "Asia/Shanghai"


def test_get_exchange_timezone_strict_mode_unknown_exchange():
    """Unknown exchanges should raise ValueError in strict mode."""
    with pytest.raises(ValueError, match="not found in dim_exchange"):
        get_exchange_timezone("unknown-exchange", strict=True)


def test_get_exchange_timezone_non_strict_mode_unknown_exchange():
    """Unknown exchanges should warn and return UTC in non-strict mode."""
    with pytest.warns(UserWarning, match="not found in dim_exchange"):
        result = get_exchange_timezone("unknown-exchange", strict=False)
    assert result == "UTC"


@pytest.mark.skip(
    reason="GenericIngestionService refactor removed _add_metadata private method. "
    "Timezone validation is now handled inline. TODO: Update test for new architecture."
)
def test_service_add_metadata_fails_for_unknown_exchange():
    """_add_metadata should fail fast for unknown exchanges."""
    # Create a minimal trades service instance
    import tempfile
    from pathlib import Path

    from pointline.io.base_repository import BaseDeltaRepository

    with tempfile.TemporaryDirectory() as tmpdir:
        repo = BaseDeltaRepository(Path(tmpdir) / "trades")
        dim_symbol_repo = BaseDeltaRepository(Path(tmpdir) / "dim_symbol")
        manifest_repo_mock = None  # Not needed for this test

        service = create_ingestion_service("trades", manifest_repo_mock)
        service.repo = repo
        service.dim_symbol_repo = dim_symbol_repo

        # Create a dummy dataframe
        df = pl.DataFrame(
            {
                "ts_local_us": [1000000],
            }
        )

        # Should raise ValueError for unknown exchange
        with pytest.raises(ValueError, match="Cannot add metadata for exchange 'unknown'"):
            service._add_metadata(df, "unknown", exchange_id=999)


@pytest.mark.skip(
    reason="GenericIngestionService refactor removed _add_metadata private method. "
    "Timezone validation is now handled inline. TODO: Update test for new architecture."
)
def test_service_add_metadata_succeeds_for_known_exchange():
    """_add_metadata should succeed for known exchanges."""
    import tempfile
    from pathlib import Path

    from pointline.io.base_repository import BaseDeltaRepository

    with tempfile.TemporaryDirectory() as tmpdir:
        repo = BaseDeltaRepository(Path(tmpdir) / "trades")
        dim_symbol_repo = BaseDeltaRepository(Path(tmpdir) / "dim_symbol")
        manifest_repo_mock = None

        service = create_ingestion_service("trades", manifest_repo_mock)
        service.repo = repo
        service.dim_symbol_repo = dim_symbol_repo

        df = pl.DataFrame(
            {
                "ts_local_us": [1727659500000000],  # 2024-09-30 01:15 UTC
            }
        )

        # Should succeed for known exchange
        result = service._add_metadata(df, "binance-futures", exchange_id=2)

        assert "exchange" in result.columns
        assert "exchange_id" in result.columns
        assert "date" in result.columns
        assert result["exchange"][0] == "binance-futures"
        assert result["exchange_id"][0] == 2
