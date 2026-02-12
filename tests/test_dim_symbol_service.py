from unittest.mock import MagicMock

import polars as pl
import pytest

from pointline.io.protocols import TableRepository


def test_dim_symbol_service_orchestration():
    # This will fail initially because DimSymbolService doesn't exist
    from pointline.dim_symbol import scd2_bootstrap
    from pointline.services.dim_symbol_service import DimSymbolService

    mock_repo = MagicMock(spec=TableRepository)

    # Create valid initial state using bootstrap
    initial_data = pl.DataFrame(
        {
            "exchange_id": [1],
            "exchange_symbol": ["BTC-PERPETUAL"],
            "tick_size": [0.5],
            "valid_from_ts": [100],
            "lot_size": [1.0],
            "contract_size": [1.0],
            "base_asset": ["BTC"],
            "quote_asset": ["USD"],
            "asset_type": [1],
        }
    )
    current_df = scd2_bootstrap(initial_data)
    mock_repo.read_all.return_value = current_df

    service = DimSymbolService(mock_repo)

    # New updates (deduplication check: repeat one row)
    updates = pl.DataFrame(
        {
            "exchange_id": [1, 1],
            "exchange_symbol": ["BTC-PERPETUAL", "BTC-PERPETUAL"],
            "tick_size": [1.0, 1.0],
            "valid_from_ts": [200, 200],  # Duplicate
            "lot_size": [1.0, 1.0],
            "contract_size": [1.0, 1.0],
            "base_asset": ["BTC", "BTC"],
            "quote_asset": ["USD", "USD"],
            "asset_type": [1, 1],
        }
    )

    service.update(updates)

    # Verify repo calls
    assert mock_repo.read_all.called
    assert mock_repo.write_full.called

    # Verify the data written to repo (should have history + current)
    written_df = mock_repo.write_full.call_args[0][0]
    assert written_df.height == 2
    assert written_df.filter(pl.col("is_current"))["tick_size"][0] == 1.0


def test_dim_symbol_service_validation_failure():
    from pointline.services.dim_symbol_service import DimSymbolService

    mock_repo = MagicMock(spec=TableRepository)
    service = DimSymbolService(mock_repo)

    # Missing required columns
    bad_updates = pl.DataFrame({"exchange_id": [1]})

    with pytest.raises(ValueError, match="missing required columns"):
        service.update(bad_updates)
