import pytest
import polars as pl
from unittest.mock import MagicMock, patch
from pointline.io.protocols import TableRepository
from pointline.dim_symbol import scd2_bootstrap

def test_dim_symbol_service_retry_on_conflict():
    from pointline.services.dim_symbol_service import DimSymbolService
    
    # Mock repo
    mock_repo = MagicMock(spec=TableRepository)
    initial_data = pl.DataFrame({
        "exchange_id": [1], "exchange_symbol": ["BTC-PERPETUAL"], "tick_size": [0.5],
        "valid_from_ts": [100], "lot_size": [1.0], "price_increment": [0.5],
        "amount_increment": [0.1], "contract_size": [1.0], "base_asset": ["BTC"],
        "quote_asset": ["USD"], "asset_type": [1]
    })
    mock_repo.read_all.return_value = scd2_bootstrap(initial_data)
    
    # Simulate a conflict on the first write attempt
    from deltalake.exceptions import CommitFailedError
    mock_repo.write_full.side_effect = [
        CommitFailedError("Conflict!"),
        None  # Success on second try
    ]
    
    service = DimSymbolService(mock_repo)
    updates = initial_data.with_columns(pl.lit(200).alias("valid_from_ts"), pl.lit(1.0).alias("tick_size"))
    
    # Run update
    service.update(updates)
    
    # Verify it re-read the state and re-tried the write
    assert mock_repo.read_all.call_count == 2
    assert mock_repo.write_full.call_count == 2

def test_dim_symbol_service_audit_logging():
    from pointline.services.dim_symbol_service import DimSymbolService
    mock_repo = MagicMock(spec=TableRepository)
    mock_repo.read_all.return_value = pl.DataFrame()
    
    with patch("pointline.services.dim_symbol_service.logger") as mock_logger:
        service = DimSymbolService(mock_repo)
        updates = pl.DataFrame({
            "exchange_id": [1], "exchange_symbol": ["A"], "valid_from_ts": [100],
            "tick_size": [0.5], "lot_size": [1.0], "price_increment": [0.5],
            "amount_increment": [0.1], "contract_size": [1.0], "base_asset": ["B"],
            "quote_asset": ["Q"], "asset_type": [1]
        })
        
        service.update(updates)
        
        # Verify logger was called with some audit info (row counts, etc.)
        assert mock_logger.info.called
        log_msg = mock_logger.info.call_args[0][0]
        assert "rows" in log_msg.lower() or "symbols" in log_msg.lower()
