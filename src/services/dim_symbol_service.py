import polars as pl
from src.services.base_service import BaseService
from src.io.protocols import TableRepository
from src.dim_symbol import scd2_upsert, required_update_columns

class DimSymbolService(BaseService):
    """
    Orchestrates SCD Type 2 updates for the dim_symbol table.
    """
    
    def __init__(self, repo: TableRepository):
        """
        Initialize with a repository implementing the TableRepository protocol.
        """
        self.repo = repo
        
    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Enforce schema, deduplicate updates, and basic quality rules.
        """
        # Enforce required columns
        req = required_update_columns()
        missing = [c for c in req if c not in data.columns]
        if missing:
            raise ValueError(f"Updates missing required columns: {missing}")
            
        # Deduplicate incoming updates by natural key and timestamp
        # This ensures idempotency for re-processed files
        return data.unique(subset=["exchange_id", "exchange_symbol", "valid_from_ts"])
        
    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Applies SCD2 domain logic by reading current state and computing the upsert.
        """
        try:
            current = self.repo.read_all()
        except Exception:
            # Table doesn't exist yet, scd2_upsert handles empty current
            current = pl.DataFrame()
            
        return scd2_upsert(current, valid_data)
        
    def write(self, result: pl.DataFrame) -> None:
        """
        Persist the new state to the repository.
        """
        self.repo.write_full(result)
