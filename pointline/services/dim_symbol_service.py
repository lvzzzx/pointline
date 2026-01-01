import logging
import time
import polars as pl
from deltalake.exceptions import CommitFailedError
from pointline.services.base_service import BaseService
from pointline.io.protocols import TableRepository
from pointline.dim_symbol import scd2_upsert, required_update_columns

logger = logging.getLogger(__name__)

class DimSymbolService(BaseService):
    """
    Orchestrates SCD Type 2 updates for the dim_symbol table with resilience and auditing.
    """
    
    def __init__(self, repo: TableRepository, max_retries: int = 3):
        """
        Initialize with a repository and retry configuration.
        """
        self.repo = repo
        self.max_retries = max_retries
        
    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Enforce schema, deduplicate updates, and basic quality rules.
        """
        req = required_update_columns()
        missing = [c for c in req if c not in data.columns]
        if missing:
            raise ValueError(f"Updates missing required columns: {missing}")
            
        # Deduplicate incoming updates by natural key and timestamp
        return data.unique(subset=["exchange_id", "exchange_symbol", "valid_from_ts"])
        
    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Applies SCD2 domain logic by reading current state and computing the upsert.
        """
        try:
            current = self.repo.read_all()
        except Exception:
            current = pl.DataFrame()
            
        return scd2_upsert(current, valid_data)
        
    def write(self, result: pl.DataFrame) -> None:
        """
        Persist the new state to the repository.
        """
        self.repo.write_full(result)

    def update(self, data: pl.DataFrame) -> None:
        """
        Orchestrates the update lifecycle with optimistic concurrency retries.
        """
        valid_data = self.validate(data)
        
        attempt = 0
        while attempt <= self.max_retries:
            try:
                result = self.compute_state(valid_data)
                self.write(result)
                
                # Audit logging
                logger.info(
                    f"DimSymbol update complete. Processed {valid_data.height} rows. "
                    f"Resulting table height: {result.height}."
                )
                return
            except CommitFailedError as e:
                attempt += 1
                if attempt > self.max_retries:
                    logger.error(f"Failed to update DimSymbol after {self.max_retries} retries due to conflict: {e}")
                    raise
                
                logger.warning(f"Conflict detected (attempt {attempt}/{self.max_retries}). Retrying...")
                time.sleep(0.1 * attempt)  # Simple backoff