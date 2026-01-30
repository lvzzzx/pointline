import logging
import time

import polars as pl
from deltalake.exceptions import CommitFailedError

from pointline.dim_symbol import required_update_columns, scd2_upsert
from pointline.io.protocols import TableRepository
from pointline.services.base_service import BaseService

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
                    logger.error(
                        f"Failed to update DimSymbol after {self.max_retries} retries due to conflict: {e}"
                    )
                    raise

                logger.warning(
                    f"Conflict detected (attempt {attempt}/{self.max_retries}). Retrying..."
                )
                time.sleep(0.1 * attempt)  # Simple backoff

    def rebuild(self, history_data: pl.DataFrame) -> None:
        """
        Replaces the entire history for the provided symbols with the new history_data.

        This is useful for 'sync' operations where the source provides a full
        audit trail (e.g. Tardis 'changes' array) and we want to correct/backfill
        the timeline rather than just appending a new current version.
        """
        from pointline.dim_symbol import NATURAL_KEY_COLS, rebuild_from_history

        # 1. Transform raw history rows into proper SCD2 chains
        new_symbol_history = rebuild_from_history(history_data)

        attempt = 0
        while attempt <= self.max_retries:
            try:
                # 2. Read existing state
                try:
                    full_table = self.repo.read_all()
                except Exception:
                    full_table = pl.DataFrame()

                # 3. Filter out the old history for the symbols we are rebuilding
                # We identify symbols by their natural key (exchange_id, exchange_symbol)
                if not full_table.is_empty():
                    # Get the keys being replaced
                    changed_keys = new_symbol_history.select(list(NATURAL_KEY_COLS)).unique()

                    # Anti-join to keep only symbols NOT being updated
                    preserved_data = full_table.join(
                        changed_keys, on=list(NATURAL_KEY_COLS), how="anti"
                    )
                else:
                    preserved_data = pl.DataFrame()

                # 4. Combine preserved data with new history
                if preserved_data.is_empty():
                    final_table = new_symbol_history
                else:
                    # Normalize schemas to ensure compatibility before concatenating
                    from pointline.dim_symbol import normalize_dim_symbol_schema

                    preserved_data = normalize_dim_symbol_schema(preserved_data)
                    new_symbol_history = normalize_dim_symbol_schema(new_symbol_history)
                    final_table = pl.concat([preserved_data, new_symbol_history], how="vertical")

                # 5. Write atomically
                self.write(final_table)

                logger.info(
                    f"DimSymbol rebuild complete. Replaced history for {new_symbol_history.select(list(NATURAL_KEY_COLS)).n_unique()} symbols. "
                    f"Total rows: {final_table.height}."
                )
                return

            except CommitFailedError as e:
                attempt += 1
                if attempt > self.max_retries:
                    logger.error(
                        f"Failed to rebuild DimSymbol after {self.max_retries} retries: {e}"
                    )
                    raise
                time.sleep(0.1 * attempt)
