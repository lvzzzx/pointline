from typing import Protocol, runtime_checkable
import polars as pl

@runtime_checkable
class TableRepository(Protocol):
    """
    Standard interface for all table repositories.
    Ensures storage-agnostic behavior in the service layer.
    """
    
    def read_all(self) -> pl.DataFrame:
        """Reads all data from the table as a Polars DataFrame."""
        ...
        
    def write_full(self, df: pl.DataFrame) -> None:
        """Writes/overwrites the full table with the provided DataFrame."""
        ...
        
    def merge(self, df: pl.DataFrame, keys: list[str]) -> None:
        """Merges incremental updates into the table based on primary keys."""
        ...
