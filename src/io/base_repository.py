import polars as pl
from pathlib import Path
from typing import Optional
from src.config import STORAGE_OPTIONS

class BaseDeltaRepository:
    """
    Base implementation for Delta Lake repositories using Polars and delta-rs.
    """
    
    def __init__(self, table_path: str | Path):
        """
        Initializes the repository with a specific table path.
        
        Args:
            table_path: The physical path to the Delta table.
        """
        self.table_path = str(table_path)
        
    def read_all(self) -> pl.DataFrame:
        """
        Reads the entire Delta table into a Polars DataFrame.
        
        Returns:
            pl.DataFrame: The table content.
        """
        return pl.read_delta(self.table_path)
        
    def write_full(self, df: pl.DataFrame) -> None:
        """
        Writes the DataFrame to the Delta table, overwriting any existing data.
        
        Args:
            df: The DataFrame to write.
        """
        from deltalake import WriterProperties
        
        # Map STORAGE_OPTIONS to delta_write_options
        write_options = {}
        if "compression" in STORAGE_OPTIONS:
            # WriterProperties expects uppercase compression name or specific enum
            write_options["writer_properties"] = WriterProperties(
                compression=STORAGE_OPTIONS["compression"].upper()
            )
        
        df.write_delta(
            self.table_path,
            mode="overwrite",
            delta_write_options=write_options
        )
        
    def merge(self, df: pl.DataFrame, keys: list[str]) -> None:
        """
        Merges updates into the table using a deterministic rebuild strategy (Anti-join + Append).
        
        Args:
            df: The DataFrame containing updates.
            keys: The primary keys used for merging.
        """
        try:
            current = self.read_all()
            
            # Perform anti-join to remove existing records that are being updated
            # Then concatenate with the new data
            updated = pl.concat([
                current.join(df.select(keys), on=keys, how="anti"),
                df
            ])
            self.write_full(updated)
        except Exception:
            # If table doesn't exist, perform a full write
            self.write_full(df)
