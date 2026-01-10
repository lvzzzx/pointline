import polars as pl
from datetime import date
from pathlib import Path
from typing import Optional
from pointline.config import STORAGE_OPTIONS

class BaseDeltaRepository:
    """
    Base implementation for Delta Lake repositories using Polars and delta-rs.
    """
    
    def __init__(self, table_path: str | Path, partition_by: list[str] | None = None):
        """
        Initializes the repository with a specific table path.
        
        Args:
            table_path: The physical path to the Delta table.
            partition_by: Optional list of column names to partition by (e.g., ["exchange", "date"]).
                         If None, table will not be partitioned.
        """
        self.table_path = str(table_path)
        self.partition_by = partition_by
        
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
        from deltalake import WriterProperties, write_deltalake
        
        # Map STORAGE_OPTIONS to writer_properties
        writer_properties = None
        if "compression" in STORAGE_OPTIONS:
            # WriterProperties expects uppercase compression name or specific enum
            writer_properties = WriterProperties(
                compression=STORAGE_OPTIONS["compression"].upper()
            )
        
        # Convert Polars DataFrame to PyArrow Table for delta-rs
        arrow_table = df.to_arrow()
        
        # Use write_deltalake which supports partition_by
        # IMPORTANT: When partition_by is specified, Delta Lake automatically:
        # 1. Uses partition columns to create directory structure (e.g., exchange=binance/date=2024-05-10/)
        # 2. Does NOT store partition columns in Parquet files (saves storage space)
        # 3. Reconstructs partition columns when reading via read_delta()
        # This is the correct Hive-style partitioning behavior - partition columns are metadata, not data
        write_deltalake(
            self.table_path,
            arrow_table,
            mode="overwrite",
            partition_by=self.partition_by,
            writer_properties=writer_properties
        )

    def append(self, df: pl.DataFrame) -> None:
        """
        Appends the DataFrame to the Delta table.
        
        Args:
            df: The DataFrame to append.
        """
        from deltalake import WriterProperties, write_deltalake
        
        # Map STORAGE_OPTIONS to writer_properties
        writer_properties = None
        if "compression" in STORAGE_OPTIONS:
            writer_properties = WriterProperties(
                compression=STORAGE_OPTIONS["compression"].upper()
            )
        
        # Convert Polars DataFrame to PyArrow Table for delta-rs
        arrow_table = df.to_arrow()
        
        # Use write_deltalake which supports partition_by
        # IMPORTANT: When partition_by is specified, Delta Lake automatically:
        # 1. Uses partition columns to create directory structure (e.g., exchange=binance/date=2024-05-10/)
        # 2. Does NOT store partition columns in Parquet files (saves storage space)
        # 3. Reconstructs partition columns when reading via read_delta()
        # This is the correct Hive-style partitioning behavior - partition columns are metadata, not data
        write_deltalake(
            self.table_path,
            arrow_table,
            mode="append",
            partition_by=self.partition_by,
            writer_properties=writer_properties
        )

    def overwrite_partition(
        self,
        data,
        *,
        predicate: str,
        target_file_size: int | None = None,
    ) -> None:
        """
        Overwrite a single partition using a predicate (Delta Lake partition overwrite).

        Args:
            data: Polars DataFrame or Arrow stream/table for the partition.
            predicate: SQL predicate that selects the partition to replace.
            target_file_size: Desired target file size (bytes) to avoid splitting.
        """
        from deltalake import WriterProperties, write_deltalake

        writer_properties = None
        if "compression" in STORAGE_OPTIONS:
            writer_properties = WriterProperties(
                compression=STORAGE_OPTIONS["compression"].upper()
            )

        if isinstance(data, pl.DataFrame):
            arrow_data = data.to_arrow()
        else:
            arrow_data = data

        write_deltalake(
            self.table_path,
            arrow_data,
            mode="overwrite",
            partition_by=self.partition_by,
            predicate=predicate,
            target_file_size=target_file_size,
            writer_properties=writer_properties,
        )

    def optimize_partition(
        self,
        *,
        filters: dict[str, object],
        target_file_size: int | None = None,
        z_order: list[str] | None = None,
    ) -> dict[str, object]:
        """Compact or Z-order a single partition using delta-rs optimize."""
        if not filters:
            raise ValueError("optimize_partition: filters must be non-empty")

        predicate_parts = []
        for key, value in filters.items():
            if isinstance(value, date):
                predicate_parts.append(f"{key} = '{value.isoformat()}'")
            elif isinstance(value, str):
                predicate_parts.append(f"{key} = '{value}'")
            else:
                predicate_parts.append(f"{key} = {value}")
        partition_filters = []
        for key, value in filters.items():
            if isinstance(value, date):
                partition_filters.append((key, "=", value.isoformat()))
            else:
                partition_filters.append((key, "=", str(value)))

        from deltalake import DeltaTable

        dt = DeltaTable(self.table_path)
        if z_order is None:
            try:
                schema_fields = dt.schema().to_pyarrow().names
                if "symbol_id" in schema_fields and "ts_local_us" in schema_fields:
                    z_order = ["symbol_id", "ts_local_us"]
            except Exception:
                z_order = None

        if z_order:
            return dt.optimize.z_order(
                z_order,
                partition_filters=partition_filters,
                target_size=target_file_size,
            )

        return dt.optimize.compact(
            partition_filters=partition_filters,
            target_size=target_file_size,
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

    def vacuum(
        self,
        *,
        retention_hours: int,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
        full: bool = False,
        keep_versions: list[int] | None = None,
    ) -> list[str]:
        """Remove files no longer referenced by the Delta table."""
        from deltalake import DeltaTable

        dt = DeltaTable(self.table_path)
        return dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
            full=full,
            keep_versions=keep_versions,
        )
