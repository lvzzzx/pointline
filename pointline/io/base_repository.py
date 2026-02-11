from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from pointline.config import STORAGE_OPTIONS

logger = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    """Raised when a DataFrame schema does not match the expected table schema."""


def validate_schema(
    df: pl.DataFrame,
    expected_schema: dict[str, pl.DataType],
    *,
    table_path: str = "",
) -> None:
    """Validate that a DataFrame matches the expected schema before writing.

    Checks:
    - All expected columns are present
    - No unexpected columns exist
    - Column types match expected types

    Args:
        df: DataFrame to validate.
        expected_schema: Mapping of column name to expected Polars DataType.
        table_path: Table path for error messages.

    Raises:
        SchemaValidationError: If validation fails.
    """
    df_schema = dict(df.schema)
    expected_cols = set(expected_schema.keys())
    actual_cols = set(df_schema.keys())

    errors: list[str] = []

    missing = expected_cols - actual_cols
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")

    unexpected = actual_cols - expected_cols
    if unexpected:
        errors.append(f"Unexpected columns: {sorted(unexpected)}")

    for col in expected_cols & actual_cols:
        actual_type = df_schema[col]
        expected_type = expected_schema[col]
        if actual_type != expected_type:
            errors.append(f"Column '{col}': expected {expected_type}, got {actual_type}")

    if errors:
        detail = "; ".join(errors)
        raise SchemaValidationError(f"Schema validation failed for '{table_path}': {detail}")


def get_writer_properties():
    """
    Create WriterProperties from STORAGE_OPTIONS configuration.

    Returns:
        WriterProperties with compression settings, or None if no compression configured.
    """
    from deltalake import WriterProperties

    if "compression" in STORAGE_OPTIONS:
        return WriterProperties(compression=STORAGE_OPTIONS["compression"].upper())
    return None


class BaseDeltaRepository:
    """
    Base implementation for Delta Lake repositories using Polars and delta-rs.
    """

    def __init__(
        self,
        table_path: str | Path,
        partition_by: list[str] | None = None,
        expected_schema: dict[str, pl.DataType] | None = None,
        *,
        table_name: str | None = None,
    ):
        """
        Initializes the repository with a specific table path.

        Args:
            table_path: The physical path to the Delta table.
            partition_by: Optional list of column names to partition by (e.g., ["exchange", "date"]).
                         If None, table will not be partitioned.
            expected_schema: Optional expected schema for write-time validation.
                            If provided, every write operation validates the DataFrame against it.
            table_name: Optional table name to auto-lookup schema from the schema registry.
                       If both table_name and expected_schema are provided, expected_schema wins.
        """
        self.table_path = str(table_path)
        self.partition_by = partition_by
        if expected_schema is not None:
            self.expected_schema = expected_schema
        elif table_name is not None:
            from pointline.schema_registry import get_schema

            self.expected_schema = get_schema(table_name)
        else:
            self.expected_schema = None

    def _validate_before_write(self, df: pl.DataFrame) -> None:
        """Validate DataFrame schema before writing, if expected_schema is set."""
        if self.expected_schema is not None:
            validate_schema(df, self.expected_schema, table_path=self.table_path)

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

        Raises:
            SchemaValidationError: If DataFrame schema doesn't match expected_schema.
        """
        from deltalake import write_deltalake

        self._validate_before_write(df)

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
            writer_properties=get_writer_properties(),
        )

    def append(self, df: pl.DataFrame) -> None:
        """
        Appends the DataFrame to the Delta table.

        Args:
            df: The DataFrame to append.

        Raises:
            SchemaValidationError: If DataFrame schema doesn't match expected_schema.
        """
        from deltalake import write_deltalake

        self._validate_before_write(df)

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
            writer_properties=get_writer_properties(),
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
        from deltalake import write_deltalake

        if isinstance(data, pl.DataFrame):
            self._validate_before_write(data)
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
            writer_properties=get_writer_properties(),
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

    def merge(self, df: pl.DataFrame, keys: list[str], use_native_merge: bool = True) -> None:
        """
        Merges updates into the table based on primary keys.

        Args:
            df: The DataFrame containing updates.
            keys: The primary keys used for merging.
            use_native_merge: If True, use Delta Lake native MERGE operation (atomic, recommended).
                            If False, use anti-join + append pattern (simpler but not atomic).

        Raises:
            SchemaValidationError: If DataFrame schema doesn't match expected_schema.
        """
        self._validate_before_write(df)
        if use_native_merge:
            self._merge_native(df, keys)
        else:
            self._merge_antijoin(df, keys)

    def _merge_native(self, df: pl.DataFrame, keys: list[str]) -> None:
        """
        Merge using Delta Lake native MERGE operation (atomic, ACID-compliant).

        This uses Delta Lake's MERGE command which provides:
        - Atomic updates (no partial state visible)
        - Optimistic concurrency control
        - Efficient (no full table rewrite)
        """
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError

        try:
            dt = DeltaTable(self.table_path)

            # Build merge predicate: target.key1 = source.key1 AND target.key2 = source.key2
            predicate = " AND ".join([f"target.{k} = source.{k}" for k in keys])

            # Execute merge: update if match, insert if no match
            (
                dt.merge(
                    source=df.to_arrow(),
                    predicate=predicate,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

        except TableNotFoundError:
            # If table doesn't exist, perform a full write
            self.write_full(df)

    def _merge_antijoin(self, df: pl.DataFrame, keys: list[str]) -> None:
        """
        Merge using anti-join + append pattern (simple but not atomic).

        This implementation:
        - Reads entire table
        - Filters out rows matching keys in new data
        - Concatenates with new data
        - Overwrites entire table

        Use only for small tables or when native merge is not available.
        """
        from deltalake.exceptions import TableNotFoundError

        try:
            current = self.read_all()

            # Perform anti-join to remove existing records that are being updated
            # Then concatenate with the new data
            updated = pl.concat([current.join(df.select(keys), on=keys, how="anti"), df])
            self.write_full(updated)
        except (TableNotFoundError, FileNotFoundError):
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
