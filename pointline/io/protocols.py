from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
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


@runtime_checkable
class AppendableTableRepository(TableRepository, Protocol):
    """Optional interface for repositories that support append operations."""

    def append(self, df: pl.DataFrame) -> None:
        """Appends data to the table. Most efficient for immutable event data."""
        ...


@dataclass
class BronzeFileMetadata:
    """Metadata extracted from the source file system before reading contents."""

    vendor: str
    exchange: str
    data_type: str
    symbol: str
    date: date
    bronze_file_path: str  # The full relative path or key
    file_size_bytes: int
    last_modified_ts: int
    sha256: str


@runtime_checkable
class BronzeSource(Protocol):
    """Abstraction for a file system scanner."""

    def list_files(self, glob_pattern: str) -> Iterator[BronzeFileMetadata]:
        """Scans storage for files matching the pattern."""
        ...


@dataclass
class IngestionResult:
    """Outcomes from the ingestion process."""

    row_count: int
    ts_local_min_us: int
    ts_local_max_us: int
    error_message: str | None = None


@runtime_checkable
class IngestionManifestRepository(Protocol):
    """Abstraction for the state ledger (silver.ingest_manifest)."""

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """Gets existing ID or mints a new one for a file."""
        ...

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        """Returns only files that need processing (efficient batch anti-join)."""
        ...

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        """Records success/failure."""
        ...
