"""Contracts for v2-owned storage adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

import polars as pl

from pointline.io.protocols import BronzeFileMetadata

if TYPE_CHECKING:
    from pointline.v2.ingestion.models import IngestionResult


@runtime_checkable
class ManifestStore(Protocol):
    """Persistent manifest state for v2 ingestion idempotency."""

    def resolve_file_id(self, meta: BronzeFileMetadata) -> int:
        """Return stable file_id for a manifest identity, creating pending state if needed."""

    def filter_pending(self, candidates: list[BronzeFileMetadata]) -> list[BronzeFileMetadata]:
        """Return candidates that are not already successful in manifest state."""

    def update_status(
        self,
        file_id: int,
        status: str,
        meta: BronzeFileMetadata,
        result: IngestionResult | None = None,
    ) -> None:
        """Persist final ingestion status for a file_id."""


@runtime_checkable
class EventStore(Protocol):
    """Persistent writer for normalized event tables."""

    def append(self, table_name: str, df: pl.DataFrame) -> None:
        """Append normalized rows to a v2 event table."""


@runtime_checkable
class DimensionStore(Protocol):
    """Read interface for dimensions required by ingestion checks."""

    def load_dim_symbol(self) -> pl.DataFrame:
        """Load `dim_symbol` for PIT coverage checks."""

    def save_dim_symbol(
        self,
        df: pl.DataFrame,
        *,
        expected_version: int | None = None,
    ) -> int:
        """Atomically replace `dim_symbol` and return the new table version."""

    def current_version(self) -> int | None:
        """Return current `dim_symbol` Delta version, or None if table is missing."""


@runtime_checkable
class QuarantineStore(Protocol):
    """Persistent writer for quarantined rows and reasons."""

    def append(
        self,
        table_name: str,
        df: pl.DataFrame,
        *,
        reason: str,
        file_id: int,
    ) -> None:
        """Append quarantined rows for one table/reason."""
