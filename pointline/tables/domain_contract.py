"""Canonical table-domain contracts.

Event table domains own stream semantics (vendor canonicalization, encode/decode).
Dimension table domains own SCD2 lifecycle semantics (bootstrap/upsert).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, TypeAlias

import polars as pl

DomainKind: TypeAlias = Literal["event", "dimension"]


@dataclass(frozen=True)
class TableSpec:
    """Canonical table metadata used by orchestrators and discovery layers."""

    table_name: str
    table_kind: DomainKind
    schema: dict[str, pl.DataType]
    partition_by: tuple[str, ...]
    has_date: bool
    layer: str
    allowed_exchanges: frozenset[str] | None
    ts_column: str


class EventTableDomain(Protocol):
    """Canonical interface implemented by event-table domain modules."""

    spec: TableSpec

    def canonicalize_vendor_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        """Convert vendor-neutral parsed frame into canonical semantic columns."""

    def encode_storage(self, df: pl.DataFrame) -> pl.DataFrame:
        """Encode canonical float columns into storage integers."""

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """Cast/select to canonical schema and column order."""

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply table validation and return filtered valid rows."""

    def required_decode_columns(self) -> tuple[str, ...]:
        """Return extra columns required to run decode."""

    def decode_storage(self, df: pl.DataFrame, *, keep_ints: bool = False) -> pl.DataFrame:
        """Decode storage integers into researcher-facing float columns."""

    def decode_storage_lazy(self, lf: pl.LazyFrame, *, keep_ints: bool = False) -> pl.LazyFrame:
        """Decode storage integers lazily into researcher-facing float columns."""


class DimensionTableDomain(Protocol):
    """Canonical interface implemented by dimension-table domain modules."""

    spec: TableSpec

    def normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """Cast/select to canonical schema and column order."""

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate dimension invariants and return normalized rows."""

    def bootstrap(self, snapshot_df: pl.DataFrame) -> pl.DataFrame:
        """Create initial table state from a full snapshot."""

    def upsert(self, current_df: pl.DataFrame, updates_df: pl.DataFrame) -> pl.DataFrame:
        """Apply incremental changes and return the next table state."""


AnyTableDomain: TypeAlias = EventTableDomain | DimensionTableDomain
