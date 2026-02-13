"""Canonical v2 schema primitives and shared constants."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl

TableKind = Literal["event", "dimension", "control"]

PRICE_SCALE = 1_000_000_000
QTY_SCALE = 1_000_000_000

INGEST_STATUS_PENDING = "pending"
INGEST_STATUS_SUCCESS = "success"
INGEST_STATUS_FAILED = "failed"
INGEST_STATUS_QUARANTINED = "quarantined"
INGEST_STATUS_VALUES: tuple[str, ...] = (
    INGEST_STATUS_PENDING,
    INGEST_STATUS_SUCCESS,
    INGEST_STATUS_FAILED,
    INGEST_STATUS_QUARANTINED,
)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: pl.DataType
    nullable: bool = False
    description: str = ""
    scale: int | None = None


@dataclass(frozen=True)
class TableSpec:
    name: str
    kind: TableKind
    column_specs: tuple[ColumnSpec, ...]
    partition_by: tuple[str, ...]
    business_keys: tuple[str, ...]
    tie_break_keys: tuple[str, ...]
    schema_version: str

    def __post_init__(self) -> None:
        column_names = self.columns()

        if len(column_names) != len(set(column_names)):
            raise ValueError(f"Duplicate column in spec '{self.name}'")

        for field_name, keys in (
            ("partition_by", self.partition_by),
            ("business_keys", self.business_keys),
            ("tie_break_keys", self.tie_break_keys),
        ):
            unknown = [key for key in keys if key not in column_names]
            if unknown:
                raise ValueError(f"{self.name}.{field_name} references unknown columns: {unknown}")

    def columns(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.column_specs)

    def required_columns(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.column_specs if not col.nullable)

    def to_polars(self) -> dict[str, pl.DataType]:
        return {col.name: col.dtype for col in self.column_specs}

    def nullable_columns(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.column_specs if col.nullable)

    def has_column(self, name: str) -> bool:
        return name in self.columns()

    def get_column(self, name: str) -> ColumnSpec:
        for column in self.column_specs:
            if column.name == name:
                return column
        raise KeyError(f"Column '{name}' not found in table '{self.name}'")

    def scaled_columns(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.column_specs if col.scale is not None)

    def scale_for(self, name: str) -> int | None:
        return self.get_column(name).scale
