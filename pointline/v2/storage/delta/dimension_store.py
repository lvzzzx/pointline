"""Delta-backed dimension store for v2 ingestion checks."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.v2 import dim_symbol as dim_symbol_core
from pointline.v2.storage.contracts import DimensionStore
from pointline.v2.storage.delta._utils import (
    normalize_to_spec,
    overwrite_delta,
    read_delta_or_empty,
    validate_against_spec,
)
from pointline.v2.storage.delta.layout import table_path


class DeltaDimensionStore(DimensionStore):
    """Delta-backed read/write dimension store for v2 ingestion."""

    def __init__(
        self,
        *,
        silver_root: Path | None = None,
        dim_symbol_path: Path | None = None,
    ) -> None:
        if dim_symbol_path is None and silver_root is None:
            raise ValueError("Provide either silver_root or dim_symbol_path")

        if dim_symbol_path is None:
            assert silver_root is not None
            dim_symbol_path = table_path(silver_root=silver_root, table_name="dim_symbol")
        self.dim_symbol_path = dim_symbol_path

    def load_dim_symbol(self) -> pl.DataFrame:
        df = read_delta_or_empty(self.dim_symbol_path, spec=DIM_SYMBOL)
        validate_against_spec(df, DIM_SYMBOL)
        return df

    def current_version(self) -> int | None:
        if not self.dim_symbol_path.exists():
            return None
        try:
            return int(DeltaTable(str(self.dim_symbol_path)).version())
        except TableNotFoundError:
            return None

    def save_dim_symbol(
        self,
        df: pl.DataFrame,
        *,
        expected_version: int | None = None,
    ) -> int:
        current = self.current_version()
        if expected_version is not None and current != expected_version:
            raise ValueError(
                f"dim_symbol version mismatch: expected {expected_version}, current {current}"
            )

        validate_against_spec(df, DIM_SYMBOL)
        normalized = normalize_to_spec(df, DIM_SYMBOL)
        dim_symbol_core.validate(normalized)
        overwrite_delta(self.dim_symbol_path, df=normalized, partition_by=DIM_SYMBOL.partition_by)

        new_version = self.current_version()
        if new_version is None:
            raise RuntimeError("dim_symbol write completed but version is unavailable")
        return new_version
