"""Delta-backed dimension store for v2 ingestion checks."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pointline.schemas.dimensions import DIM_SYMBOL
from pointline.v2.storage.contracts import DimensionStore
from pointline.v2.storage.delta._utils import read_delta_or_empty, validate_against_spec
from pointline.v2.storage.delta.layout import table_path


class DeltaDimensionStore(DimensionStore):
    """Read dimension tables needed by v2 ingestion."""

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
