"""Delta-backed event store for v2 ingestion."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import polars as pl

from pointline.schemas.registry import get_table_spec
from pointline.storage.contracts import EventStore
from pointline.storage.delta._utils import append_delta, validate_against_spec
from pointline.storage.delta.layout import table_path


class DeltaEventStore(EventStore):
    """Append-only event writer using canonical v2 table specs."""

    def __init__(
        self,
        *,
        silver_root: Path,
        table_paths: Mapping[str, Path] | None = None,
    ) -> None:
        self.silver_root = silver_root
        self.table_paths = dict(table_paths or {})

    def _resolve_path(self, table_name: str) -> Path:
        override = self.table_paths.get(table_name)
        if override is not None:
            return override
        return table_path(silver_root=self.silver_root, table_name=table_name)

    def append(self, table_name: str, df: pl.DataFrame) -> None:
        spec = get_table_spec(table_name)
        if spec.kind != "event":
            raise ValueError(f"DeltaEventStore only accepts event tables, got '{table_name}'")

        validate_against_spec(df, spec)
        append_delta(
            self._resolve_path(table_name),
            df=df,
            partition_by=spec.partition_by,
        )
