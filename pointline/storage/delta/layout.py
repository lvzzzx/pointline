"""Path layout helpers for v2 Delta storage adapters."""

from __future__ import annotations

from pathlib import Path


def default_silver_root(lake_root: Path) -> Path:
    return lake_root / "silver"


def table_path(*, silver_root: Path, table_name: str) -> Path:
    return silver_root / table_name
