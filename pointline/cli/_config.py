"""Configuration resolution: CLI flag > env var > root convention > error.

Layout contract::

    ${ROOT}/bronze/{vendor}/    — raw vendor files
    ${ROOT}/silver/{table}/     — Delta Lake tables
"""

from __future__ import annotations

import os
from pathlib import Path


def resolve_root(args_value: str | Path | None) -> Path | None:
    """Resolve lake root from CLI flag or ``POINTLINE_ROOT`` env var.

    Returns ``None`` when neither is set (callers fall back to explicit paths).
    """
    if args_value is not None:
        return Path(args_value)
    env = os.environ.get("POINTLINE_ROOT")
    if env:
        return Path(env)
    return None


def resolve_silver_root(
    args_silver: str | Path | None,
    *,
    root: Path | None = None,
) -> Path:
    """Resolve silver root: ``--silver-root`` > ``--root``/silver > env vars > error."""
    if args_silver is not None:
        return Path(args_silver)
    env = os.environ.get("POINTLINE_SILVER_ROOT")
    if env:
        return Path(env)
    if root is not None:
        return root / "silver"
    raise SystemExit("error: --root (or POINTLINE_ROOT) or --silver-root is required")


def resolve_bronze_root(
    args_bronze: str | Path | None,
    *,
    root: Path | None = None,
    vendor: str | None = None,
) -> Path:
    """Resolve bronze root: ``--bronze-root`` > ``--root``/bronze/{vendor} > env vars > error."""
    if args_bronze is not None:
        return Path(args_bronze)
    env = os.environ.get("POINTLINE_BRONZE_ROOT")
    if env:
        return Path(env)
    if root is not None:
        bronze = root / "bronze"
        if vendor:
            bronze = bronze / vendor
        return bronze
    raise SystemExit("error: --root (or POINTLINE_ROOT) or --bronze-root is required")


def resolve_tushare_token(args_value: str | None) -> str:
    """Resolve Tushare token from CLI flag or ``TUSHARE_TOKEN`` env var."""
    if args_value is not None:
        return args_value
    env = os.environ.get("TUSHARE_TOKEN")
    if env:
        return env
    raise SystemExit("error: --token or TUSHARE_TOKEN is required")
