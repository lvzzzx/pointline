"""dim_exchange table: exchange metadata as data instead of hardcoded config.

Replaces EXCHANGE_MAP, EXCHANGE_METADATA, EXCHANGE_TIMEZONES in config.py.
Unpartitioned Silver dimension table.
"""

from __future__ import annotations

import polars as pl

DIM_EXCHANGE_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,  # PK, e.g., "binance-futures"
    "exchange_id": pl.Int16,  # Unique, stable integer ID
    "asset_class": pl.Utf8,  # e.g., "crypto-spot", "crypto-derivatives", "stocks-cn"
    "timezone": pl.Utf8,  # IANA timezone, e.g., "UTC", "Asia/Shanghai"
    "description": pl.Utf8,
    "is_active": pl.Boolean,
}


def normalize_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast to canonical schema and select only schema columns."""
    missing = [col for col in DIM_EXCHANGE_SCHEMA if col not in df.columns]
    if missing:
        raise ValueError(f"dim_exchange missing required columns: {missing}")

    casts = [pl.col(col).cast(dtype) for col, dtype in DIM_EXCHANGE_SCHEMA.items()]
    return df.with_columns(casts).select(list(DIM_EXCHANGE_SCHEMA.keys()))


def bootstrap_from_config() -> pl.DataFrame:
    """Build dim_exchange DataFrame from current hardcoded config values.

    This is used for initial seeding. After migration, exchanges are managed
    by inserting rows into the dim_exchange table directly.
    """
    from pointline.config import EXCHANGE_MAP, EXCHANGE_METADATA, EXCHANGE_TIMEZONES

    rows = []
    for exchange, exchange_id in EXCHANGE_MAP.items():
        meta = EXCHANGE_METADATA.get(exchange, {})
        rows.append(
            {
                "exchange": exchange,
                "exchange_id": exchange_id,
                "asset_class": meta.get("asset_class", "unknown"),
                "timezone": EXCHANGE_TIMEZONES.get(exchange, "UTC"),
                "description": meta.get("description", ""),
                "is_active": meta.get("is_active", True),
            }
        )

    df = pl.DataFrame(rows, schema=DIM_EXCHANGE_SCHEMA)
    return df.sort("exchange_id")


def canonical_columns() -> tuple[str, ...]:
    return tuple(DIM_EXCHANGE_SCHEMA.keys())
