"""Type definitions for the Pointline research API.

This module provides shared type aliases for type safety and IDE autocomplete support.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

# Table name literal for type checking and IDE autocomplete
# This provides static type checking and prevents typos in table names.
#
# IMPORTANT: When adding new tables to TABLE_PATHS in config.py, update this literal.
# A CI test (test_type_registry_sync.py) ensures this stays in sync.
TableName = Literal[
    "dim_symbol",
    "stock_basic_cn",
    "dim_asset_stats",
    "ingest_manifest",
    "validation_log",
    "dq_summary",
    "trades",
    "quotes",
    "book_snapshot_25",
    "derivative_ticker",
    "kline_1h",
    "szse_l3_orders",
    "szse_l3_ticks",
]

# Timestamp input type - accepts int microseconds, datetime objects, or ISO strings
# This allows flexible timestamp specification in research API functions.
# ISO strings: "2024-05-01", "2024-05-01T12:00:00", "2024-05-01T12:00:00+00:00"
TimestampInput = int | datetime | str

# Column selection type
ColumnList = tuple[str, ...] | list[str]

__all__ = [
    "TableName",
    "TimestampInput",
    "ColumnList",
]
