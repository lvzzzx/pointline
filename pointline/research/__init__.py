"""Research API for the Pointline data lake.

This package provides two layers for querying data:

1. Core API (explicit symbol resolution):
   - research.load_trades(symbol_id=..., start_ts_us=..., end_ts_us=...)
   - Best for: production research, reproducibility, when you need control

2. Query API (automatic symbol resolution):
   - research.query.trades(exchange=..., symbol=..., start=..., end=...)
   - Best for: exploration, prototyping, quick checks

Example - Core API (explicit):
    >>> from pointline import research, registry
    >>> from datetime import datetime, timezone
    >>>
    >>> # Explicitly resolve symbol_ids
    >>> symbols = registry.find_symbol("SOLUSDT", exchange="binance-futures")
    >>> symbol_ids = symbols["symbol_id"].to_list()
    >>>
    >>> # Load with explicit symbol_ids
    >>> trades = research.load_trades(
    ...     symbol_id=symbol_ids,
    ...     start_ts_us=datetime(2024, 5, 1, tzinfo=timezone.utc),
    ...     end_ts_us=datetime(2024, 5, 2, tzinfo=timezone.utc),
    ... )

Example - Query API (implicit):
    >>> from pointline.research import query
    >>>
    >>> # Automatic symbol resolution
    >>> trades = query.trades(
    ...     exchange="binance-futures",
    ...     symbol="SOLUSDT",
    ...     start="2024-05-01",
    ...     end="2024-05-02",
    ... )
"""

# Import everything from core module to maintain backward compatibility
# Import query module (not individual functions, so users access via query.*)
from pointline.research import query
from pointline.research.core import (
    _apply_filters,
    _derive_date_bounds_from_ts,
    _normalize_timestamp,
    _ts_us_to_date,
    _validate_ts_col,
    _validate_ts_range,
    list_tables,
    load_book_snapshot_25,
    load_quotes,
    load_trades,
    read_table,
    scan_table,
    table_path,
)

__all__ = [
    # Core API functions
    "list_tables",
    "table_path",
    "scan_table",
    "read_table",
    "load_trades",
    "load_quotes",
    "load_book_snapshot_25",
    # Query module (convenience layer)
    "query",
    # Internal functions (exposed for testing/advanced use)
    "_normalize_timestamp",
    "_apply_filters",
    "_derive_date_bounds_from_ts",
    "_ts_us_to_date",
    "_validate_ts_col",
    "_validate_ts_range",
]
