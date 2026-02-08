"""Research API for the Pointline data lake.

This package provides four layers:

1. Core API (explicit symbol resolution):
   - research.load_trades(symbol_id=..., start_ts_us=..., end_ts_us=...)
   - Best for: production research, reproducibility, when you need control

2. Query API (automatic symbol resolution):
   - research.query.trades(exchange=..., symbol=..., start=..., end=...)
   - Best for: exploration, prototyping, quick checks
   - Optional: decoded=True for float outputs at the edge

3. Pipeline API (contract-first v2 execution):
   - research.pipeline(request: dict) -> dict
   - Best for: production research workflows with strict PIT/gate controls

4. Workflow API (hybrid multi-stage orchestration):
   - research.workflow(request: dict) -> dict
   - Best for: composing event_joined/tick_then_bar/bar_then_feature in one run

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

Example - Core API (decoded convenience):
    >>> from pointline import research
    >>> trades = research.load_trades_decoded(
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

Example - Query API (decoded):
    >>> trades = query.trades(
    ...     exchange="binance-futures",
    ...     symbol="SOLUSDT",
    ...     start="2024-05-01",
    ...     end="2024-05-02",
    ...     decoded=True,
    ... )
"""

# Import everything from core module to maintain backward compatibility
# Import query module (not individual functions, so users access via query.*)
from pointline.research import features, query
from pointline.research.contracts import (
    validate_quant_research_input_v2,
    validate_quant_research_output_v2,
    validate_quant_research_workflow_input_v2,
    validate_quant_research_workflow_output_v2,
)
from pointline.research.core import (
    _apply_filters,
    _derive_date_bounds_from_ts,
    _normalize_timestamp,
    _ts_us_to_date,
    _validate_ts_col,
    _validate_ts_range,
    load_book_snapshot_25,
    load_book_snapshot_25_decoded,
    load_kline_1h_decoded,
    load_quotes,
    load_quotes_decoded,
    load_trades,
    load_trades_decoded,
    read_table,
    scan_table,
    table_path,
)

# Import discovery functions (data exploration API)
from pointline.research.discovery import (
    data_coverage,
    list_exchanges,
    list_symbols,
    list_tables,
    summarize_symbol,
)
from pointline.research.pipeline import compile_request, pipeline
from pointline.research.workflow import compile_workflow_request, workflow

__all__ = [
    # Core API functions
    "table_path",
    "scan_table",
    "read_table",
    "load_trades",
    "load_quotes",
    "load_book_snapshot_25",
    "load_trades_decoded",
    "load_quotes_decoded",
    "load_book_snapshot_25_decoded",
    "load_kline_1h_decoded",
    # Discovery API functions
    "list_exchanges",
    "list_symbols",
    "list_tables",
    "data_coverage",
    "summarize_symbol",
    # Query module (convenience layer)
    "query",
    # Pipeline API
    "pipeline",
    "compile_request",
    "workflow",
    "compile_workflow_request",
    "validate_quant_research_input_v2",
    "validate_quant_research_output_v2",
    "validate_quant_research_workflow_input_v2",
    "validate_quant_research_workflow_output_v2",
    # Feature engineering utilities
    "features",
    # Internal functions (exposed for testing/advanced use)
    "_normalize_timestamp",
    "_apply_filters",
    "_derive_date_bounds_from_ts",
    "_ts_us_to_date",
    "_validate_ts_col",
    "_validate_ts_range",
]
