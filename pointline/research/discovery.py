"""Data discovery API for exploring the Pointline data lake.

This module provides functions to discover available exchanges, symbols, tables,
and data coverage without prior knowledge of the data lake contents.

Designed for both human researchers and LLM agents to enable self-service exploration.

Examples:
    >>> from pointline import research
    >>>
    >>> # Discover exchanges
    >>> exchanges = research.list_exchanges(asset_class="crypto-derivatives")
    >>>
    >>> # Find symbols
    >>> symbols = research.list_symbols(exchange="binance-futures", base_asset="BTC")
    >>>
    >>> # Check coverage
    >>> coverage = research.data_coverage("binance-futures", "BTCUSDT")
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl

from pointline.config import (
    TABLE_PATHS,
    get_asset_class_exchanges,
    get_asset_type_name,
    get_exchange_metadata,
    get_exchange_supported_tables,
    get_table_path,
    normalize_exchange,
)
from pointline.introspection import get_schema
from pointline.research.core import _normalize_timestamp
from pointline.tables.asset_class import ASSET_CLASS_TAXONOMY
from pointline.tables.dim_symbol import read_dim_symbol_table


def list_exchanges(
    *,
    asset_class: str | list[str] | None = None,
    active_only: bool = True,
    include_stats: bool = False,
) -> pl.DataFrame:
    """List all exchanges with available data.

    Args:
        asset_class: Filter by asset class. Options:
            - "crypto": All crypto exchanges (spot + derivatives)
            - "crypto-spot": Crypto spot only
            - "crypto-derivatives": Crypto derivatives only
            - "stocks": All stock exchanges
            - "stocks-cn": Chinese stocks only
            - "all": All asset classes
            - Can pass list: ["crypto-spot", "stocks-cn"]
        active_only: If True, exclude historical-only exchanges (e.g., ftx, coinbase-pro)
        include_stats: If True, include symbol counts (requires dim_symbol read)

    Returns:
        DataFrame with columns:
            - exchange: str (e.g., "binance-futures")
            - exchange_id: int
            - asset_class: str (e.g., "crypto-derivatives")
            - description: str
            - is_active: bool
            - [if include_stats] symbol_count: int

    Examples:
        >>> # List all active exchanges
        >>> exchanges = list_exchanges()
        >>>
        >>> # List only crypto derivatives
        >>> crypto_deriv = list_exchanges(asset_class="crypto-derivatives")
        >>>
        >>> # List Chinese stocks + crypto spot
        >>> combined = list_exchanges(asset_class=["stocks-cn", "crypto-spot"])
    """
    from pointline.config import _ensure_dim_exchange

    dim_ex = _ensure_dim_exchange()
    source_items: list[tuple[str, dict]] = list(dim_ex.items())

    exchanges_data = []

    for exchange_name, metadata in source_items:
        # Filter by active status
        if active_only and not metadata.get("is_active", True):
            continue

        # Filter by asset class
        if asset_class is not None:
            # Normalize to list
            asset_classes = [asset_class] if isinstance(asset_class, str) else asset_class

            # Handle "all" special case
            if "all" not in asset_classes:
                # Expand parent classes to children
                expanded_classes = set()
                for ac in asset_classes:
                    if ac in ASSET_CLASS_TAXONOMY:
                        taxonomy_entry = ASSET_CLASS_TAXONOMY[ac]
                        if "children" in taxonomy_entry:
                            expanded_classes.update(taxonomy_entry["children"])
                        else:
                            expanded_classes.add(ac)
                    else:
                        expanded_classes.add(ac)
                    # Also match parent prefix (e.g., "crypto" matches "crypto-spot")
                    expanded_classes.add(ac)

                row_class = metadata.get("asset_class", "")
                if row_class not in expanded_classes and not any(
                    row_class.startswith(ec + "-") for ec in expanded_classes
                ):
                    continue

        exchanges_data.append(
            {
                "exchange": exchange_name,
                "exchange_id": metadata["exchange_id"],
                "asset_class": metadata.get("asset_class", "unknown"),
                "description": metadata.get("description", ""),
                "is_active": metadata.get("is_active", True),
            }
        )

    # Create DataFrame
    df = pl.DataFrame(exchanges_data)

    if df.is_empty():
        # Return empty DataFrame with correct schema
        schema = {
            "exchange": pl.Utf8,
            "exchange_id": pl.Int16,
            "asset_class": pl.Utf8,
            "description": pl.Utf8,
            "is_active": pl.Boolean,
        }
        if include_stats:
            schema["symbol_count"] = pl.Int64
        return pl.DataFrame(schema=schema)

    # Add symbol counts if requested
    if include_stats:
        # Read dim_symbol and count unique symbols per exchange
        dim_symbol = read_dim_symbol_table(columns=["exchange", "symbol_id"])
        symbol_counts = (
            dim_symbol.group_by("exchange")
            .agg(pl.col("symbol_id").n_unique().alias("symbol_count"))
            .with_columns(pl.col("exchange").str.to_lowercase())
        )

        # Left join to preserve exchanges with no symbols
        df = df.with_columns(pl.col("exchange").str.to_lowercase().alias("_exchange_lower"))
        df = df.join(
            symbol_counts.rename({"exchange": "_exchange_lower"}),
            on="_exchange_lower",
            how="left",
        )
        df = df.drop("_exchange_lower")
        df = df.with_columns(pl.col("symbol_count").fill_null(0))

    # Sort by asset_class then exchange_id
    df = df.sort(["asset_class", "exchange_id"])

    return df


def list_symbols(
    *,
    exchange: str | list[str] | None = None,
    asset_class: str | list[str] | None = None,
    base_asset: str | list[str] | None = None,
    quote_asset: str | list[str] | None = None,
    asset_type: str | list[str] | None = None,
    search: str | None = None,
    current_only: bool = True,
    include_stats: bool = False,
) -> pl.DataFrame:
    """List symbols with flexible filtering.

    Args:
        exchange: Filter by exchange name(s)
        asset_class: Filter by asset class (e.g., "crypto", "stocks-cn")
        base_asset: Filter by base asset (e.g., "BTC", "ETH")
        quote_asset: Filter by quote asset (e.g., "USDT", "USD")
        asset_type: Filter by asset type (e.g., "spot", "perpetual", "future")
        search: Fuzzy search across symbol name, base/quote assets
        current_only: If True, only return currently active symbols (is_current=true)
        include_stats: If True, add row counts and date ranges (NOT YET IMPLEMENTED)

    Returns:
        DataFrame with columns from dim_symbol:
            - symbol_id: int
            - exchange_id: int
            - exchange: str
            - exchange_symbol: str
            - base_asset: str
            - quote_asset: str
            - asset_type: str (decoded: "spot", "perpetual", etc.)
            - tick_size: float
            - lot_size: float
            - contract_size: float
            - valid_from_ts: int
            - valid_until_ts: int
            - is_current: bool

    Examples:
        >>> # List all symbols on Binance Futures
        >>> symbols = list_symbols(exchange="binance-futures")
        >>>
        >>> # Find all BTC perpetuals across exchanges
        >>> btc_perps = list_symbols(base_asset="BTC", asset_type="perpetual")
        >>>
        >>> # Search for SOL symbols
        >>> sol = list_symbols(search="SOL")
        >>>
        >>> # Chinese stocks on SZSE
        >>> szse_stocks = list_symbols(exchange="szse", asset_class="stocks-cn")
    """
    # Read dim_symbol
    df = read_dim_symbol_table()

    # Filter by current_only
    if current_only:
        df = df.filter(pl.col("is_current") == True)  # noqa: E712

    # Filter by exchange
    if exchange is not None:
        exchanges = [exchange] if isinstance(exchange, str) else list(exchange)
        # Normalize exchange names
        normalized = [normalize_exchange(e) for e in exchanges]
        df = df.filter(pl.col("exchange").is_in(normalized))

    # Filter by asset_class
    if asset_class is not None:
        asset_classes = [asset_class] if isinstance(asset_class, str) else list(asset_class)
        # Get exchanges for each asset class
        matching_exchanges = []
        for ac in asset_classes:
            matching_exchanges.extend(get_asset_class_exchanges(ac))
        # Normalize
        matching_exchanges = [normalize_exchange(e) for e in matching_exchanges]
        df = df.filter(pl.col("exchange").is_in(matching_exchanges))

    # Filter by base_asset
    if base_asset is not None:
        assets = [base_asset] if isinstance(base_asset, str) else list(base_asset)
        # Case-insensitive matching
        df = df.filter(pl.col("base_asset").str.to_uppercase().is_in([a.upper() for a in assets]))

    # Filter by quote_asset
    if quote_asset is not None:
        assets = [quote_asset] if isinstance(quote_asset, str) else list(quote_asset)
        # Case-insensitive matching
        df = df.filter(pl.col("quote_asset").str.to_uppercase().is_in([a.upper() for a in assets]))

    # Filter by asset_type
    if asset_type is not None:
        from pointline.config import TYPE_MAP

        types = [asset_type] if isinstance(asset_type, str) else list(asset_type)
        # Map human-readable types to integers
        type_codes = []
        for t in types:
            t_lower = t.lower()
            if t_lower in TYPE_MAP:
                type_codes.append(TYPE_MAP[t_lower])
            elif t.isdigit():
                # Try as integer
                type_codes.append(int(t))

        if type_codes:
            df = df.filter(pl.col("asset_type").is_in(type_codes))

    # Fuzzy search
    if search is not None:
        q = search.lower()
        df = df.filter(
            pl.col("exchange_symbol").str.to_lowercase().str.contains(q)
            | pl.col("base_asset").str.to_lowercase().str.contains(q)
            | pl.col("quote_asset").str.to_lowercase().str.contains(q)
        )

    # Decode asset_type to human-readable names
    if not df.is_empty():
        df = df.with_columns(
            pl.col("asset_type")
            .map_elements(get_asset_type_name, return_dtype=pl.Utf8)
            .alias("asset_type_name")
        )

        # Reorder columns to put asset_type_name after asset_type
        cols = df.columns
        asset_type_idx = cols.index("asset_type")
        new_cols = cols[: asset_type_idx + 1] + ["asset_type_name"] + cols[asset_type_idx + 1 : -1]
        df = df.select(new_cols)

    # TODO: Add stats (row counts, date ranges) if include_stats=True
    if include_stats:
        # Placeholder - requires querying manifest table
        pass

    return df


def list_tables(
    layer: str = "silver",
    include_stats: bool = False,
) -> pl.DataFrame:
    """List available tables with metadata.

    Args:
        layer: Data lake layer ("silver", "gold", "reference")
        include_stats: If True, include size/row count info (NOT YET IMPLEMENTED)

    Returns:
        DataFrame with columns:
            - table_name: str
            - layer: str
            - path: str
            - has_date_partition: bool
            - description: str (placeholder - could be populated from docstrings)

    Examples:
        >>> tables = list_tables()
        >>> print(tables)
    """
    valid_layers = {"silver", "gold", "reference"}
    if layer:
        normalized_layer = layer.strip().lower()
        if normalized_layer in {"all", "*"}:
            layer = None
        elif normalized_layer not in valid_layers:
            raise ValueError(
                "layer must be one of {'silver', 'gold', 'reference'} or None. "
                f"Got '{layer}'. If you meant an exchange (e.g., 'binance-futures'), "
                "use `list_exchanges()` or `list_symbols(exchange=...)` instead."
            )
        else:
            layer = normalized_layer

    schema = {
        "table_name": pl.Utf8,
        "layer": pl.Utf8,
        "path": pl.Utf8,
        "has_date_partition": pl.Boolean,
        "description": pl.Utf8,
    }
    tables_data = []

    for table_name, rel_path in TABLE_PATHS.items():
        # Filter by layer
        table_layer = rel_path.split("/")[0]  # e.g., "silver/trades" → "silver"
        if layer and layer != table_layer:
            continue

        table_path = get_table_path(table_name)
        has_date = "date" in get_schema(table_name)

        tables_data.append(
            {
                "table_name": table_name,
                "layer": table_layer,
                "path": str(table_path),
                "has_date_partition": has_date,
                "description": _get_table_description(table_name),
            }
        )

    df = pl.DataFrame(tables_data, schema=schema)

    # TODO: Add stats (size, row counts) if include_stats=True
    if include_stats:
        # Placeholder - requires scanning Delta Lake metadata
        pass

    return df.sort("table_name")


def data_coverage(
    exchange: str,
    symbol: str,
    *,
    tables: list[str] | None = None,
    as_of: datetime | str | int | None = None,
) -> dict[str, dict[str, Any]]:
    """Check data coverage for a specific symbol across tables.

    Args:
        exchange: Exchange name (e.g., "binance-futures")
        symbol: Exchange symbol (e.g., "BTCUSDT")
        tables: List of tables to check (default: all market data tables)
        as_of: Check coverage as of a specific time (for SCD Type 2 filtering)

    Returns:
        Dictionary mapping table_name → coverage info:
        {
            "trades": {
                "available": True,
                "symbol_id": 12345,
                "reason": None,  # or error message if not available
            },
            ...
        }

    Examples:
        >>> coverage = data_coverage("binance-futures", "BTCUSDT")
        >>> print(coverage["trades"])
    """
    # Resolve symbol_id
    from pointline.tables.dim_symbol import find_symbol

    # Normalize exchange
    normalized_exchange = normalize_exchange(exchange)

    # Find symbol
    symbols_df = find_symbol(symbol, exchange=normalized_exchange)

    if symbols_df.is_empty():
        # Return empty coverage for all tables
        result = {}
        check_tables = tables if tables else _get_default_tables()
        for table_name in check_tables:
            result[table_name] = {
                "available": False,
                "reason": f"Symbol '{symbol}' not found on exchange '{exchange}'",
            }
        return result

    # Filter by as_of time if provided
    if as_of is not None:
        as_of_ts_us = _normalize_timestamp(as_of, "as_of")
        symbols_df = symbols_df.filter(
            (pl.col("valid_from_ts") <= as_of_ts_us) & (pl.col("valid_until_ts") > as_of_ts_us)
        )

        if symbols_df.is_empty():
            result = {}
            check_tables = tables if tables else _get_default_tables()
            for table_name in check_tables:
                result[table_name] = {
                    "available": False,
                    "reason": f"Symbol '{symbol}' not active on '{exchange}' at specified time",
                }
            return result

    # Get symbol_ids
    symbol_ids = symbols_df["symbol_id"].to_list()

    # Check coverage for each table
    result = {}
    check_tables = tables if tables else _get_default_tables()

    for table_name in check_tables:
        # Check if table is supported by this exchange
        supported_tables = get_exchange_supported_tables(normalized_exchange)
        if supported_tables is not None and table_name not in supported_tables:
            result[table_name] = {
                "available": False,
                "reason": f"Table '{table_name}' not supported for exchange '{exchange}'",
            }
            continue

        # Check if table exists
        if table_name not in TABLE_PATHS:
            result[table_name] = {
                "available": False,
                "reason": f"Table '{table_name}' not found in data lake",
            }
            continue

        # For now, just mark as available if we found the symbol
        # TODO: Query manifest or Delta metadata for actual row counts, date ranges
        result[table_name] = {
            "available": True,
            "symbol_id": symbol_ids[0] if len(symbol_ids) == 1 else symbol_ids,
            "reason": None,
        }

    return result


def summarize_symbol(
    symbol: str,
    *,
    exchange: str | None = None,
    as_of: datetime | str | int | None = None,
) -> None:
    """Print a rich, human-readable summary of a symbol.

    Args:
        symbol: Exchange symbol (e.g., "BTCUSDT", "000001")
        exchange: Exchange name (optional, will search all if omitted)
        as_of: Show metadata as of a specific time

    Outputs:
        Rich formatted summary to stdout (not returned)

    Examples:
        >>> # Auto-detect exchange
        >>> summarize_symbol("BTCUSDT")
        >>>
        >>> # Specify exchange
        >>> summarize_symbol("BTCUSDT", exchange="binance-futures")
    """
    # Read dim_symbol directly to get is_current column
    dim_symbol = read_dim_symbol_table()

    # Filter by symbol and exchange
    if exchange:
        symbols_df = dim_symbol.filter(
            (pl.col("exchange_symbol") == symbol)
            & (pl.col("exchange") == normalize_exchange(exchange))
        )
    else:
        symbols_df = dim_symbol.filter(pl.col("exchange_symbol") == symbol)

    if symbols_df.is_empty():
        print(f"Symbol '{symbol}' not found")
        if exchange:
            print(f"  Exchange: {exchange}")
        return

    # Filter by as_of if provided
    if as_of is not None:
        as_of_ts_us = _normalize_timestamp(as_of, "as_of")
        symbols_df = symbols_df.filter(
            (pl.col("valid_from_ts") <= as_of_ts_us) & (pl.col("valid_until_ts") > as_of_ts_us)
        )

        if symbols_df.is_empty():
            print(f"Symbol '{symbol}' not active at specified time")
            return

    # If multiple exchanges, show all
    if symbols_df["exchange"].n_unique() > 1:
        print(f"Found '{symbol}' on {symbols_df['exchange'].n_unique()} exchanges:")
        for exch in symbols_df["exchange"].unique().sort():
            print(f"  - {exch}")
        print("\nUse exchange parameter to see details for a specific exchange.")
        return

    # Single exchange - show detailed summary
    # Take the most recent version if multiple
    symbols_df = symbols_df.sort("valid_from_ts", descending=True)
    symbol_info = symbols_df.head(1).to_dicts()[0]

    print("=" * 60)
    print(f"Symbol: {symbol_info['exchange_symbol']} ({symbol_info['exchange']})")
    print("=" * 60)
    print()
    print("Metadata")
    print("-" * 60)
    print(f"  Symbol ID:        {symbol_info['symbol_id']}")
    print(f"  Exchange:         {symbol_info['exchange']} (ID: {symbol_info['exchange_id']})")
    print(f"  Base Asset:       {symbol_info['base_asset']}")
    print(f"  Quote Asset:      {symbol_info['quote_asset']}")
    print(f"  Asset Type:       {get_asset_type_name(symbol_info['asset_type'])}")
    print()
    print("  Contract Specs:")
    print(f"    Tick Size:      {symbol_info['tick_size']}")
    print(f"    Lot Size:       {symbol_info['lot_size']}")
    print(f"    Contract Size:  {symbol_info['contract_size']}")
    print()
    print("  Validity:")
    print(f"    From:           {_format_timestamp(symbol_info['valid_from_ts'])}")
    if symbol_info.get("is_current", False):
        print("    Until:          Active (current)")
    else:
        print(f"    Until:          {_format_timestamp(symbol_info['valid_until_ts'])}")
    print()

    # Show coverage
    coverage = data_coverage(symbol_info["exchange"], symbol_info["exchange_symbol"])

    print("Available Data")
    print("-" * 60)
    for table_name, info in coverage.items():
        if info["available"]:
            print(f"  ✓ {table_name:<20}")
        else:
            print(f"  ✗ {table_name:<20} ({info['reason']})")
    print()

    # Quick start example
    print("Quick Start")
    print("-" * 60)
    print("  from pointline.research import query")
    print()
    print("  trades = query.trades(")
    print(f'      exchange="{symbol_info["exchange"]}",')
    print(f'      symbol="{symbol_info["exchange_symbol"]}",')
    print('      start="2024-05-01",')
    print('      end="2024-05-02",')
    print("      decoded=True,")
    print("  )")
    print()
    print("=" * 60)


# Helper functions


def _get_table_description(table_name: str) -> str:
    """Get human-readable description for a table."""
    descriptions = {
        "dim_symbol": "SCD Type 2 symbol metadata",
        "stock_basic_cn": "Chinese stock basic information",
        "dim_asset_stats": "Daily asset statistics (circulating supply, market cap)",
        "ingest_manifest": "ETL ingestion tracking ledger",
        "validation_log": "Data quality validation log",
        "dq_summary": "Data quality summary statistics",
        "trades": "Individual trade executions",
        "quotes": "Top-of-book bid/ask quotes",
        "book_snapshot_25": "Top 25 levels order book snapshots",
        "derivative_ticker": "Funding rates, OI, mark/index prices",
        "kline_1h": "1-hour OHLCV candlesticks",
        "l3_orders": "China exchange Level 3 order placements",
        "l3_ticks": "China exchange Level 3 trade executions and cancellations",
    }
    return descriptions.get(table_name, "")


def symbol_metadata(
    symbol: str,
    exchange: str | None = None,
) -> pl.DataFrame:
    """Return symbol metadata from dim_symbol.

    Args:
        symbol: The exchange symbol to look up (e.g., "BTCUSDT").
        exchange: Optional exchange filter (e.g., "binance-futures").

    Returns:
        DataFrame with symbol metadata.
        Returns empty DataFrame if symbol not found.
    """
    dim = read_dim_symbol_table()
    result = dim.filter(pl.col("exchange_symbol") == symbol)
    if exchange is not None:
        result = result.filter(pl.col("exchange") == normalize_exchange(exchange))
    return result


def trading_days(
    exchange: str,
    start: str | datetime,
    end: str | datetime,
) -> list:
    """Return trading days for an exchange in a date range.

    For 24/7 crypto exchanges, returns every calendar day.
    For exchanges with a dim_trading_calendar entry, reads from the table.

    Args:
        exchange: Exchange name (e.g., "binance-futures", "szse")
        start: Start date (ISO string or datetime, inclusive)
        end: End date (ISO string or datetime, inclusive)

    Returns:
        Sorted list of ``datetime.date`` objects that are trading days.
    """
    import datetime as dt

    from pointline.tables.dim_trading_calendar import bootstrap_crypto
    from pointline.tables.dim_trading_calendar import trading_days as _td

    if isinstance(start, str):
        start_date = dt.date.fromisoformat(start)
    elif isinstance(start, datetime):
        start_date = start.date()
    else:
        start_date = start

    if isinstance(end, str):
        end_date = dt.date.fromisoformat(end)
    elif isinstance(end, datetime):
        end_date = end.date()
    else:
        end_date = end

    # Try reading from dim_trading_calendar table
    try:
        cal_path = get_table_path("dim_trading_calendar")
        cal_df = (
            pl.scan_delta(str(cal_path))
            .filter(
                (pl.col("exchange") == exchange)
                & (pl.col("date") >= start_date)
                & (pl.col("date") <= end_date)
            )
            .collect()
        )
        if not cal_df.is_empty():
            return _td(cal_df, exchange, start_date, end_date)
    except Exception:
        pass

    # Fallback: check if this is a crypto exchange (24/7)
    meta = get_exchange_metadata(exchange)
    if meta is None:
        normalized = normalize_exchange(exchange)
        if normalized != exchange:
            meta = get_exchange_metadata(normalized)
    asset_class = meta.get("asset_class", "") if meta else ""
    if asset_class.startswith("crypto"):
        cal_df = bootstrap_crypto(exchange, start_date, end_date)
        return _td(cal_df, exchange, start_date, end_date)

    # No calendar data available — return empty
    return []


def _get_default_tables() -> list[str]:
    """Get default list of tables to check for coverage."""
    return [
        "trades",
        "quotes",
        "book_snapshot_25",
        "derivative_ticker",
        "kline_1h",
        "l3_orders",
        "l3_ticks",
    ]


def _format_timestamp(ts_us: int) -> str:
    """Format microsecond timestamp as human-readable string."""
    if ts_us >= 2**63 - 1000:  # Near max int64
        return "Infinity"
    dt = datetime.fromtimestamp(ts_us / 1_000_000)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
