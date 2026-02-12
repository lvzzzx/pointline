"""Human-friendly error message helpers shared across modules."""

from __future__ import annotations

from difflib import get_close_matches


def exchange_required_error() -> str:
    return (
        "exchange is required for this query so partition pruning stays deterministic. "
        "Pass an explicit exchange name (for example: binance-futures)."
    )


def timestamp_required_error() -> str:
    return (
        "start_ts_us and end_ts_us are required. Integer microseconds are preferred; "
        "datetime objects are accepted when timezone-aware (timezone.utc)."
    )


def invalid_timestamp_range_error(start_ts_us: int, end_ts_us: int) -> str:
    return (
        f"Invalid timestamp range: start_ts_us={start_ts_us}, end_ts_us={end_ts_us}. "
        "end_ts_us must be greater than start_ts_us."
    )


def exchange_not_found_error(exchange: str, available_exchanges: list[str]) -> str:
    normalized = sorted(set(available_exchanges))
    suggestions = get_close_matches(exchange, normalized, n=3, cutoff=0.5)

    msg = f"Exchange '{exchange}' not found in dim_exchange. "
    if suggestions:
        msg += f"Did you mean: {', '.join(suggestions)}? "

    preview = normalized[:10]
    remaining = len(normalized) - len(preview)
    msg += f"Available exchanges: {', '.join(preview)}"
    if remaining > 0:
        msg += f", ... and {remaining} more"
    return msg


def symbol_not_found_error(symbol: str, exchange: str | None = None) -> str:
    prefix = f"Symbol '{symbol}' on exchange '{exchange}'" if exchange else f"Symbol '{symbol}'"

    return (
        f"{prefix} not found in dim_symbol. "
        "Possible causes: symbol delisted, symbol spelling mismatch, or dim_symbol not yet loaded. "
        "Try tables.dim_symbol.find_symbol(...) to inspect available symbols."
    )


def table_not_found_error(table_name: str, available_tables: list[str]) -> str:
    tables = sorted(set(available_tables))
    suggestions = get_close_matches(table_name, tables, n=3, cutoff=0.5)

    msg = f"Table '{table_name}' not found in TABLE_PATHS. "
    if suggestions:
        msg += f"Did you mean: {', '.join(suggestions)}? "
    msg += f"Known tables: {', '.join(tables)}. "
    msg += "Use research.list_tables() to inspect currently available datasets."
    return msg
