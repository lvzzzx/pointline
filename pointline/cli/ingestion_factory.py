"""Ingestion service factory for CLI.

This factory creates vendor-agnostic ingestion services using the GenericIngestionService
with table-specific strategies.
"""

from __future__ import annotations

# Ensure parsers are registered by importing the package
import pointline.io.vendors  # noqa: F401  # Trigger vendor auto-registration
from pointline import tables
from pointline.config import get_table_path
from pointline.io.base_repository import BaseDeltaRepository
from pointline.services.generic_ingestion_service import GenericIngestionService, TableStrategy

TABLE_PARTITIONS = {
    "trades": ["exchange", "date"],
    "quotes": ["exchange", "date"],
    "book_snapshot_25": ["exchange", "date"],
    "derivative_ticker": ["exchange", "date"],
    "kline_1h": ["exchange", "date"],
    "szse_l3_orders": ["exchange", "date"],
    "szse_l3_ticks": ["exchange", "date"],
}


def create_ingestion_service(data_type: str, manifest_repo, *, interval: str | None = None):
    """Create the appropriate ingestion service based on data type.

    Uses GenericIngestionService with table-specific strategies for vendor-agnostic ingestion.

    Args:
        data_type: Type of data (trades, quotes, klines, etc.)
        manifest_repo: Ingestion manifest repository
        interval: For klines, the interval (1h, 4h, 1d, etc.)

    Returns:
        GenericIngestionService configured for the specified data type

    Raises:
        ValueError: If data_type is unsupported or klines interval is missing
    """
    dim_symbol_repo = BaseDeltaRepository(get_table_path("dim_symbol"))

    # Trades
    if data_type == "trades":
        repo = BaseDeltaRepository(
            get_table_path("trades"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.trades.encode_fixed_point,
            validate=tables.trades.validate_trades,
            normalize_schema=tables.trades.normalize_trades_schema,
            resolve_symbol_ids=tables.trades.resolve_symbol_ids,
        )
        return GenericIngestionService("trades", strategy, repo, dim_symbol_repo, manifest_repo)

    # Quotes
    if data_type == "quotes":
        repo = BaseDeltaRepository(
            get_table_path("quotes"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.quotes.encode_fixed_point,
            validate=tables.quotes.validate_quotes,
            normalize_schema=tables.quotes.normalize_quotes_schema,
            resolve_symbol_ids=tables.quotes.resolve_symbol_ids,
        )
        return GenericIngestionService("quotes", strategy, repo, dim_symbol_repo, manifest_repo)

    # Book snapshots
    if data_type == "book_snapshot_25":
        repo = BaseDeltaRepository(
            get_table_path("book_snapshot_25"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.book_snapshots.encode_fixed_point,
            validate=tables.book_snapshots.validate_book_snapshots,
            normalize_schema=tables.book_snapshots.normalize_book_snapshots_schema,
            resolve_symbol_ids=tables.book_snapshots.resolve_symbol_ids,
        )
        return GenericIngestionService(
            "book_snapshot_25", strategy, repo, dim_symbol_repo, manifest_repo
        )

    # Derivative ticker
    if data_type == "derivative_ticker":
        repo = BaseDeltaRepository(
            get_table_path("derivative_ticker"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.derivative_ticker.encode_fixed_point,
            validate=tables.derivative_ticker.validate_derivative_ticker,
            normalize_schema=tables.derivative_ticker.normalize_derivative_ticker_schema,
            resolve_symbol_ids=tables.derivative_ticker.resolve_symbol_ids,
        )
        return GenericIngestionService(
            "derivative_ticker", strategy, repo, dim_symbol_repo, manifest_repo
        )

    # Klines
    if data_type == "klines":
        if not interval:
            raise ValueError("Klines data requires 'interval' to be specified (e.g., '1h', '4h')")
        table_name = f"kline_{interval}"
        repo = BaseDeltaRepository(
            get_table_path(table_name),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.klines.encode_fixed_point,
            validate=tables.klines.validate_klines,
            normalize_schema=tables.klines.normalize_klines_schema,
            resolve_symbol_ids=tables.klines.resolve_symbol_ids,
            ts_col="ts_bucket_start_us",  # Klines use bucket timestamps, not event timestamps
        )
        return GenericIngestionService(table_name, strategy, repo, dim_symbol_repo, manifest_repo)

    # SZSE L3 Orders
    if data_type == "l3_orders":
        repo = BaseDeltaRepository(
            get_table_path("szse_l3_orders"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.szse_l3_orders.encode_fixed_point,
            validate=tables.szse_l3_orders.validate_szse_l3_orders,
            normalize_schema=tables.szse_l3_orders.normalize_szse_l3_orders_schema,
            resolve_symbol_ids=tables.szse_l3_orders.resolve_symbol_ids,
        )
        return GenericIngestionService(
            "szse_l3_orders", strategy, repo, dim_symbol_repo, manifest_repo
        )

    # SZSE L3 Ticks
    if data_type == "l3_ticks":
        repo = BaseDeltaRepository(
            get_table_path("szse_l3_ticks"),
            partition_by=["exchange", "date"],
        )
        strategy = TableStrategy(
            encode_fixed_point=tables.szse_l3_ticks.encode_fixed_point,
            validate=tables.szse_l3_ticks.validate_szse_l3_ticks,
            normalize_schema=tables.szse_l3_ticks.normalize_szse_l3_ticks_schema,
            resolve_symbol_ids=tables.szse_l3_ticks.resolve_symbol_ids,
        )
        return GenericIngestionService(
            "szse_l3_ticks", strategy, repo, dim_symbol_repo, manifest_repo
        )

    raise ValueError(f"Unsupported data type: {data_type}")
