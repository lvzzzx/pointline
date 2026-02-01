"""Ingestion service factory for CLI."""

from __future__ import annotations

from pointline.config import get_table_path
from pointline.io.base_repository import BaseDeltaRepository
from pointline.services.book_snapshots_service import BookSnapshotsIngestionService
from pointline.services.derivative_ticker_service import DerivativeTickerIngestionService
from pointline.services.klines_service import KlinesIngestionService
from pointline.services.quotes_service import QuotesIngestionService
from pointline.services.szse_l3_orders_service import SzseL3OrdersIngestionService
from pointline.services.szse_l3_ticks_service import SzseL3TicksIngestionService
from pointline.services.trades_service import TradesIngestionService

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

    Args:
        data_type: Type of data (trades, quotes, klines, etc.)
        manifest_repo: Ingestion manifest repository
        interval: For klines, the interval (1h, 4h, 1d, etc.)
    """
    dim_symbol_repo = BaseDeltaRepository(get_table_path("dim_symbol"))

    # Map bronze layer data_type to canonical table name.
    if data_type == "trades":
        repo = BaseDeltaRepository(
            get_table_path("trades"),
            partition_by=["exchange", "date"],
        )
        return TradesIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "quotes":
        repo = BaseDeltaRepository(
            get_table_path("quotes"),
            partition_by=["exchange", "date"],
        )
        return QuotesIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "book_snapshot_25":
        repo = BaseDeltaRepository(
            get_table_path("book_snapshot_25"),
            partition_by=["exchange", "date"],
        )
        return BookSnapshotsIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "derivative_ticker":
        repo = BaseDeltaRepository(
            get_table_path("derivative_ticker"),
            partition_by=["exchange", "date"],
        )
        return DerivativeTickerIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "klines":
        if not interval:
            raise ValueError("Klines data requires 'interval' to be specified (e.g., '1h', '4h')")
        table_name = f"kline_{interval}"
        repo = BaseDeltaRepository(
            get_table_path(table_name),
            partition_by=["exchange", "date"],
        )
        return KlinesIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "l3_orders":
        repo = BaseDeltaRepository(
            get_table_path("szse_l3_orders"),
            partition_by=["exchange", "date"],
        )
        return SzseL3OrdersIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "l3_ticks":
        repo = BaseDeltaRepository(
            get_table_path("szse_l3_ticks"),
            partition_by=["exchange", "date"],
        )
        return SzseL3TicksIngestionService(repo, dim_symbol_repo, manifest_repo)
    raise ValueError(f"Unsupported data type: {data_type}")
