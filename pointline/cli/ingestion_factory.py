"""Ingestion service factory for CLI."""

from __future__ import annotations

from pointline.config import get_table_path
from pointline.io.base_repository import BaseDeltaRepository
from pointline.services.book_snapshots_service import BookSnapshotsIngestionService
from pointline.services.derivative_ticker_service import DerivativeTickerIngestionService
from pointline.services.l2_updates_service import L2UpdatesIngestionService
from pointline.services.klines_service import KlinesIngestionService
from pointline.services.quotes_service import QuotesIngestionService
from pointline.services.trades_service import TradesIngestionService

TABLE_PARTITIONS = {
    "trades": ["exchange", "date"],
    "quotes": ["exchange", "date"],
    "book_snapshot_25": ["exchange", "date"],
    "derivative_ticker": ["exchange", "date"],
    "kline_1h": ["exchange", "date"],
    "l2_updates": ["exchange", "date", "symbol_id"],
    "l2_state_checkpoint": ["exchange", "date", "symbol_id"],
}


def create_ingestion_service(data_type: str, manifest_repo):
    """Create the appropriate ingestion service based on data type."""
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
    if data_type == "incremental_book_L2":
        repo = BaseDeltaRepository(
            get_table_path("l2_updates"),
            partition_by=["exchange", "date", "symbol_id"],
        )
        return L2UpdatesIngestionService(repo, dim_symbol_repo, manifest_repo)
    if data_type == "kline_1h":
        repo = BaseDeltaRepository(
            get_table_path("kline_1h"),
            partition_by=["exchange", "date"],
        )
        return KlinesIngestionService(repo, dim_symbol_repo, manifest_repo)
    raise ValueError(f"Unsupported data type: {data_type}")
