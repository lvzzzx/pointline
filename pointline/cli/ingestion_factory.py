"""Ingestion service factory for CLI.

This factory creates vendor-agnostic ingestion services using the GenericIngestionService
with canonical table-domain modules.
"""

from __future__ import annotations

# Ensure parsers are registered by importing the package
import pointline.io.vendors  # noqa: F401  # Trigger vendor auto-registration
from pointline.config import get_table_path
from pointline.io.base_repository import BaseDeltaRepository
from pointline.services.generic_ingestion_service import GenericIngestionService
from pointline.tables.domain_registry import get_event_domain

# Partition keys used by Delta maintenance commands.
# Keep this mapping explicit for CLI ergonomics.
TABLE_PARTITIONS = {
    "trades": ["exchange", "date"],
    "quotes": ["exchange", "date"],
    "book_snapshot_25": ["exchange", "date"],
    "derivative_ticker": ["exchange", "date"],
    "liquidations": ["exchange", "date"],
    "options_chain": ["exchange", "date"],
    "kline_1h": ["exchange", "date"],
    "kline_1d": ["exchange", "date"],
    "l3_orders": ["exchange", "date"],
    "l3_ticks": ["exchange", "date"],
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

    table_name = data_type
    if data_type == "klines":
        if not interval:
            raise ValueError("Klines data requires 'interval' to be specified (e.g., '1h', '4h')")
        table_name = f"kline_{interval}"
    supported = {
        "trades",
        "quotes",
        "book_snapshot_25",
        "derivative_ticker",
        "liquidations",
        "options_chain",
        "kline_1h",
        "kline_1d",
        "l3_orders",
        "l3_ticks",
    }
    if table_name not in supported:
        raise ValueError(f"Unsupported data type: {data_type}")

    domain = get_event_domain(table_name)
    repo = BaseDeltaRepository(
        get_table_path(table_name),
        partition_by=list(domain.spec.partition_by),
    )
    return GenericIngestionService(table_name, domain, repo, dim_symbol_repo, manifest_repo)
