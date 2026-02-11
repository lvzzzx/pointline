"""Data quality registry for silver tables."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableDQConfig:
    """Configuration for data quality checks on a table."""

    table_name: str
    key_columns: tuple[str, ...]
    ts_column: str | None
    numeric_columns: tuple[str, ...]
    manifest_data_type: str | None = None


TABLE_DQ_CONFIGS: dict[str, TableDQConfig] = {
    "trades": TableDQConfig(
        table_name="trades",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_local_us",
        numeric_columns=("px_int", "qty_int"),
        manifest_data_type="trades",
    ),
    "quotes": TableDQConfig(
        table_name="quotes",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_local_us",
        numeric_columns=("bid_px_int", "bid_sz_int", "ask_px_int", "ask_sz_int"),
        manifest_data_type="quotes",
    ),
    "book_snapshot_25": TableDQConfig(
        table_name="book_snapshot_25",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_local_us",
        numeric_columns=(),
        manifest_data_type="book_snapshot_25",
    ),
    "derivative_ticker": TableDQConfig(
        table_name="derivative_ticker",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_local_us",
        numeric_columns=(
            "mark_px",
            "index_px",
            "last_px",
            "funding_rate",
            "predicted_funding_rate",
            "open_interest",
        ),
        manifest_data_type="derivative_ticker",
    ),
    "kline_1h": TableDQConfig(
        table_name="kline_1h",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_bucket_start_us",
        numeric_columns=(
            "open_px_int",
            "high_px_int",
            "low_px_int",
            "close_px_int",
            "volume_qty_int",
            "quote_volume",
            "trade_count",
            "taker_buy_base_qty_int",
            "taker_buy_quote_qty",
        ),
        manifest_data_type="kline_1h",
    ),
    "l3_orders": TableDQConfig(
        table_name="l3_orders",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_local_us",
        numeric_columns=("px_int", "order_qty_int"),
        manifest_data_type="l3_orders",
    ),
    "l3_ticks": TableDQConfig(
        table_name="l3_ticks",
        key_columns=("file_id", "file_line_number"),
        ts_column="ts_local_us",
        numeric_columns=("px_int", "qty_int"),
        manifest_data_type="l3_ticks",
    ),
    "dim_symbol": TableDQConfig(
        table_name="dim_symbol",
        key_columns=("exchange_id", "exchange_symbol", "valid_from_ts"),
        ts_column="valid_from_ts",
        numeric_columns=("tick_size", "lot_size", "price_increment", "amount_increment"),
    ),
    "dim_asset_stats": TableDQConfig(
        table_name="dim_asset_stats",
        key_columns=("base_asset", "date", "source"),
        ts_column="updated_at_ts",
        numeric_columns=(
            "circulating_supply",
            "total_supply",
            "max_supply",
            "market_cap_usd",
            "fully_diluted_valuation_usd",
        ),
    ),
    "ingest_manifest": TableDQConfig(
        table_name="ingest_manifest",
        key_columns=("file_id",),
        ts_column="ingested_at",
        numeric_columns=("file_size_bytes", "row_count"),
    ),
}


def get_dq_config(table_name: str) -> TableDQConfig:
    """Get DQ configuration for a table."""
    if table_name not in TABLE_DQ_CONFIGS:
        raise ValueError(f"Unknown DQ table: {table_name}")
    return TABLE_DQ_CONFIGS[table_name]


def list_dq_tables() -> tuple[str, ...]:
    """List all tables with DQ configurations."""
    return tuple(TABLE_DQ_CONFIGS.keys())
