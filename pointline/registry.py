"""
Registry for resolving symbol metadata.
Acts as a read-only interface to the dim_symbol table.
"""
from __future__ import annotations

import logging
import polars as pl
from pointline.dim_symbol import read_dim_symbol_table

logger = logging.getLogger(__name__)

def _read_dim_symbol() -> pl.DataFrame:
    """
    Read dim_symbol from Delta each call to avoid stale metadata.
    """
    try:
        # We select descriptive columns for search capability
        # We assume these are globally unique and stable across time for a given symbol_id
        return read_dim_symbol_table(
            columns=[
                "symbol_id",
                "exchange_id",
                "exchange",
                "exchange_symbol",
                "base_asset",
                "quote_asset",
                "asset_type",
                "tick_size",
                "lot_size",
                "price_increment",
                "amount_increment",
                "contract_size",
                "valid_from_ts",
                "valid_until_ts",
            ],
            unique_by=["symbol_id"],
        )
    except Exception as exc:
        logger.warning(f"Failed to load dim_symbol registry: {exc}")
        # Return empty schema if table doesn't exist
        return pl.DataFrame(
            schema={
                "symbol_id": pl.Int64,
                "exchange_id": pl.Int16,
                "exchange": pl.Utf8,
                "exchange_symbol": pl.Utf8,
                "base_asset": pl.Utf8,
                "quote_asset": pl.Utf8,
                "asset_type": pl.UInt8,
                "tick_size": pl.Float64,
                "lot_size": pl.Float64,
                "price_increment": pl.Float64,
                "amount_increment": pl.Float64,
                "contract_size": pl.Float64,
                "valid_from_ts": pl.Int64,
                "valid_until_ts": pl.Int64,
            }
        )

def resolve_symbol(symbol_id: int) -> tuple[str, int, str]:
    """
    Resolves a symbol_id to its (exchange_name, exchange_id, exchange_symbol).
    
    Args:
        symbol_id: The global symbol identifier.
        
    Returns:
        tuple[str, int, str]: (normalized_exchange_name, exchange_id, exchange_symbol)
        
    Raises:
        ValueError: If symbol_id is not found.
    """
    df = _read_dim_symbol()
    row = df.filter(pl.col("symbol_id") == symbol_id)
    
    if row.height == 0:
        raise ValueError(f"Symbol ID {symbol_id} not found in dim_symbol registry.")

    exchange_id = row["exchange_id"][0]
    exchange_name = row["exchange"][0]
    exchange_symbol = row["exchange_symbol"][0]
    
    return exchange_name, exchange_id, exchange_symbol

def resolve_symbols(symbol_ids: Iterable[int]) -> list[str]:
    """
    Resolves a list of symbol_ids to a unique list of exchange names.
    Useful for partition pruning across multiple symbols.
    """
    df = _read_dim_symbol()
    ids = list(symbol_ids)
    
    matches = df.filter(pl.col("symbol_id").is_in(ids))
        
    return matches.select("exchange").unique()["exchange"].to_list()

def find_symbol(
    query: str | None = None,
    *,
    exchange: str | None = None,
    base_asset: str | None = None,
    quote_asset: str | None = None
) -> pl.DataFrame:
    """
    Find symbols matching search criteria.
    
    Args:
        query: Fuzzy search string (matches against exchange_symbol, base_asset, quote_asset)
        exchange: Exact filter for exchange name (case-insensitive)
        base_asset: Exact filter for base asset (case-insensitive)
        quote_asset: Exact filter for quote asset (case-insensitive)
        
    Returns:
        pl.DataFrame: DataFrame containing matching symbols with their IDs and metadata.
    """
    df = _read_dim_symbol()
    
    if exchange:
        df = df.filter(pl.col("exchange").str.to_lowercase() == exchange.lower())
        
    if base_asset:
        df = df.filter(pl.col("base_asset").str.to_lowercase() == base_asset.lower())
        
    if quote_asset:
        df = df.filter(pl.col("quote_asset").str.to_lowercase() == quote_asset.lower())
        
    if query:
        q = query.lower()
        # Case-insensitive contains search across descriptive fields
        df = df.filter(
            pl.col("exchange_symbol").str.to_lowercase().str.contains(q) |
            pl.col("base_asset").str.to_lowercase().str.contains(q) |
            pl.col("quote_asset").str.to_lowercase().str.contains(q)
        )
        
    return df
