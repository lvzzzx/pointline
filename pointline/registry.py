"""
Registry for resolving symbol metadata.
Acts as a read-only interface to the dim_symbol table.
"""
from __future__ import annotations

import logging
import polars as pl
from pointline.config import get_table_path, get_exchange_name

logger = logging.getLogger(__name__)

_SYMBOL_CACHE: pl.DataFrame | None = None

def _get_symbol_cache(force_refresh: bool = False) -> pl.DataFrame:
    """
    Lazy loads the dim_symbol table into memory.
    """
    global _SYMBOL_CACHE
    if _SYMBOL_CACHE is None or force_refresh:
        path = get_table_path("dim_symbol")
        try:
            # We select descriptive columns for search capability
            # We assume these are globally unique and stable across time for a given symbol_id
            _SYMBOL_CACHE = pl.read_delta(str(path)).select(
                [
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
                    "valid_until_ts"
                ]
            ).unique(subset=["symbol_id"])
        except Exception as e:
            logger.warning(f"Failed to load dim_symbol registry: {e}")
            # Return empty schema if table doesn't exist
            _SYMBOL_CACHE = pl.DataFrame(schema={
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
                "valid_until_ts": pl.Int64
            })
            
    return _SYMBOL_CACHE

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
    df = _get_symbol_cache()
    row = df.filter(pl.col("symbol_id") == symbol_id)
    
    if row.height == 0:
        # Retry once with refresh
        logger.info(f"Symbol {symbol_id} not in cache, refreshing registry...")
        df = _get_symbol_cache(force_refresh=True)
        row = df.filter(pl.col("symbol_id") == symbol_id)
        
        if row.height == 0:
            raise ValueError(f"Symbol ID {symbol_id} not found in dim_symbol registry.")

    exchange_id = row["exchange_id"][0]
    exchange_name = row["exchange"][0]
    exchange_symbol = row["exchange_symbol"][0]
    
    return exchange_name, exchange_id, exchange_symbol

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
    df = _get_symbol_cache()
    
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