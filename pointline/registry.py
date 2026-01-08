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
            # We only need symbol_id and exchange_id for resolution
            # We assume symbol_id is globally unique and stable across time
            _SYMBOL_CACHE = pl.read_delta(str(path)).select(
                ["symbol_id", "exchange_id"]
            ).unique()
        except Exception as e:
            logger.warning(f"Failed to load dim_symbol registry: {e}")
            # Return empty schema if table doesn't exist
            _SYMBOL_CACHE = pl.DataFrame(schema={"symbol_id": pl.Int64, "exchange_id": pl.Int16})
            
    return _SYMBOL_CACHE

def resolve_symbol(symbol_id: int) -> tuple[str, int]:
    """
    Resolves a symbol_id to its (exchange_name, exchange_id).
    
    Args:
        symbol_id: The global symbol identifier.
        
    Returns:
        tuple[str, int]: (normalized_exchange_name, exchange_id)
        
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
    
    # Map ID to name using config source of truth
    exchange_name = get_exchange_name(exchange_id)
    
    return exchange_name, exchange_id
