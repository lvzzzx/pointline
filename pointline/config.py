import os
from pathlib import Path

# Base Paths
LAKE_ROOT = Path(os.getenv("LAKE_ROOT", "./data/lake"))

# Table Registry (Table Name -> Relative Path from LAKE_ROOT)
TABLE_PATHS = {
    "dim_symbol": "silver/dim_symbol",
    "ingest_manifest": "silver/ingest_manifest",
    "trades": "silver/trades",
    "quotes": "silver/quotes",
    "book_snapshots_top25": "silver/book_snapshots_top25",
}

# Storage Settings
STORAGE_OPTIONS = {
    "compression": "zstd",
}

# Exchange Registry
# Maps exchange names (as used by Tardis API) to internal exchange_id (u16)
# IDs should be stable - do not reassign existing IDs
# NOTE: Existing IDs (1-3) are preserved for backward compatibility
EXCHANGE_MAP = {
    # Major Spot Exchanges (preserving existing IDs)
    "binance": 1,
    "binance-futures": 2,  # Preserved from original
    "coinbase": 3,
    
    # Additional Spot Exchanges
    "kraken": 4,
    "okx": 5,  # Formerly OKEx
    "huobi": 6,
    "gate": 7,  # Gate.io
    "bitfinex": 8,
    "bitstamp": 9,
    "gemini": 10,
    "crypto-com": 11,
    "kucoin": 12,
    "binance-us": 13,
    "coinbase-pro": 14,  # Legacy Coinbase Pro
    
    # Derivatives Exchanges
    "binance-coin-futures": 20,
    "deribit": 21,
    "bybit": 22,
    "okx-futures": 23,
    "bitmex": 24,
    "ftx": 25,  # Historical data only
    "dydx": 26,
}


def normalize_exchange(exchange: str) -> str:
    """
    Normalize exchange name for consistent lookup.
    
    Normalizes by lowercasing and trimming whitespace.
    This is the canonical normalization used before EXCHANGE_MAP lookup.
    
    Args:
        exchange: Raw exchange name (may have mixed case, whitespace)
        
    Returns:
        Normalized exchange string (lowercase, trimmed)
    """
    return exchange.lower().strip()


def get_exchange_id(exchange: str) -> int:
    """
    Get exchange_id for a given exchange name.
    
    This is the canonical source of truth for exchange → exchange_id mapping.
    Normalizes the exchange name before lookup.
    
    Args:
        exchange: Exchange name (will be normalized before lookup)
        
    Returns:
        Exchange ID (Int16 compatible)
        
    Raises:
        ValueError: If exchange is not found in EXCHANGE_MAP after normalization
    """
    normalized = normalize_exchange(exchange)
    if normalized not in EXCHANGE_MAP:
        raise ValueError(
            f"Exchange '{exchange}' (normalized: '{normalized}') not found in EXCHANGE_MAP. "
            f"Available exchanges: {sorted(EXCHANGE_MAP.keys())}"
        )
    return EXCHANGE_MAP[normalized]


# Asset Type Registry
# Maps Tardis instrument type strings to internal asset_type (u8)
# Supports aliases for common variations
TYPE_MAP = {
    # Primary types
    "spot": 0,
    "perpetual": 1,
    "future": 2,
    "option": 3,
    
    # Aliases (map to same values)
    "perp": 1,  # Common abbreviation for perpetual
    "swap": 1,  # Some exchanges call perpetuals "swaps"
    "futures": 2,  # Plural form
    "options": 3,  # Plural form
}





def get_table_path(table_name: str) -> Path:
    """
    Resolves the absolute path for a given table name.
    
    Args:
        table_name: The name of the table to resolve.
        
    Returns:
        Path: The absolute path to the table.
        
    Raises:
        KeyError: If the table name is not registered in TABLE_PATHS.
    """
    if table_name not in TABLE_PATHS:
        raise KeyError(f"Table '{table_name}' not found in TABLE_PATHS registry.")
    
    return LAKE_ROOT / TABLE_PATHS[table_name]
