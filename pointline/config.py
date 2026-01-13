import os
import re
import warnings
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib

# Base Paths
DEFAULT_LAKE_ROOT = Path.home() / "data" / "lake"
CONFIG_PATH = Path(
    os.getenv("POINTLINE_CONFIG", str(Path.home() / ".config" / "pointline" / "config.toml"))
).expanduser()


def _read_config_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        warnings.warn(f"Failed to parse Pointline config at {path}: {exc}")
        return {}


def _resolve_lake_root() -> Path:
    env_value = os.getenv("LAKE_ROOT")
    if env_value:
        return Path(env_value).expanduser()

    config = _read_config_file(CONFIG_PATH)
    config_value = config.get("lake_root")
    if isinstance(config_value, str) and config_value.strip():
        return Path(config_value).expanduser()

    return DEFAULT_LAKE_ROOT


LAKE_ROOT = _resolve_lake_root()


def load_config() -> dict:
    """Return parsed config content from CONFIG_PATH."""
    return _read_config_file(CONFIG_PATH)


def get_config_lake_root() -> str | None:
    """Return lake_root from config, if present."""
    config = load_config()
    value = config.get("lake_root")
    return value if isinstance(value, str) and value.strip() else None


def _format_toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def set_config_lake_root(value: str | Path) -> Path:
    """Persist lake_root to the user config file."""
    resolved = Path(value).expanduser()
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_line = f"lake_root = {_format_toml_string(str(resolved))}"

    if CONFIG_PATH.exists():
        lines = CONFIG_PATH.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    updated = False
    pattern = re.compile(r"^\s*lake_root\s*=")
    for idx, line in enumerate(lines):
        if pattern.match(line):
            lines[idx] = new_line
            updated = True
            break

    if not updated:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(new_line)

    CONFIG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return resolved

# Table Registry (Table Name -> Relative Path from LAKE_ROOT)
TABLE_PATHS = {
    "dim_symbol": "silver/dim_symbol",
    "dim_asset_stats": "silver/dim_asset_stats",
    "ingest_manifest": "silver/ingest_manifest",
    "trades": "silver/trades",
    "quotes": "silver/quotes",
    "book_snapshot_25": "silver/book_snapshot_25",
    "derivative_ticker": "silver/derivative_ticker",
    "l2_updates": "silver/l2_updates",
    "l2_state_checkpoint": "gold/l2_state_checkpoint",
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


def get_exchange_name(exchange_id: int) -> str:
    """
    Get normalized exchange name for a given exchange_id.
    
    This is the reverse mapping of get_exchange_id().
    
    Args:
        exchange_id: Exchange ID to look up
        
    Returns:
        Normalized exchange name (e.g., "binance-futures")
        
    Raises:
        ValueError: If exchange_id is not found in EXCHANGE_MAP
    """
    for name, eid in EXCHANGE_MAP.items():
        if eid == exchange_id:
            return normalize_exchange(name)
    raise ValueError(
        f"Exchange ID {exchange_id} not found in EXCHANGE_MAP. "
        f"Available IDs: {sorted(EXCHANGE_MAP.values())}"
    )


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


# Asset to CoinGecko Mapping
# Maps base_asset (from dim_symbol) to CoinGecko coin_id
# Used for fetching asset statistics from CoinGecko API
ASSET_TO_COINGECKO_MAP = {
    # Major cryptocurrencies
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "ADA": "cardano",
    "TRX": "tron",
    "UNI": "uniswap",
    "DOT": "polkadot",
    "DOGE": "dogecoin",
    "AVAX": "avalanche-2",
    "SHIB": "shiba-inu",
    "MATIC": "matic-network",
    "LTC": "litecoin",
    "BCH": "bitcoin-cash",
    "XLM": "stellar",
    "XMR": "monero",
    "ZEC": "zcash",
    "LINK": "chainlink",
    "ATOM": "cosmos",
    "ALGO": "algorand",
    "FIL": "filecoin",
    "ETC": "ethereum-classic",
    "HBAR": "hedera-hashgraph",
    "NEAR": "near",
    "APT": "aptos",
    "SUI": "sui",
    "TON": "the-open-network",
    "OP": "optimism",
    "ARB": "arbitrum",
    "INJ": "injective-protocol",
    "TIA": "celestia",
    "SEI": "sei-network",
    "TAO": "bittensor",
    "HYPE": "hyperliquid",
    "CCUSDT": "cetus-protocol",  # Note: May need adjustment based on actual CoinGecko ID
}


def get_coingecko_coin_id(base_asset: str) -> str | None:
    """
    Get CoinGecko coin_id for a given base_asset.
    
    This is the canonical source of truth for base_asset → CoinGecko coin_id mapping.
    
    Args:
        base_asset: Base asset ticker (e.g., "BTC", "ETH")
        
    Returns:
        CoinGecko coin_id (e.g., "bitcoin", "ethereum") or None if not found
    """
    return ASSET_TO_COINGECKO_MAP.get(base_asset.upper())





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
