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
        warnings.warn(f"Failed to parse Pointline config at {path}: {exc}", stacklevel=2)
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
BRONZE_ROOT = LAKE_ROOT / "bronze"


def load_config() -> dict:
    """Return parsed config content from CONFIG_PATH."""
    return _read_config_file(CONFIG_PATH)


def get_bronze_root(vendor: str) -> Path:
    """Return the bronze root for a given vendor (e.g., tardis, binance_vision)."""
    return BRONZE_ROOT / vendor


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

    lines = CONFIG_PATH.read_text(encoding="utf-8").splitlines() if CONFIG_PATH.exists() else []

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
    "validation_log": "silver/validation_log",
    "dq_summary": "silver/dq_summary",
    "trades": "silver/trades",
    "quotes": "silver/quotes",
    "book_snapshot_25": "silver/book_snapshot_25",
    "derivative_ticker": "silver/derivative_ticker",
    "kline_1h": "silver/kline_1h",
    "szse_l3_orders": "silver/szse_l3_orders",
    "szse_l3_ticks": "silver/szse_l3_ticks",
}

# Table registry for date column availability (used for safe filtering).
TABLE_HAS_DATE = {
    "dim_symbol": False,
    "dim_asset_stats": True,
    "ingest_manifest": True,
    "validation_log": False,
    "dq_summary": True,
    "trades": True,
    "quotes": True,
    "book_snapshot_25": True,
    "derivative_ticker": True,
    "kline_1h": True,
    "szse_l3_orders": True,
    "szse_l3_ticks": True,
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
    # Chinese Stock Exchanges
    "szse": 30,  # Shenzhen Stock Exchange
    "sse": 31,  # Shanghai Stock Exchange
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


# Exchange Timezone Registry
# Maps exchange names to their timezone for exchange-local date partitioning
# Rationale: Partition date represents the trading day in exchange-local time,
# ensuring "one trading day = one partition" for efficient queries
EXCHANGE_TIMEZONES = {
    # Crypto (24/7, use UTC as default)
    "binance": "UTC",
    "binance-futures": "UTC",
    "binance-coin-futures": "UTC",
    "binance-us": "UTC",
    "coinbase": "UTC",
    "coinbase-pro": "UTC",
    "kraken": "UTC",
    "okx": "UTC",
    "okx-futures": "UTC",
    "huobi": "UTC",
    "gate": "UTC",
    "bitfinex": "UTC",
    "bitstamp": "UTC",
    "gemini": "UTC",
    "crypto-com": "UTC",
    "kucoin": "UTC",
    "deribit": "UTC",
    "bybit": "UTC",
    "bitmex": "UTC",
    "ftx": "UTC",
    "dydx": "UTC",
    # Chinese Stock Exchanges (China Standard Time, UTC+8, no DST)
    "szse": "Asia/Shanghai",  # Shenzhen Stock Exchange
    "sse": "Asia/Shanghai",  # Shanghai Stock Exchange
}


def get_exchange_timezone(exchange: str, *, strict: bool = True) -> str:
    """
    Get timezone for exchange-local date partitioning.

    This timezone is used to derive the partition date from ts_local_us,
    ensuring that one trading day maps to one partition.

    Args:
        exchange: Exchange name (will be normalized before lookup)
        strict: If True, raise ValueError when exchange not in registry.
               If False, log warning and return "UTC" default.

    Returns:
        IANA timezone string (e.g., "UTC", "Asia/Shanghai")

    Raises:
        ValueError: If strict=True and exchange not found in EXCHANGE_TIMEZONES

    Examples:
        >>> get_exchange_timezone("binance-futures")
        'UTC'
        >>> get_exchange_timezone("szse")
        'Asia/Shanghai'
        >>> get_exchange_timezone("unknown", strict=True)
        Traceback (most recent call last):
        ValueError: Exchange 'unknown' not found in EXCHANGE_TIMEZONES registry...
    """
    normalized = normalize_exchange(exchange)

    if normalized not in EXCHANGE_TIMEZONES:
        if strict:
            raise ValueError(
                f"Exchange '{exchange}' (normalized: '{normalized}') not found in "
                f"EXCHANGE_TIMEZONES registry. This could cause incorrect date partitioning. "
                f"Please add the exchange to EXCHANGE_TIMEZONES in pointline/config.py with "
                f"its correct IANA timezone (e.g., 'UTC', 'Asia/Shanghai', 'America/New_York'). "
                f"Available exchanges: {sorted(EXCHANGE_TIMEZONES.keys())}"
            )
        else:
            import warnings

            warnings.warn(
                f"Exchange '{exchange}' (normalized: '{normalized}') not found in "
                f"EXCHANGE_TIMEZONES registry. Falling back to UTC default. "
                f"This may cause incorrect date partitioning for regional exchanges. "
                f"Add to EXCHANGE_TIMEZONES to fix.",
                UserWarning,
                stacklevel=2,
            )
            return "UTC"

    return EXCHANGE_TIMEZONES[normalized]


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
    # Level 3 order book types (SZSE, SSE)
    "l3_orders": 10,  # Individual order placements
    "l3_ticks": 11,  # Trade executions and cancellations
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

    Supports dynamic kline tables: kline_1h, kline_4h, kline_1d, etc.

    Args:
        table_name: The name of the table to resolve.

    Returns:
        Path: The absolute path to the table.

    Raises:
        KeyError: If the table name is not registered and doesn't match a known pattern.
    """
    # Check exact match first
    if table_name in TABLE_PATHS:
        return LAKE_ROOT / TABLE_PATHS[table_name]

    # Handle dynamic kline tables: kline_{interval}
    if table_name.startswith("kline_"):
        return LAKE_ROOT / f"silver/{table_name}"

    raise KeyError(
        f"Table '{table_name}' not found in TABLE_PATHS registry "
        f"and doesn't match known patterns (e.g., kline_*)."
    )
