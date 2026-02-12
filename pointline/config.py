"""Configuration management module for Pointline."""

from __future__ import annotations

import logging
import os
import re
import threading
import time
import warnings
from pathlib import Path

from pointline._error_messages import exchange_not_found_error

logger = logging.getLogger(__name__)

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
    "stock_basic_cn": "silver/stock_basic_cn",
    "dim_asset_stats": "silver/dim_asset_stats",
    "ingest_manifest": "silver/ingest_manifest",
    "validation_log": "silver/validation_log",
    "dim_exchange": "silver/dim_exchange",
    "dim_trading_calendar": "silver/dim_trading_calendar",
    "dq_summary": "silver/dq_summary",
    "trades": "silver/trades",
    "quotes": "silver/quotes",
    "book_snapshot_25": "silver/book_snapshot_25",
    "derivative_ticker": "silver/derivative_ticker",
    "liquidations": "silver/liquidations",
    "options_chain": "silver/options_chain",
    "kline_1h": "silver/kline_1h",
    "kline_1d": "silver/kline_1d",
    "l3_orders": "silver/l3_orders",
    "l3_ticks": "silver/l3_ticks",
}

# Table registry for date column availability (used for safe filtering).
TABLE_HAS_DATE = {
    "dim_symbol": False,
    "dim_exchange": False,
    "dim_trading_calendar": True,
    "stock_basic_cn": False,
    "dim_asset_stats": True,
    "ingest_manifest": True,
    "validation_log": True,
    "dq_summary": True,
    "trades": True,
    "quotes": True,
    "book_snapshot_25": True,
    "derivative_ticker": True,
    "liquidations": True,
    "options_chain": True,
    "kline_1h": True,
    "kline_1d": True,
    "l3_orders": True,
    "l3_ticks": True,
}

# Table-level exchange restrictions.
# Tables listed here can only contain data from the specified exchanges.
# L3 tables are structurally coupled to Chinese market microstructure
# (channel_no, appl_seq_num sequencing, CN trading phases).
TABLE_ALLOWED_EXCHANGES: dict[str, frozenset[str]] = {
    "l3_orders": frozenset({"szse", "sse"}),
    "l3_ticks": frozenset({"szse", "sse"}),
}

# Storage Settings
STORAGE_OPTIONS = {
    "compression": "zstd",
}

# Exchange Registry (seed data — canonical source is dim_exchange table)
# Maps exchange names (as used by Tardis API) to internal exchange_id (u16)
# IDs should be stable within the active local deployment.
_SEED_EXCHANGE_MAP = {
    # Major Spot Exchanges
    "binance": 1,
    "binance-futures": 2,
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

# Pre-computed reverse mapping: exchange_id -> exchange name (O(1) lookup)
_REVERSE_EXCHANGE_MAP: dict[int, str] = {eid: name for name, eid in _SEED_EXCHANGE_MAP.items()}


# ---------------------------------------------------------------------------
# dim_exchange table loader (cached, with fallback to hardcoded dicts)
# ---------------------------------------------------------------------------
_dim_exchange_cache_lock = threading.Lock()
_dim_exchange_cache: dict[str, dict] | None = None
_dim_exchange_cache_at: float = 0.0
_DIM_EXCHANGE_TTL: float = 3600.0  # 1 hour (exchanges change rarely)


def _load_dim_exchange() -> dict[str, dict] | None:
    """Load dim_exchange table into a dict keyed by exchange name.

    Returns None if the table doesn't exist yet (cold start).
    Caches for _DIM_EXCHANGE_TTL seconds.
    """
    global _dim_exchange_cache, _dim_exchange_cache_at

    now = time.monotonic()
    if _dim_exchange_cache is not None and (now - _dim_exchange_cache_at) < _DIM_EXCHANGE_TTL:
        return _dim_exchange_cache

    with _dim_exchange_cache_lock:
        now = time.monotonic()
        if _dim_exchange_cache is not None and (now - _dim_exchange_cache_at) < _DIM_EXCHANGE_TTL:
            return _dim_exchange_cache

        try:
            import polars as pl

            table_path = get_table_path("dim_exchange")
            if not table_path.exists():
                return None
            df = pl.read_delta(str(table_path))
            if df.is_empty():
                return None
            result = {}
            for row in df.iter_rows(named=True):
                result[row["exchange"]] = row
            _dim_exchange_cache = result
            _dim_exchange_cache_at = time.monotonic()
            return result
        except Exception:
            return None


def invalidate_exchange_cache() -> None:
    """Force next call to re-read dim_exchange from disk."""
    global _dim_exchange_cache, _dim_exchange_cache_at
    with _dim_exchange_cache_lock:
        _dim_exchange_cache = None
        _dim_exchange_cache_at = 0.0


def normalize_exchange(exchange: str) -> str:
    """
    Normalize exchange name for consistent lookup.

    Normalizes by lowercasing and trimming whitespace.
    This is the canonical normalization used before exchange lookup.

    Args:
        exchange: Raw exchange name (may have mixed case, whitespace)

    Returns:
        Normalized exchange string (lowercase, trimmed)
    """
    return exchange.lower().strip()


def get_exchange_id(exchange: str) -> int:
    """Get exchange_id for a given exchange name.

    Args:
        exchange: Exchange name (will be normalized before lookup)

    Returns:
        Exchange ID (Int16 compatible)

    Raises:
        ValueError: If exchange is not found
    """
    normalized = normalize_exchange(exchange)
    dim_ex = _ensure_dim_exchange()
    if normalized in dim_ex:
        return dim_ex[normalized]["exchange_id"]

    raise ValueError(exchange_not_found_error(exchange, list(dim_ex.keys())))


def get_exchange_name(exchange_id: int) -> str:
    """Get normalized exchange name for a given exchange_id.

    Args:
        exchange_id: Exchange ID to look up

    Returns:
        Normalized exchange name (e.g., "binance-futures")

    Raises:
        ValueError: If exchange_id is not found
    """
    dim_ex = _ensure_dim_exchange()
    for name, row in dim_ex.items():
        if row["exchange_id"] == exchange_id:
            return normalize_exchange(name)

    available_ids = sorted(row["exchange_id"] for row in dim_ex.values())
    raise ValueError(f"Exchange ID {exchange_id} not found. Available IDs: {available_ids}")


# Exchange Timezone Registry (seed data — canonical source is dim_exchange table)
# Maps exchange names to their timezone for exchange-local date partitioning
# Rationale: Partition date represents the trading day in exchange-local time,
# ensuring "one trading day = one partition" for efficient queries
_SEED_EXCHANGE_TIMEZONES = {
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

    Args:
        exchange: Exchange name (will be normalized before lookup)
        strict: If True, raise ValueError when exchange not in registry.
               If False, log warning and return "UTC" default.

    Returns:
        IANA timezone string (e.g., "UTC", "Asia/Shanghai")

    Raises:
        ValueError: If strict=True and exchange not found

    Examples:
        >>> get_exchange_timezone("binance-futures")
        'UTC'
        >>> get_exchange_timezone("szse")
        'Asia/Shanghai'
    """
    normalized = normalize_exchange(exchange)
    dim_ex = _ensure_dim_exchange()

    if normalized in dim_ex:
        return dim_ex[normalized].get("timezone", "UTC")

    if strict:
        raise ValueError(
            f"Exchange '{exchange}' (normalized: '{normalized}') not found in "
            f"dim_exchange registry. This could cause incorrect date partitioning. "
            f"Run `pointline exchange init` or add the exchange to dim_exchange with "
            f"its correct IANA timezone (e.g., 'UTC', 'Asia/Shanghai', 'America/New_York'). "
            f"Available exchanges: {sorted(dim_ex.keys())}"
        )
    else:
        warnings.warn(
            f"Exchange '{exchange}' (normalized: '{normalized}') not found in "
            f"dim_exchange registry. Falling back to UTC default. "
            f"This may cause incorrect date partitioning for regional exchanges. "
            f"Run `pointline exchange init` to bootstrap.",
            UserWarning,
            stacklevel=2,
        )
        return "UTC"


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

# Reverse mapping: asset_type integer -> human-readable string
ASSET_TYPE_NAMES = {
    0: "spot",
    1: "perpetual",
    2: "future",
    3: "option",
    10: "l3_orders",
    11: "l3_ticks",
}


def get_asset_type_name(asset_type: int) -> str:
    """Get human-readable name for asset_type integer.

    Args:
        asset_type: Asset type code (e.g., 0, 1, 2, 3, 10, 11)

    Returns:
        Human-readable name (e.g., "spot", "perpetual", "future")

    Examples:
        >>> get_asset_type_name(0)
        'spot'
        >>> get_asset_type_name(1)
        'perpetual'
    """
    return ASSET_TYPE_NAMES.get(asset_type, f"unknown_{asset_type}")


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


# Asset Class Taxonomy (canonical source: pointline.tables.asset_class)
# Re-exported here for backward compatibility.
from pointline.tables.asset_class import ASSET_CLASS_TAXONOMY as ASSET_CLASS_TAXONOMY  # noqa: E402

# Exchange Metadata Registry (seed data — canonical source is dim_exchange table)
# Extended metadata for data discovery and UI presentation
# Maps exchange name → metadata dict
_SEED_EXCHANGE_METADATA = {
    # Crypto Spot Exchanges
    "binance": {
        "exchange_id": 1,
        "asset_class": "crypto-spot",
        "description": "Binance Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes", "book_snapshot_25", "kline_1h"],
    },
    "coinbase": {
        "exchange_id": 3,
        "asset_class": "crypto-spot",
        "description": "Coinbase Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "kraken": {
        "exchange_id": 4,
        "asset_class": "crypto-spot",
        "description": "Kraken Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "okx": {
        "exchange_id": 5,
        "asset_class": "crypto-spot",
        "description": "OKX Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "huobi": {
        "exchange_id": 6,
        "asset_class": "crypto-spot",
        "description": "Huobi Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "gate": {
        "exchange_id": 7,
        "asset_class": "crypto-spot",
        "description": "Gate.io Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "bitfinex": {
        "exchange_id": 8,
        "asset_class": "crypto-spot",
        "description": "Bitfinex Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "bitstamp": {
        "exchange_id": 9,
        "asset_class": "crypto-spot",
        "description": "Bitstamp Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "gemini": {
        "exchange_id": 10,
        "asset_class": "crypto-spot",
        "description": "Gemini Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "crypto-com": {
        "exchange_id": 11,
        "asset_class": "crypto-spot",
        "description": "Crypto.com Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "kucoin": {
        "exchange_id": 12,
        "asset_class": "crypto-spot",
        "description": "KuCoin Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "binance-us": {
        "exchange_id": 13,
        "asset_class": "crypto-spot",
        "description": "Binance US Spot Trading",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "coinbase-pro": {
        "exchange_id": 14,
        "asset_class": "crypto-spot",
        "description": "Coinbase Pro (Legacy)",
        "is_active": False,
        "supported_tables": ["trades", "quotes"],
    },
    # Crypto Derivatives Exchanges
    "binance-futures": {
        "exchange_id": 2,
        "asset_class": "crypto-derivatives",
        "description": "Binance USDT-Margined Perpetual Futures",
        "is_active": True,
        "supported_tables": [
            "trades",
            "quotes",
            "book_snapshot_25",
            "derivative_ticker",
            "liquidations",
            "options_chain",
            "kline_1h",
        ],
    },
    "binance-coin-futures": {
        "exchange_id": 20,
        "asset_class": "crypto-derivatives",
        "description": "Binance COIN-Margined Futures",
        "is_active": True,
        "supported_tables": [
            "trades",
            "quotes",
            "derivative_ticker",
            "liquidations",
            "options_chain",
        ],
    },
    "deribit": {
        "exchange_id": 21,
        "asset_class": "crypto-derivatives",
        "description": "Deribit BTC/ETH Options and Futures",
        "is_active": True,
        "supported_tables": [
            "trades",
            "quotes",
            "derivative_ticker",
            "liquidations",
            "options_chain",
        ],
    },
    "bybit": {
        "exchange_id": 22,
        "asset_class": "crypto-derivatives",
        "description": "Bybit Derivatives",
        "is_active": True,
        "supported_tables": [
            "trades",
            "quotes",
            "derivative_ticker",
            "liquidations",
            "options_chain",
        ],
    },
    "okx-futures": {
        "exchange_id": 23,
        "asset_class": "crypto-derivatives",
        "description": "OKX Futures and Perpetuals",
        "is_active": True,
        "supported_tables": [
            "trades",
            "quotes",
            "derivative_ticker",
            "liquidations",
            "options_chain",
        ],
    },
    "bitmex": {
        "exchange_id": 24,
        "asset_class": "crypto-derivatives",
        "description": "BitMEX Perpetual Swaps",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    "ftx": {
        "exchange_id": 25,
        "asset_class": "crypto-derivatives",
        "description": "FTX (Historical data only - exchange defunct)",
        "is_active": False,
        "supported_tables": ["trades", "quotes"],
    },
    "dydx": {
        "exchange_id": 26,
        "asset_class": "crypto-derivatives",
        "description": "dYdX Perpetual Swaps",
        "is_active": True,
        "supported_tables": ["trades", "quotes"],
    },
    # Chinese Stock Exchanges
    "szse": {
        "exchange_id": 30,
        "asset_class": "stocks-cn",
        "description": "Shenzhen Stock Exchange (Level 3 Order Book)",
        "is_active": True,
        "supported_tables": ["l3_orders", "l3_ticks"],
    },
    "sse": {
        "exchange_id": 31,
        "asset_class": "stocks-cn",
        "description": "Shanghai Stock Exchange (Level 3 Order Book)",
        "is_active": True,
        "supported_tables": ["l3_orders", "l3_ticks"],
    },
}

# Supported tables per exchange — data coverage hint, NOT exchange metadata.
# Extracted from _SEED_EXCHANGE_METADATA to keep dim_exchange lean.
_EXCHANGE_SUPPORTED_TABLES: dict[str, list[str]] = {
    name: meta["supported_tables"]
    for name, meta in _SEED_EXCHANGE_METADATA.items()
    if "supported_tables" in meta
}


def get_exchange_supported_tables(exchange: str) -> list[str] | None:
    """Get list of tables supported by an exchange (data coverage hint).

    Args:
        exchange: Exchange name (will be normalized before lookup)

    Returns:
        List of supported table names, or None if not found
    """
    return _EXCHANGE_SUPPORTED_TABLES.get(normalize_exchange(exchange))


# ---------------------------------------------------------------------------
# Auto-bootstrap dim_exchange from seed data on first access
# ---------------------------------------------------------------------------
_dim_exchange_bootstrap_lock = threading.Lock()
_dim_exchange_bootstrapped = False


def _ensure_dim_exchange() -> dict[str, dict]:
    """Return dim_exchange dict, auto-bootstrapping from seed data if needed.

    Thread-safe. If the dim_exchange table does not exist on disk, it is
    created from _SEED_* dicts and written to silver/dim_exchange/.
    """
    global _dim_exchange_bootstrapped

    cached = _load_dim_exchange()
    if cached is not None:
        return cached

    if _dim_exchange_bootstrapped:
        # Already tried once; avoid infinite retry. Fall back to seed data.
        return _seed_as_dim_exchange_dict()

    with _dim_exchange_bootstrap_lock:
        # Double-check after acquiring lock
        cached = _load_dim_exchange()
        if cached is not None:
            return cached

        if _dim_exchange_bootstrapped:
            return _seed_as_dim_exchange_dict()

        try:
            from pointline.tables.dim_exchange import bootstrap_from_config

            df = bootstrap_from_config()
            table_path = get_table_path("dim_exchange")
            table_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_delta(str(table_path), mode="overwrite")
            logger.info("Auto-bootstrapped dim_exchange at %s (%d rows)", table_path, len(df))
            _dim_exchange_bootstrapped = True
            invalidate_exchange_cache()
            result = _load_dim_exchange()
            if result is not None:
                return result
        except Exception as exc:
            logger.debug("dim_exchange auto-bootstrap failed: %s", exc)
            _dim_exchange_bootstrapped = True

        return _seed_as_dim_exchange_dict()


def _seed_as_dim_exchange_dict() -> dict[str, dict]:
    """Build a dim_exchange-shaped dict from seed data (last resort fallback)."""
    result: dict[str, dict] = {}
    for name, eid in _SEED_EXCHANGE_MAP.items():
        meta = _SEED_EXCHANGE_METADATA.get(name, {})
        result[name] = {
            "exchange": name,
            "exchange_id": eid,
            "asset_class": meta.get("asset_class", "unknown"),
            "timezone": _SEED_EXCHANGE_TIMEZONES.get(name, "UTC"),
            "description": meta.get("description", ""),
            "is_active": meta.get("is_active", True),
        }
    return result


# ---------------------------------------------------------------------------
# Public get_*() functions — read from dim_exchange (no fallback paths)
# ---------------------------------------------------------------------------


def get_exchange_metadata(exchange: str) -> dict | None:
    """Get metadata for a given exchange.

    Args:
        exchange: Exchange name (will be normalized before lookup)

    Returns:
        Metadata dict or None if not found

    Examples:
        >>> meta = get_exchange_metadata("binance-futures")
        >>> meta["asset_class"]
        'crypto-derivatives'
    """
    normalized = normalize_exchange(exchange)
    dim_ex = _ensure_dim_exchange()
    if normalized in dim_ex:
        row = dim_ex[normalized]
        return {
            "exchange_id": row["exchange_id"],
            "asset_class": row.get("asset_class", "unknown"),
            "description": row.get("description", ""),
            "is_active": row.get("is_active", True),
        }
    return None


def get_asset_class_exchanges(asset_class: str) -> list[str]:
    """Get all exchanges belonging to an asset class.

    Args:
        asset_class: Asset class name (e.g., "crypto", "crypto-spot", "stocks-cn")

    Returns:
        List of exchange names

    Examples:
        >>> get_asset_class_exchanges("crypto-spot")
        ['binance', 'coinbase', 'kraken', ...]
        >>> get_asset_class_exchanges("crypto")  # includes spot + derivatives
        ['binance', 'coinbase', ..., 'binance-futures', 'deribit', ...]
    """
    dim_ex = _ensure_dim_exchange()
    result = []
    for name, row in dim_ex.items():
        row_class = row.get("asset_class", "")
        if row_class == asset_class or row_class.startswith(asset_class + "-"):
            result.append(name)
    return result


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
