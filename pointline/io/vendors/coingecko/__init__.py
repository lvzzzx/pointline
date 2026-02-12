"""CoinGecko vendor plugin package.

This package provides CoinGecko API client integration for market data.
"""

# Import plugin class
# Import client
from pointline.io.vendors.coingecko.client import CoinGeckoClient
from pointline.io.vendors.coingecko.plugin import CoingeckoVendor

# Register plugin
from pointline.io.vendors.registry import register_vendor

# Asset to CoinGecko Mapping
# Maps base_asset (from dim_symbol) to CoinGecko coin_id.
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
    "CCUSDT": "cetus-protocol",  # May need adjustment based on actual CoinGecko ID.
}


def get_coingecko_coin_id(base_asset: str) -> str | None:
    """Get CoinGecko coin_id for a given base_asset ticker."""
    return ASSET_TO_COINGECKO_MAP.get(base_asset.upper())


register_vendor(CoingeckoVendor())

__all__ = [
    "ASSET_TO_COINGECKO_MAP",
    # Plugin
    "CoingeckoVendor",
    # Client
    "CoinGeckoClient",
    "get_coingecko_coin_id",
]
