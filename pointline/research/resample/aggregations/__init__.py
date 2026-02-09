"""Custom aggregations for research workflows.

This module provides domain-specific aggregations:
- Microstructure: microprice, spread distribution, OFI
- Trade flow: signed trade imbalance
- Derivatives: funding rate, open interest
- Crypto MFT: flow imbalance, spread BPS, book imbalance, realized volatility, etc.

All aggregations are auto-registered on import.
"""

# Import to trigger registration
from . import crypto_mft, derivatives, microstructure, trade_flow

__all__ = ["microstructure", "trade_flow", "derivatives", "crypto_mft"]
