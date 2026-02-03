"""Tardis vendor plugin.

This plugin provides parsers and client for Tardis.dev historical crypto market data.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import polars as pl


class TardisVendor:
    """Tardis.dev vendor plugin.

    Provides parsers for:
    - trades
    - quotes
    - book_snapshot_25
    - derivative_ticker
    """

    name = "tardis"
    display_name = "Tardis.dev"
    supports_parsers = True
    supports_download = True
    supports_prehooks = False

    def get_parsers(self) -> dict[str, Callable[[pl.DataFrame], pl.DataFrame]]:
        """Get all Tardis parsers.

        Returns:
            Dictionary mapping data_type to parser function
        """
        # Import parsers locally to avoid circular imports
        from pointline.io.vendors.tardis.parsers import (
            parse_tardis_book_snapshots_csv,
            parse_tardis_derivative_ticker_csv,
            parse_tardis_quotes_csv,
            parse_tardis_trades_csv,
        )

        return {
            "trades": parse_tardis_trades_csv,
            "quotes": parse_tardis_quotes_csv,
            "book_snapshot_25": parse_tardis_book_snapshots_csv,
            "derivative_ticker": parse_tardis_derivative_ticker_csv,
        }

    def get_download_client(self) -> Any:
        """Get Tardis API client.

        Returns:
            TardisClient instance
        """
        from pointline.io.vendors.tardis.client import TardisClient

        return TardisClient()

    def run_prehook(self, bronze_root: Path) -> None:
        """Tardis doesn't need prehooks."""
        raise NotImplementedError("Tardis vendor doesn't support prehooks")
