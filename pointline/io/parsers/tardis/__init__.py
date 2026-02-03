"""Tardis vendor parsers.

This package contains parsers for Tardis data files.
All parsers are automatically registered via the @register_parser decorator.
"""

# Import all parsers to trigger registration
from pointline.io.parsers.tardis.book_snapshots import parse_tardis_book_snapshots_csv
from pointline.io.parsers.tardis.derivative_ticker import parse_tardis_derivative_ticker_csv
from pointline.io.parsers.tardis.quotes import parse_tardis_quotes_csv
from pointline.io.parsers.tardis.trades import parse_tardis_trades_csv

__all__ = [
    "parse_tardis_trades_csv",
    "parse_tardis_quotes_csv",
    "parse_tardis_book_snapshots_csv",
    "parse_tardis_derivative_ticker_csv",
]
