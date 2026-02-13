"""Parser dispatch mapping for Tardis stream data types."""

from __future__ import annotations

from collections.abc import Callable

import polars as pl

from pointline.vendors.tardis.parsers import (
    parse_tardis_incremental_l2,
    parse_tardis_quotes,
    parse_tardis_trades,
)

TardisParser = Callable[[pl.DataFrame, str, str], pl.DataFrame]

_PARSER_BY_DATA_TYPE: dict[str, TardisParser] = {
    "trades": parse_tardis_trades,
    "quotes": parse_tardis_quotes,
    "incremental_book_L2": parse_tardis_incremental_l2,
    "incremental_book_l2": parse_tardis_incremental_l2,
    "orderbook_updates": parse_tardis_incremental_l2,
}


def get_tardis_parser(data_type: str) -> TardisParser:
    try:
        return _PARSER_BY_DATA_TYPE[data_type]
    except KeyError as exc:
        supported = ", ".join(sorted(_PARSER_BY_DATA_TYPE))
        raise ValueError(
            f"Unsupported Tardis data_type '{data_type}'. Supported: {supported}"
        ) from exc
