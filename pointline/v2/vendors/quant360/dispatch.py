"""Parser dispatch mapping for Quant360 stream data types."""

from __future__ import annotations

from collections.abc import Callable

import polars as pl

from pointline.v2.vendors.quant360.parsers import (
    parse_l2_snapshot_stream,
    parse_order_stream,
    parse_tick_stream,
)

Quant360Parser = Callable[[pl.DataFrame, str, str], pl.DataFrame]

_PARSER_BY_DATA_TYPE: dict[str, Quant360Parser] = {
    "cn_order_events": parse_order_stream,
    "order_new": parse_order_stream,
    "l3_orders": parse_order_stream,
    "cn_tick_events": parse_tick_stream,
    "tick_new": parse_tick_stream,
    "l3_ticks": parse_tick_stream,
    "cn_l2_snapshots": parse_l2_snapshot_stream,
    "L2_new": parse_l2_snapshot_stream,
    "l2_new": parse_l2_snapshot_stream,
}


def get_quant360_stream_parser(data_type: str) -> Quant360Parser:
    try:
        return _PARSER_BY_DATA_TYPE[data_type]
    except KeyError as exc:
        supported = ", ".join(sorted(_PARSER_BY_DATA_TYPE))
        raise ValueError(
            f"Unsupported Quant360 data_type '{data_type}'. Supported: {supported}"
        ) from exc
