from __future__ import annotations

import pytest

from pointline.v2.vendors.quant360 import get_quant360_stream_parser, parse_order_stream


def test_dispatch_maps_aliases_to_order_parser() -> None:
    assert get_quant360_stream_parser("order_new") is parse_order_stream
    assert get_quant360_stream_parser("l3_orders") is parse_order_stream
    assert get_quant360_stream_parser("cn_order_events") is parse_order_stream


def test_dispatch_rejects_unknown_data_type() -> None:
    with pytest.raises(ValueError, match="Unsupported Quant360 data_type"):
        get_quant360_stream_parser("unknown")
