from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pointline.vendors.quant360 import parse_quant360_timestamp


def test_parse_quant360_timestamp_to_utc_microseconds() -> None:
    ts_us = parse_quant360_timestamp("20240102093000123")
    expected = int(datetime(2024, 1, 2, 1, 30, 0, 123000, tzinfo=timezone.utc).timestamp() * 1e6)
    assert ts_us == expected


def test_parse_quant360_timestamp_accepts_integer_input() -> None:
    ts_us = parse_quant360_timestamp(20240102093000123)
    expected = int(datetime(2024, 1, 2, 1, 30, 0, 123000, tzinfo=timezone.utc).timestamp() * 1e6)
    assert ts_us == expected


def test_parse_quant360_timestamp_rejects_invalid_shape() -> None:
    with pytest.raises(ValueError, match="YYYYMMDDHHMMSSmmm"):
        parse_quant360_timestamp("20240102093000")
