from __future__ import annotations

import polars as pl
import pytest

from pointline.research.primitives import decode_scaled_columns
from pointline.schemas.types import PRICE_SCALE, QTY_SCALE


def test_decode_scaled_columns_precision_contract() -> None:
    frame = pl.DataFrame(
        {
            "price": [1, 500_000_000, 1_250_000_000, 123_456_789_000],
            "qty": [10_000_000_000, 250_000_000, 1, 9_999_999_999],
        }
    )

    out = decode_scaled_columns(frame, "trades")

    assert out.columns == ["price", "qty", "price_decoded", "qty_decoded"]
    assert out["price"].to_list() == frame["price"].to_list()
    assert out["qty"].to_list() == frame["qty"].to_list()

    expected_price = [value / PRICE_SCALE for value in frame["price"].to_list()]
    expected_qty = [value / QTY_SCALE for value in frame["qty"].to_list()]

    assert out["price_decoded"].to_list() == pytest.approx(expected_price, abs=1e-12)
    assert out["qty_decoded"].to_list() == pytest.approx(expected_qty, abs=1e-12)


def test_decode_scaled_columns_preserves_nulls() -> None:
    frame = pl.DataFrame(
        {
            "price": [100_000_000_000, None],
            "qty": [None, 5_000_000_000],
        },
        schema={"price": pl.Int64, "qty": pl.Int64},
    )

    out = decode_scaled_columns(frame, "trades")

    assert out["price_decoded"].to_list()[1] is None
    assert out["qty_decoded"].to_list()[0] is None
    assert out["price_decoded"].to_list()[0] == pytest.approx(100.0, abs=1e-12)
    assert out["qty_decoded"].to_list()[1] == pytest.approx(5.0, abs=1e-12)
