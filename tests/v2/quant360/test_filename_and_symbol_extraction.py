from __future__ import annotations

from datetime import date

import pytest

from pointline.v2.vendors.quant360 import parse_archive_filename, parse_symbol_from_member_path


def test_parse_archive_filename_for_order_stream() -> None:
    meta = parse_archive_filename("order_new_STK_SZ_20240102.7z")
    assert meta.stream_type == "order_new"
    assert meta.exchange == "szse"
    assert meta.market == "STK"
    assert meta.trading_date == date(2024, 1, 2)
    assert meta.canonical_data_type == "cn_order_events"


def test_parse_archive_filename_for_l2_snapshot_stream() -> None:
    meta = parse_archive_filename("L2_new_STK_SZ_20240102.7z")
    assert meta.stream_type == "L2_new"
    assert meta.exchange == "szse"
    assert meta.canonical_data_type == "cn_l2_snapshots"


def test_parse_archive_filename_rejects_invalid_name() -> None:
    with pytest.raises(ValueError, match="Quant360 archive filename"):
        parse_archive_filename("orders_20240102.7z")


def test_parse_symbol_from_member_path() -> None:
    assert parse_symbol_from_member_path("order_new_STK_SZ_20240102/000001.csv") == "000001"


def test_parse_symbol_from_member_path_rejects_non_csv() -> None:
    with pytest.raises(ValueError, match="CSV"):
        parse_symbol_from_member_path("order_new_STK_SZ_20240102/000001.txt")
