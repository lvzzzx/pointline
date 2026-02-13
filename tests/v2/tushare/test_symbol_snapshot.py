"""Tests for Tushare stock_basic → v2 dim_symbol mapping."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from pointline.v2.dim_symbol import bootstrap, upsert, validate
from pointline.v2.vendors.tushare.symbols import (
    CN_LOT_SIZE,
    CN_TICK_SIZE,
    stock_basic_to_delistings,
    stock_basic_to_snapshot,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TUSHARE_COLUMNS = [
    "ts_code",
    "symbol",
    "name",
    "exchange",
    "market",
    "list_status",
    "list_date",
    "delist_date",
]


def _raw_tushare(rows: list[dict]) -> pl.DataFrame:
    """Build a synthetic Tushare stock_basic DataFrame."""
    # Ensure all columns exist even if not in every row
    full_rows = [{col: row.get(col) for col in _TUSHARE_COLUMNS} for row in rows]
    return pl.DataFrame(full_rows)


def _ts_us(d: date) -> int:
    """Convert a date to UTC midnight microseconds."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1_000_000)


# ---------------------------------------------------------------------------
# stock_basic_to_snapshot
# ---------------------------------------------------------------------------


class TestSnapshot:
    def test_snapshot_maps_fields_correctly(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": None,
                }
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        assert snap.height == 1
        row = snap.row(0, named=True)
        assert row["exchange"] == "szse"
        assert row["exchange_symbol"] == "000001"
        assert row["canonical_symbol"] == "000001.SZ"
        assert row["market_type"] == "主板"
        assert row["base_asset"] == "平安银行"
        assert row["quote_asset"] == "CNY"
        assert row["tick_size"] == CN_TICK_SIZE
        assert row["lot_size"] == CN_LOT_SIZE
        assert row["contract_size"] is None

    def test_snapshot_filters_listed_and_paused(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "B",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "P",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000003.SZ",
                    "symbol": "000003",
                    "name": "C",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "D",
                    "list_date": "20200101",
                    "delist_date": "20230101",
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        assert snap.height == 2
        symbols = snap["exchange_symbol"].to_list()
        assert "000001" in symbols
        assert "000002" in symbols
        assert "000003" not in symbols

    def test_snapshot_normalizes_exchange(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "600000.SH",
                    "symbol": "600000",
                    "name": "B",
                    "exchange": "SSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        exchanges = snap["exchange"].to_list()
        assert "szse" in exchanges
        assert "sse" in exchanges

    def test_snapshot_filters_unknown_exchange(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "899001.BJ",
                    "symbol": "899001",
                    "name": "X",
                    "exchange": "BSE",
                    "market": "北交所",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        assert snap.height == 0

    def test_snapshot_coalesces_base_asset(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": None,
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        assert snap.row(0, named=True)["base_asset"] == "000001"

    def test_snapshot_schema_compatible_with_bootstrap(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        dim = bootstrap(snap, effective_ts_us=1_000_000)
        validate(dim)
        assert dim.height == 1

    def test_snapshot_multi_exchange(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "600000.SH",
                    "symbol": "600000",
                    "name": "B",
                    "exchange": "SSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "C",
                    "exchange": "SZSE",
                    "market": "创业板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        assert snap.height == 3
        assert snap.filter(pl.col("exchange") == "szse").height == 2
        assert snap.filter(pl.col("exchange") == "sse").height == 1


# ---------------------------------------------------------------------------
# stock_basic_to_delistings
# ---------------------------------------------------------------------------


class TestDelistings:
    def test_delistings_extracts_delisted_only(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "B",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "D",
                    "list_date": "20100101",
                    "delist_date": "20230615",
                },
            ]
        )
        dl = stock_basic_to_delistings(raw)
        assert dl.height == 1
        assert dl.row(0, named=True)["exchange_symbol"] == "000002"

    def test_delistings_parses_delist_date(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000099.SZ",
                    "symbol": "000099",
                    "name": "X",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "D",
                    "list_date": "20100101",
                    "delist_date": "20230615",
                },
            ]
        )
        dl = stock_basic_to_delistings(raw)
        expected = _ts_us(date(2023, 6, 15))
        assert dl.row(0, named=True)["delisted_at_ts_us"] == expected

    def test_delistings_skips_null_delist_date(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000099.SZ",
                    "symbol": "000099",
                    "name": "X",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "D",
                    "list_date": "20100101",
                    "delist_date": None,
                },
            ]
        )
        dl = stock_basic_to_delistings(raw)
        assert dl.height == 0

    def test_delistings_schema_compatible_with_upsert(self):
        raw = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "B",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "D",
                    "list_date": "20100101",
                    "delist_date": "20230615",
                },
            ]
        )
        snap = stock_basic_to_snapshot(raw)
        dl = stock_basic_to_delistings(raw)

        # bootstrap with the active stock, then upsert with delisting
        dim = bootstrap(snap, effective_ts_us=1_000_000)

        # The delisted stock wasn't in the initial bootstrap, so add it first
        snap_with_delisted = stock_basic_to_snapshot(
            _raw_tushare(
                [
                    {
                        "ts_code": "000001.SZ",
                        "symbol": "000001",
                        "name": "A",
                        "exchange": "SZSE",
                        "market": "主板",
                        "list_status": "L",
                        "list_date": "20200101",
                        "delist_date": None,
                    },
                    {
                        "ts_code": "000002.SZ",
                        "symbol": "000002",
                        "name": "B",
                        "exchange": "SZSE",
                        "market": "主板",
                        "list_status": "L",
                        "list_date": "20100101",
                        "delist_date": None,
                    },
                ]
            )
        )
        dim = upsert(dim, snap_with_delisted, effective_ts_us=2_000_000, delistings=dl.clear())
        # Now upsert removing 000002 via delistings
        snap_without = stock_basic_to_snapshot(
            _raw_tushare(
                [
                    {
                        "ts_code": "000001.SZ",
                        "symbol": "000001",
                        "name": "A",
                        "exchange": "SZSE",
                        "market": "主板",
                        "list_status": "L",
                        "list_date": "20200101",
                        "delist_date": None,
                    },
                ]
            )
        )
        dim = upsert(dim, snap_without, effective_ts_us=3_000_000, delistings=dl)
        validate(dim)
        # 000002 should have a closed row
        closed = dim.filter((pl.col("exchange_symbol") == "000002") & (~pl.col("is_current")))
        assert closed.height >= 1


# ---------------------------------------------------------------------------
# Integration (round-trip with dim_symbol)
# ---------------------------------------------------------------------------


class TestIntegration:
    def test_bootstrap_then_upsert_with_delist(self):
        """Bootstrap from snapshot, then delist a stock."""
        raw_t0 = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "B",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        dim = bootstrap(stock_basic_to_snapshot(raw_t0), effective_ts_us=1_000_000)
        validate(dim)
        assert dim.height == 2

        # t1: 000002 gets delisted
        raw_t1 = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "B",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "D",
                    "list_date": "20200101",
                    "delist_date": "20240101",
                },
            ]
        )
        snap_t1 = stock_basic_to_snapshot(raw_t1)
        dl_t1 = stock_basic_to_delistings(raw_t1)
        dim = upsert(dim, snap_t1, effective_ts_us=2_000_000, delistings=dl_t1)
        validate(dim)

        # 000001 still current, 000002 closed
        assert dim.filter(pl.col("is_current")).height == 1
        assert dim.filter(pl.col("is_current"))["exchange_symbol"][0] == "000001"
        closed = dim.filter((pl.col("exchange_symbol") == "000002") & (~pl.col("is_current")))
        assert closed.height == 1
        assert closed["valid_until_ts_us"][0] == _ts_us(date(2024, 1, 1))

    def test_upsert_new_ipo(self):
        """A stock not in dim appears in the next snapshot → new listing."""
        raw_t0 = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
            ]
        )
        dim = bootstrap(stock_basic_to_snapshot(raw_t0), effective_ts_us=1_000_000)

        # t1: new IPO appears
        raw_t1 = _raw_tushare(
            [
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "A",
                    "exchange": "SZSE",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "20200101",
                    "delist_date": None,
                },
                {
                    "ts_code": "301999.SZ",
                    "symbol": "301999",
                    "name": "新股IPO",
                    "exchange": "SZSE",
                    "market": "创业板",
                    "list_status": "L",
                    "list_date": "20240601",
                    "delist_date": None,
                },
            ]
        )
        snap_t1 = stock_basic_to_snapshot(raw_t1)
        dl_t1 = stock_basic_to_delistings(raw_t1)
        dim = upsert(dim, snap_t1, effective_ts_us=2_000_000, delistings=dl_t1)
        validate(dim)

        assert dim.filter(pl.col("is_current")).height == 2
        new = dim.filter(pl.col("exchange_symbol") == "301999")
        assert new.height == 1
        assert new["market_type"][0] == "创业板"
        assert new["is_current"][0] is True
