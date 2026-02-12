"""Tests for canonical table-domain registry."""

from __future__ import annotations

import polars as pl

from pointline.tables.domain_registry import (
    get_dimension_domain,
    get_event_domain,
    list_dimension_domains,
    list_domains,
    list_event_domains,
)

EVENT_TABLES = {
    "trades",
    "quotes",
    "book_snapshot_25",
    "derivative_ticker",
    "liquidations",
    "options_chain",
    "kline_1h",
    "kline_1d",
    "l3_orders",
    "l3_ticks",
}


def test_event_domains_registered() -> None:
    domains = set(list_event_domains())
    assert domains == EVENT_TABLES
    assert "dim_symbol" not in domains


def test_trades_domain_canonicalizes_side_raw() -> None:
    domain = get_event_domain("trades")
    raw = pl.DataFrame({"side_raw": ["buy", "sell", "unknown"]})
    canonical = domain.canonicalize_vendor_frame(raw)
    assert canonical["side"].to_list() == [0, 1, 2]
    assert domain.spec.table_kind == "event"


def test_l3_domains_require_allowed_exchanges() -> None:
    orders = get_event_domain("l3_orders")
    ticks = get_event_domain("l3_ticks")
    assert orders.spec.allowed_exchanges == frozenset({"szse", "sse"})
    assert ticks.spec.allowed_exchanges == frozenset({"szse", "sse"})


def test_dim_symbol_registered_as_dimension_domain() -> None:
    all_domains = set(list_domains())
    dimension_domains = set(list_dimension_domains())

    assert "dim_symbol" in all_domains
    assert dimension_domains == {"dim_symbol"}

    dim_domain = get_dimension_domain("dim_symbol")
    assert dim_domain.spec.table_kind == "dimension"
    assert dim_domain.spec.ts_column == "valid_from_ts"
