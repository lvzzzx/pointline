"""Tests for canonical table-domain registry."""

from __future__ import annotations

import polars as pl

from pointline.tables.domain_registry import get_domain, list_domains

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
    domains = set(list_domains())
    assert EVENT_TABLES.issubset(domains)


def test_trades_domain_canonicalizes_side_raw() -> None:
    domain = get_domain("trades")
    raw = pl.DataFrame({"side_raw": ["buy", "sell", "unknown"]})
    canonical = domain.canonicalize_vendor_frame(raw)
    assert canonical["side"].to_list() == [0, 1, 2]


def test_l3_domains_require_allowed_exchanges() -> None:
    orders = get_domain("l3_orders")
    ticks = get_domain("l3_ticks")
    assert orders.spec.allowed_exchanges == frozenset({"szse", "sse"})
    assert ticks.spec.allowed_exchanges == frozenset({"szse", "sse"})
