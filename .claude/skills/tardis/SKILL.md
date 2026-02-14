---
name: tardis
description: >-
  Tardis.dev cryptocurrency market data CSV format reference. Use when:
  (1) downloading or parsing Tardis CSV datasets (trades, order book, quotes,
  derivative_ticker, liquidations, options_chain), (2) looking up Tardis CSV
  schemas, field definitions, or grouped symbol conventions, (3) reconstructing
  order books from incremental_book_L2 data, (4) working with crypto exchange
  data from Binance, Deribit, BitMEX, Bybit, OKEx, or other supported exchanges,
  (5) any task involving tardis-dev Python SDK or Tardis datasets API.
---

# Tardis.dev — Crypto Market Data CSV Datasets

Tardis.dev provides downloadable tick-level CSV datasets for 26+ cryptocurrency exchanges, covering spot, futures, and options markets. All files are gzip-compressed CSV with microsecond UTC timestamps.

## Access

```python
# pip install tardis-dev
from tardis_dev import datasets

datasets.download(
    exchange="deribit",
    data_types=["trades", "quotes", "derivative_ticker"],
    from_date="2024-01-01",
    to_date="2024-01-02",
    symbols=["BTC-PERPETUAL"],
    api_key="YOUR_API_KEY"  # Optional for first-of-month samples
)
```

```bash
# HTTP direct download
curl -o trades.csv.gz \
  https://datasets.tardis.dev/v1/deribit/trades/2024/01/01/BTC-PERPETUAL.csv.gz
```

Free samples: first day of each month, no API key required.

## File Format

```
<exchange>_<data_type>_<YYYY-MM-DD>_<symbol>.csv.gz
```

- Gzip-compressed CSV, comma-delimited
- All timestamps: **microseconds since epoch (UTC)**
- Symbols: uppercase, exchange-native (replace `/` `:` with `-` in URLs)

## Common Fields (All Data Types)

| Field | Type | Description |
|---|---|---|
| `exchange` | String | Exchange name (lowercase) |
| `symbol` | String | Instrument symbol (uppercase) |
| `timestamp` | Integer | Exchange timestamp (µs UTC) |
| `local_timestamp` | Integer | Tardis collection server timestamp (µs UTC) |

## Data Types Quick Reference

| Data Type | Description | Key Fields |
|---|---|---|
| `trades` | Individual trades | `id`, `side` (buy/sell/unknown), `price`, `amount` |
| `quotes` | Best bid/ask (top-of-book) | `bid_price`, `bid_amount`, `ask_price`, `ask_amount` |
| `incremental_book_L2` | Tick-level order book updates | `is_snapshot`, `side`, `price`, `amount` (absolute) |
| `book_snapshot_25` | Top 25 levels per side | `bids[0..24].price/amount`, `asks[0..24].price/amount` |
| `book_snapshot_5` | Top 5 levels per side | `bids[0..4].price/amount`, `asks[0..4].price/amount` |
| `derivative_ticker` | Funding, mark/index price, OI | `mark_price`, `funding_rate`, `open_interest` |
| `liquidations` | Forced liquidation events | `side` (buy=short liq, sell=long liq), `price`, `amount` |
| `options_chain` | Options greeks, IV, strikes | `type`, `strike`, `expiration`, `delta/gamma/vega/theta` |

## Grouped Symbols

Download all instruments of a market type in one file:

| Grouped Symbol | Description |
|---|---|
| `SPOT` | All spot instruments |
| `FUTURES` | All futures contracts |
| `PERPETUALS` | All perpetual swaps |
| `OPTIONS` | All options |

CSV rows still contain individual `symbol` values — the grouped name is only for download.

## Order Book Reconstruction (incremental_book_L2)

- `amount` is **absolute** (not delta) — `0` means remove the level
- When `is_snapshot` transitions `false` → `true`, discard existing book state
- Group by `local_timestamp` to identify atomic multi-level updates

## Trade Side Semantics

| Data Type | `side` value | Meaning |
|---|---|---|
| `trades` | `buy` | Taker bought (aggressive buyer hit ask) |
| `trades` | `sell` | Taker sold (aggressive seller hit bid) |
| `trades` | `unknown` | Exchange did not provide direction |
| `liquidations` | `buy` | Short position liquidated (forced buy) |
| `liquidations` | `sell` | Long position liquidated (forced sell) |

## Timestamp Semantics

| Field | Source | Use |
|---|---|---|
| `timestamp` | Exchange-provided | Preferred for event time |
| `local_timestamp` | Tardis server | Fallback; includes network latency (<100ms typical) |

All timestamps are **UTC microseconds since Unix epoch**.

## Key Gotchas

1. **`amount` in book data is absolute**, not a delta. Zero = level removed.
2. **Trade `id` format varies** by exchange (numeric, UUID, or empty).
3. **`open_interest` units vary** — some exchanges use contracts, others base asset notional.
4. **Options greeks** may be empty for illiquid options or unsupported exchanges.
5. **First message of day** may be a partial snapshot for order book data.
6. **Symbol URL encoding** — replace `/` and `:` with `-` (e.g., `BTC/USD` → `BTC-USD`).

## Full Data Format Reference

For complete field definitions per data type, exchange coverage table, order book reconstruction code, and data quality notes, read [references/tardis_csv.md](references/tardis_csv.md).
