---
name: quant360
description: >-
  Quant360 Chinese stock Level 2/3 data format and Chinese A-share trading rules
  reference. Use when: (1) parsing or writing parsers for Quant360 order/tick/L2
  CSV files, (2) working with SSE or SZSE exchange feed schemas and their differences,
  (3) determining aggressor side from order references, (4) understanding Chinese
  stock trading phases, auction mechanisms, or price limit rules, (5) working with
  CN L2/L3 market microstructure data.
---

# Quant360 — Chinese Stock Level 2/3 Data

Quant360 (data.quant360.com) provides Level 2/3 tick-by-tick data for Chinese A-shares: order streams, trade/tick streams, and L2 order book snapshots for SSE and SZSE.

## Data Format

- **Delivery:** `.7z` archives (LZMA2), one per type/exchange/date
- **Internal:** One CSV per symbol (e.g., `600000.csv`, `000001.csv`)
- **Encoding:** UTF-8, header row
- **Timestamps:** `YYYYMMDDHHMMSSmmm` format, China Standard Time (UTC+8)

### File Naming

```
<type>_<market>_<exchange>_<YYYYMMDD>.7z
```

| Component | Values |
|---|---|
| `type` | `order_new`, `tick_new`, `L2_new` |
| `market` | `STK` (stocks), `ConFI` (convertible bonds) |
| `exchange` | `SH` (SSE), `SZ` (SZSE) |

## SSE vs SZSE — Key Differences

| Feature | SSE (SH) | SZSE (SZ) |
|---|---|---|
| Symbol location | `SecurityID` column | Filename only |
| Order ID | `OrderNo` | `ApplSeqNum` |
| Side encoding | `B`/`S` (strings) | `1`/`2` (integers) |
| Order type | `A`=Add, `D`=Delete | `1`=Market, `2`=Limit |
| Cancel events | `OrdType='D'` in order stream | `ExecType='4'` in tick stream |
| Trade linkage | `BuyNo` + `SellNo` | `BidApplSeqNum` + `OfferApplSeqNum` |
| Trade direction | Explicit `TradeBSFlag` (B/S/N) | Inferred from order refs |
| L2 snapshots | Not available | Available (`L2_new_*`) |
| Closing auction | No explicit phase (14:57-15:00 is closed) | Dedicated 14:57-15:00 phase |

## Aggressor Side Determination

Compare order reference numbers — the later-arriving (higher sequence) order is the aggressor:

```
BuyOrderRef > SellOrderRef → BUYER is aggressor (主动买 / 外盘)
SellOrderRef > BuyOrderRef → SELLER is aggressor (主动卖 / 内盘)
```

- SSE provides `TradeBSFlag` directly but can be cross-validated with order refs
- SZSE has no native flag — must infer from `BidApplSeqNum` vs `OfferApplSeqNum`
- Auction trades (`TradeBSFlag='N'` or equal refs) → `UNKNOWN`

## Chinese A-Share Trading Rules

### Trading Sessions (CST / UTC+8)

| Phase | Time | Description |
|---|---|---|
| Pre-Open Auction | 09:15 - 09:25 | Call auction, orders accepted |
| Opening Auction | 09:25 - 09:30 | Opening price determination |
| Morning Session | 09:30 - 11:30 | Continuous trading |
| Lunch Break | 11:30 - 13:00 | No trading |
| Afternoon Session | 13:00 - 14:57 | Continuous trading |
| Closing Auction (SZSE only) | 14:57 - 15:00 | Closing price determination |
| After-Hours (STAR/ChiNext only) | 15:05 - 15:30 | Limited trading |

### Price Limits (涨跌停)

| Board | Daily Limit | Notes |
|---|---|---|
| Main Board (主板) | ±10% | Based on previous close |
| ChiNext (创业板) | ±20% | Since 2020 reform |
| STAR Market (科创板) | ±20% | Since inception |
| New listings | No limit for first 5 days | ChiNext/STAR Market only |

### Order Types

| Type | Description |
|---|---|
| Limit Order | Trade at specified price or better |
| Market Order | Trade immediately at best available price |
| Cancel | Withdraw a resting order from the book |

### Lot Sizes

| Board | Standard Lot |
|---|---|
| Main Board / ChiNext | 100 shares |
| STAR Market (科创板) | 200 shares |

### Tick Size

All boards: **0.01 CNY** per share.

## Quick Schema Reference

### Order Stream — SSE

`SecurityID`, `TransactTime`, `OrderNo`, `Price`, `Balance`, `OrderBSFlag` (B/S), `OrdType` (A/D), `OrderIndex`, `ChannelNo`, `BizIndex`

### Order Stream — SZSE

`ApplSeqNum`, `Side` (1/2), `OrdType` (1/2), `Price`, `OrderQty`, `TransactTime`, `ChannelNo`
(Symbol from filename; also includes optional `ExpirationDays`, `ExpirationType`, `Contactor`, `ConfirmID`)

### Tick/Trade Stream — SSE

`SecurityID`, `TradeTime`, `TradePrice`, `TradeQty`, `TradeAmount`, `BuyNo`, `SellNo`, `TradeIndex`, `ChannelNo`, `TradeBSFlag` (B/S/N), `BizIndex`

### Tick/Trade Stream — SZSE

`ApplSeqNum`, `BidApplSeqNum`, `OfferApplSeqNum`, `Price`, `Qty`, `ExecType` (F/4), `TransactTime`, `ChannelNo`
(Symbol from filename; `ExecType='F'`=Fill, `'4'`=Cancel)

### L2 Snapshots — SZSE Only

Key fields: `QuotTime`, `PreClosePx`, `OpenPx`, `HighPx`, `LowPx`, `LastPx`, `Volume`, `Amount`, `BidPrice` (10 levels), `BidOrderQty`, `OfferPrice` (10 levels), `OfferOrderQty`, `UpperLimitPx`, `LowerLimitPx`, `TradingPhaseCode`

Array fields are JSON bracket notation: `"[11.63,11.62,11.61,...]"`

~30-second snapshot intervals during trading hours.

## Symbol Code Ranges

| Exchange | Code Range | Market |
|---|---|---|
| SSE | 600000-699999 | Main Board |
| SSE | 680000-689999 | STAR Market (科创板) |
| SSE | 510000-519999 | ETFs |
| SZSE | 000001-009999 | Main Board |
| SZSE | 300000-309999 | ChiNext (创业板) |
| SZSE | 159000-159999 | ETFs |

## Full Data Format Reference

For complete column definitions, exchange semantics, timestamp details, and data volume estimates, read [references/quant360_cn_l2.md](references/quant360_cn_l2.md).
