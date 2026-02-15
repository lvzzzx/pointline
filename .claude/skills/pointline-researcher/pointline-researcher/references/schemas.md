# Pointline Table Schema Reference

All tables use `PRICE_SCALE = QTY_SCALE = 1_000_000_000`. Scaled columns are `Int64`.

## Event Tables (partitioned by `exchange`, `trading_date`)

### trades

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | Lowercased exchange name |
| trading_date | Date | | | Derived from ts_event_us in exchange-local tz |
| symbol | Utf8 | | | Exchange symbol string |
| symbol_id | Int64 | | | dim_symbol foreign key |
| ts_event_us | Int64 | | | Event timestamp, UTC microseconds |
| ts_local_us | Int64 | | Yes | Local/receive timestamp |
| file_id | Int64 | | | Ingestion lineage |
| file_seq | Int64 | | | Row sequence within file |
| trade_id | Utf8 | | Yes | Exchange-native trade ID |
| side | Utf8 | | | "buy" or "sell" |
| is_buyer_maker | Boolean | | Yes | True if buyer is maker |
| price | Int64 | PRICE_SCALE | | Trade price |
| qty | Int64 | QTY_SCALE | | Trade quantity |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

### quotes

| Column | Type | Scale | Nullable |
|---|---|---|---|
| exchange | Utf8 | | |
| trading_date | Date | | |
| symbol | Utf8 | | |
| symbol_id | Int64 | | |
| ts_event_us | Int64 | | |
| ts_local_us | Int64 | | Yes |
| file_id | Int64 | | |
| file_seq | Int64 | | |
| bid_price | Int64 | PRICE_SCALE | |
| bid_qty | Int64 | QTY_SCALE | |
| ask_price | Int64 | PRICE_SCALE | |
| ask_qty | Int64 | QTY_SCALE | |
| seq_num | Int64 | | Yes |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

### orderbook_updates

| Column | Type | Scale | Nullable |
|---|---|---|---|
| exchange | Utf8 | | |
| trading_date | Date | | |
| symbol | Utf8 | | |
| symbol_id | Int64 | | |
| ts_event_us | Int64 | | |
| ts_local_us | Int64 | | Yes |
| file_id | Int64 | | |
| file_seq | Int64 | | |
| book_seq | Int64 | | Yes |
| side | Utf8 | | |
| price | Int64 | PRICE_SCALE | |
| qty | Int64 | QTY_SCALE | |
| is_snapshot | Boolean | | |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, book_seq, file_id, file_seq)`

### derivative_ticker

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | |
| trading_date | Date | | | |
| symbol | Utf8 | | | |
| symbol_id | Int64 | | | |
| ts_event_us | Int64 | | | |
| ts_local_us | Int64 | | Yes | |
| file_id | Int64 | | | |
| file_seq | Int64 | | | |
| mark_price | Int64 | PRICE_SCALE | | Mark price |
| index_price | Int64 | PRICE_SCALE | Yes | Underlying index price |
| last_price | Int64 | PRICE_SCALE | Yes | Last traded price |
| open_interest | Int64 | QTY_SCALE | Yes | OI (contracts or base units — varies by exchange) |
| funding_rate | Float64 | | Yes | Current funding rate |
| predicted_funding_rate | Float64 | | Yes | Predicted next funding rate |
| funding_ts_us | Int64 | | Yes | Next funding event timestamp |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

**Validation:** `mark_price > 0`

### liquidations

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | |
| trading_date | Date | | | |
| symbol | Utf8 | | | |
| symbol_id | Int64 | | | |
| ts_event_us | Int64 | | | |
| ts_local_us | Int64 | | Yes | |
| file_id | Int64 | | | |
| file_seq | Int64 | | | |
| liquidation_id | Utf8 | | Yes | Exchange-native liquidation ID |
| side | Utf8 | | | "buy" or "sell" |
| price | Int64 | PRICE_SCALE | | |
| qty | Int64 | QTY_SCALE | | |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

**Validation:** `side ∈ {buy, sell}`, `price > 0`, `qty > 0`

### options_chain

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | |
| trading_date | Date | | | |
| symbol | Utf8 | | | |
| symbol_id | Int64 | | | |
| ts_event_us | Int64 | | | |
| ts_local_us | Int64 | | Yes | |
| file_id | Int64 | | | |
| file_seq | Int64 | | | |
| option_type | Utf8 | | | "call" or "put" |
| strike | Int64 | PRICE_SCALE | | Strike price |
| expiration_ts_us | Int64 | | | Contract expiration timestamp |
| open_interest | Int64 | QTY_SCALE | Yes | |
| last_price | Int64 | PRICE_SCALE | Yes | |
| bid_price | Int64 | PRICE_SCALE | Yes | |
| bid_qty | Int64 | QTY_SCALE | Yes | |
| bid_iv | Float64 | | Yes | Bid implied volatility |
| ask_price | Int64 | PRICE_SCALE | Yes | |
| ask_qty | Int64 | QTY_SCALE | Yes | |
| ask_iv | Float64 | | Yes | Ask implied volatility |
| mark_price | Int64 | PRICE_SCALE | Yes | |
| mark_iv | Float64 | | Yes | Mark implied volatility |
| underlying_index | Utf8 | | Yes | |
| underlying_price | Int64 | PRICE_SCALE | Yes | |
| delta | Float64 | | Yes | |
| gamma | Float64 | | Yes | |
| vega | Float64 | | Yes | |
| theta | Float64 | | Yes | |
| rho | Float64 | | Yes | |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, file_id, file_seq)`

**Validation:** `option_type ∈ {call, put}`, `strike > 0`, `expiration_ts_us > 0`

### cn_order_events

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | "sse" or "szse" |
| trading_date | Date | | | |
| symbol | Utf8 | | | e.g. "600000", "000001" |
| symbol_id | Int64 | | | |
| ts_event_us | Int64 | | | |
| ts_local_us | Int64 | | Yes | |
| file_id | Int64 | | | |
| file_seq | Int64 | | | |
| channel_id | Int32 | | | Feed channel (1-6) |
| channel_seq | Int64 | | | Per-channel feed sequence |
| channel_biz_seq | Int64 | | Yes | Per-channel business seq |
| symbol_order_seq | Int64 | | Yes | Per-symbol order counter |
| order_ref | Int64 | | | Order reference number |
| event_kind | Utf8 | | | "ADD" or "CANCEL" |
| side | Utf8 | | | "buy" or "sell" |
| order_type | Utf8 | | Yes | "limit" or "market" |
| price | Int64 | PRICE_SCALE | | |
| qty | Int64 | QTY_SCALE | | |

**Tie-break keys:** `(exchange, symbol_id, trading_date, channel_id, channel_seq, file_id, file_seq)`

### cn_tick_events

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | |
| trading_date | Date | | | |
| symbol | Utf8 | | | |
| symbol_id | Int64 | | | |
| ts_event_us | Int64 | | | |
| ts_local_us | Int64 | | Yes | |
| file_id | Int64 | | | |
| file_seq | Int64 | | | |
| channel_id | Int32 | | | |
| channel_seq | Int64 | | | |
| channel_biz_seq | Int64 | | Yes | |
| symbol_trade_seq | Int64 | | Yes | Per-symbol trade counter |
| bid_order_ref | Int64 | | Yes | Buy-side order reference |
| ask_order_ref | Int64 | | Yes | Sell-side order reference |
| event_kind | Utf8 | | | "FILL" or "CANCEL" |
| aggressor_side | Utf8 | | Yes | "buy", "sell", or null (auction) |
| price | Int64 | PRICE_SCALE | | |
| qty | Int64 | QTY_SCALE | | |

**Tie-break keys:** `(exchange, symbol_id, trading_date, channel_id, channel_seq, file_id, file_seq)`

### cn_l2_snapshots

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| exchange | Utf8 | | | |
| trading_date | Date | | | |
| symbol | Utf8 | | | |
| symbol_id | Int64 | | | |
| ts_event_us | Int64 | | | |
| ts_local_us | Int64 | | Yes | |
| file_id | Int64 | | | |
| file_seq | Int64 | | | |
| snapshot_seq | Int64 | | | |
| image_status | Utf8 | | Yes | |
| trading_phase_code | Utf8 | | Yes | |
| bid_price_levels | List(Int64) | | | 10-level bid prices (scaled) |
| bid_qty_levels | List(Int64) | | | 10-level bid quantities |
| ask_price_levels | List(Int64) | | | 10-level ask prices (scaled) |
| ask_qty_levels | List(Int64) | | | 10-level ask quantities |
| bid_order_count_levels | List(Int64) | | Yes | Orders per bid level |
| ask_order_count_levels | List(Int64) | | Yes | Orders per ask level |
| pre_close_price | Int64 | PRICE_SCALE | Yes | |
| open_price | Int64 | PRICE_SCALE | Yes | |
| high_price | Int64 | PRICE_SCALE | Yes | |
| low_price | Int64 | PRICE_SCALE | Yes | |
| last_price | Int64 | PRICE_SCALE | Yes | |
| volume | Int64 | | Yes | Cumulative volume |
| amount | Int64 | | Yes | Cumulative turnover |
| num_trades | Int64 | | Yes | |
| total_bid_qty | Int64 | | Yes | |
| total_ask_qty | Int64 | | Yes | |

**Tie-break keys:** `(exchange, symbol_id, ts_event_us, snapshot_seq, file_id, file_seq)`

~30-second snapshot intervals. SZSE only. Bid/ask level arrays are scaled by PRICE_SCALE.

## Dimension Table

### dim_symbol (SCD Type 2, unpartitioned)

| Column | Type | Scale | Nullable | Notes |
|---|---|---|---|---|
| symbol_id | Int64 | | | Stable surrogate key |
| exchange | Utf8 | | | |
| exchange_symbol | Utf8 | | | Native exchange symbol |
| canonical_symbol | Utf8 | | | Normalized symbol |
| market_type | Utf8 | | | e.g. "perpetual", "spot", "main_board" |
| base_asset | Utf8 | | | |
| quote_asset | Utf8 | | | |
| valid_from_ts_us | Int64 | | | Validity window start |
| valid_until_ts_us | Int64 | | | Validity window end (exclusive) |
| is_current | Boolean | | | True for latest version |
| tick_size | Int64 | PRICE_SCALE | Yes | Min price increment |
| lot_size | Int64 | QTY_SCALE | Yes | Min order quantity |
| contract_size | Int64 | QTY_SCALE | Yes | For derivatives |
| updated_at_ts_us | Int64 | | | |

**Business keys:** `(exchange, exchange_symbol, valid_from_ts_us)`

Validity semantics: `valid_from_ts_us <= event_ts < valid_until_ts_us`. Current rows have `valid_until_ts_us = 2^63 - 1`.

## Control Tables (unpartitioned)

### ingest_manifest

| Column | Type | Nullable |
|---|---|---|
| file_id | Int64 | |
| vendor | Utf8 | |
| data_type | Utf8 | |
| bronze_path | Utf8 | |
| file_hash | Utf8 | |
| status | Utf8 | |
| rows_total | Int64 | |
| rows_written | Int64 | |
| rows_quarantined | Int64 | |
| trading_date_min | Date | Yes |
| trading_date_max | Date | Yes |
| created_at_ts_us | Int64 | |
| processed_at_ts_us | Int64 | Yes |
| status_reason | Utf8 | Yes |

**Business keys:** `(vendor, data_type, bronze_path, file_hash)`

### validation_log

| Column | Type | Nullable |
|---|---|---|
| file_id | Int64 | |
| rule_name | Utf8 | |
| severity | Utf8 | |
| logged_at_ts_us | Int64 | |
| file_seq | Int64 | Yes |
| field_name | Utf8 | Yes |
| field_value | Utf8 | Yes |
| ts_event_us | Int64 | Yes |
| symbol | Utf8 | Yes |
| symbol_id | Int64 | Yes |
| message | Utf8 | Yes |

## Storage Layout

```
silver_root/
├── trades/              (partitioned: exchange=*/trading_date=*)
├── quotes/
├── orderbook_updates/
├── derivative_ticker/
├── liquidations/
├── options_chain/
├── cn_order_events/
├── cn_tick_events/
├── cn_l2_snapshots/
├── dim_symbol/          (unpartitioned)
├── ingest_manifest/
└── validation_log/
```

Use `table_path(silver_root, table_name)` from `pointline.storage.delta.layout` to resolve paths.

## TableSpec API

```python
from pointline.schemas import get_table_spec

spec = get_table_spec("trades")
spec.columns()           # tuple of column names
spec.to_polars()         # dict[str, pl.DataType]
spec.scaled_columns()    # columns with scale factor
spec.scale_for("price")  # returns PRICE_SCALE
spec.tie_break_keys      # deterministic sort order
spec.partition_by        # Delta Lake partitions
```
