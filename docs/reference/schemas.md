# Data Lake Schema Reference

This document is synchronized with the live code schema registry.

Authoritative schema definitions live in:
- `pointline/schema_registry.py`
- table modules in `pointline/tables/`, `pointline/dim_symbol.py`, and `pointline/io/delta_manifest_repo.py`

## Table Registry

| Table | Lake Path | Partitioned By | has_date (registry) | has_date (config) | Columns |
|---|---|---|---:|---:|---:|
| `dim_symbol` | `silver/dim_symbol` | `` | False | False | 17 |
| `stock_basic_cn` | `silver/stock_basic_cn` | `` | False | False | 21 |
| `dim_asset_stats` | `silver/dim_asset_stats` | `` | True | True | 11 |
| `ingest_manifest` | `silver/ingest_manifest` | `` | True | True | 19 |
| `validation_log` | `silver/validation_log` | `` | True | True | 19 |
| `dim_exchange` | `silver/dim_exchange` | `` | False | False | 6 |
| `dim_trading_calendar` | `silver/dim_trading_calendar` | `` | True | True | 6 |
| `dq_summary` | `silver/dq_summary` | `` | True | True | 14 |
| `trades` | `silver/trades` | `exchange, date` | True | True | 15 |
| `quotes` | `silver/quotes` | `exchange, date` | True | True | 13 |
| `book_snapshot_25` | `silver/book_snapshot_25` | `exchange, date` | True | True | 11 |
| `derivative_ticker` | `silver/derivative_ticker` | `exchange, date` | True | True | 14 |
| `liquidations` | `silver/liquidations` | `exchange, date` | True | True | 11 |
| `options_chain` | `silver/options_chain` | `exchange, date` | True | True | 26 |
| `kline_1h` | `silver/kline_1h` | `exchange, date` | True | True | 16 |
| `kline_1d` | `silver/kline_1d` | `exchange, date` | True | True | 16 |
| `l3_orders` | `silver/l3_orders` | `exchange, date` | True | True | 13 |
| `l3_ticks` | `silver/l3_ticks` | `exchange, date` | True | True | 14 |

## Reference And Metadata Tables

### `dim_symbol`

- Storage path: `silver/dim_symbol`
- Partition columns: `[]`
- Has date column: `False`

| Column | Type |
|---|---|
| `symbol_id` | `Int64` |
| `exchange_id` | `Int16` |
| `exchange` | `String` |
| `exchange_symbol` | `String` |
| `base_asset` | `String` |
| `quote_asset` | `String` |
| `asset_type` | `UInt8` |
| `tick_size` | `Float64` |
| `lot_size` | `Float64` |
| `contract_size` | `Float64` |
| `expiry_ts_us` | `Int64` |
| `underlying_symbol_id` | `Int64` |
| `strike` | `Float64` |
| `put_call` | `String` |
| `valid_from_ts` | `Int64` |
| `valid_until_ts` | `Int64` |
| `is_current` | `Boolean` |

### `stock_basic_cn`

- Storage path: `silver/stock_basic_cn`
- Partition columns: `[]`
- Has date column: `False`

| Column | Type |
|---|---|
| `ts_code` | `String` |
| `symbol` | `String` |
| `name` | `String` |
| `area` | `String` |
| `industry` | `String` |
| `fullname` | `String` |
| `enname` | `String` |
| `cnspell` | `String` |
| `market` | `String` |
| `exchange` | `String` |
| `curr_type` | `String` |
| `list_status` | `String` |
| `list_date` | `Date` |
| `delist_date` | `Date` |
| `is_hs` | `String` |
| `act_name` | `String` |
| `act_ent_type` | `String` |
| `exchange_id` | `Int16` |
| `exchange_symbol` | `String` |
| `as_of_date` | `Date` |
| `ingest_ts_us` | `Int64` |

### `dim_asset_stats`

- Storage path: `silver/dim_asset_stats`
- Partition columns: `[]`
- Has date column: `True`

| Column | Type |
|---|---|
| `base_asset` | `String` |
| `date` | `Date` |
| `coingecko_coin_id` | `String` |
| `circulating_supply` | `Float64` |
| `total_supply` | `Float64` |
| `max_supply` | `Float64` |
| `market_cap_usd` | `Float64` |
| `fully_diluted_valuation_usd` | `Float64` |
| `updated_at_ts` | `Int64` |
| `fetched_at_ts` | `Int64` |
| `source` | `String` |

### `ingest_manifest`

- Storage path: `silver/ingest_manifest`
- Partition columns: `[]`
- Has date column: `True`

| Column | Type |
|---|---|
| `file_id` | `Int32` |
| `vendor` | `String` |
| `data_type` | `String` |
| `bronze_file_name` | `String` |
| `sha256` | `String` |
| `file_size_bytes` | `Int64` |
| `last_modified_ts` | `Int64` |
| `date` | `Date` |
| `status` | `String` |
| `created_at_us` | `Int64` |
| `processed_at_us` | `Int64` |
| `row_count` | `Int64` |
| `ts_local_min_us` | `Int64` |
| `ts_local_max_us` | `Int64` |
| `error_message` | `String` |
| `scd2_new` | `Int32` |
| `scd2_modified` | `Int32` |
| `scd2_delisted` | `Int32` |
| `scd2_unchanged` | `Int32` |

### `validation_log`

- Storage path: `silver/validation_log`
- Partition columns: `[]`
- Has date column: `True`

| Column | Type |
|---|---|
| `validation_id` | `Int64` |
| `file_id` | `Int32` |
| `table_name` | `String` |
| `validated_at` | `Int64` |
| `validation_status` | `String` |
| `expected_rows` | `Int64` |
| `ingested_rows` | `Int64` |
| `missing_rows` | `Int64` |
| `extra_rows` | `Int64` |
| `mismatched_rows` | `Int64` |
| `mismatch_sample` | `String` |
| `validation_duration_ms` | `Int64` |
| `vendor` | `String` |
| `data_type` | `String` |
| `exchange` | `String` |
| `date` | `Date` |
| `filtered_row_count` | `Int64` |
| `filtered_symbol_count` | `Int64` |
| `error_message` | `String` |

### `dim_exchange`

- Storage path: `silver/dim_exchange`
- Partition columns: `[]`
- Has date column: `False`

| Column | Type |
|---|---|
| `exchange` | `String` |
| `exchange_id` | `Int16` |
| `asset_class` | `String` |
| `timezone` | `String` |
| `description` | `String` |
| `is_active` | `Boolean` |

### `dim_trading_calendar`

- Storage path: `silver/dim_trading_calendar`
- Partition columns: `[]`
- Has date column: `True`

| Column | Type |
|---|---|
| `exchange` | `String` |
| `date` | `Date` |
| `is_trading_day` | `Boolean` |
| `session_type` | `String` |
| `open_time_us` | `Int64` |
| `close_time_us` | `Int64` |

### `dq_summary`

- Storage path: `silver/dq_summary`
- Partition columns: `[]`
- Has date column: `True`

| Column | Type |
|---|---|
| `run_id` | `Int64` |
| `table_name` | `String` |
| `date` | `Date` |
| `row_count` | `Int64` |
| `null_counts` | `String` |
| `duplicate_rows` | `Int64` |
| `min_ts_us` | `Int64` |
| `max_ts_us` | `Int64` |
| `freshness_lag_sec` | `Int64` |
| `status` | `String` |
| `issue_counts` | `String` |
| `profile_stats` | `String` |
| `validated_at` | `Int64` |
| `validation_duration_ms` | `Int64` |

## Silver Market Data Tables

### `trades`

- Storage path: `silver/trades`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `ts_exch_us` | `Int64` |
| `trade_id` | `String` |
| `side` | `UInt8` |
| `px_int` | `Int64` |
| `qty_int` | `Int64` |
| `flags` | `Int32` |
| `conditions` | `Int32` |
| `venue_id` | `Int16` |
| `sequence_number` | `Int64` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `quotes`

- Storage path: `silver/quotes`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `ts_exch_us` | `Int64` |
| `bid_px_int` | `Int64` |
| `bid_sz_int` | `Int64` |
| `ask_px_int` | `Int64` |
| `ask_sz_int` | `Int64` |
| `conditions` | `Int32` |
| `venue_id` | `Int16` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `book_snapshot_25`

- Storage path: `silver/book_snapshot_25`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `ts_exch_us` | `Int64` |
| `bids_px_int` | `List(Int64)` |
| `bids_sz_int` | `List(Int64)` |
| `asks_px_int` | `List(Int64)` |
| `asks_sz_int` | `List(Int64)` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `derivative_ticker`

- Storage path: `silver/derivative_ticker`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `ts_exch_us` | `Int64` |
| `mark_px_int` | `Int64` |
| `index_px_int` | `Int64` |
| `last_px_int` | `Int64` |
| `funding_rate_int` | `Int64` |
| `predicted_funding_rate_int` | `Int64` |
| `funding_ts_us` | `Int64` |
| `oi_int` | `Int64` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `liquidations`

- Storage path: `silver/liquidations`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `ts_exch_us` | `Int64` |
| `liq_id` | `String` |
| `side` | `UInt8` |
| `px_int` | `Int64` |
| `qty_int` | `Int64` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `options_chain`

- Storage path: `silver/options_chain`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `underlying_symbol` | `String` |
| `symbol` | `String` |
| `underlying_index` | `String` |
| `ts_local_us` | `Int64` |
| `ts_exch_us` | `Int64` |
| `option_type` | `UInt8` |
| `strike_int` | `Int64` |
| `expiry_ts_us` | `Int64` |
| `bid_px_int` | `Int64` |
| `ask_px_int` | `Int64` |
| `bid_sz_int` | `Int64` |
| `ask_sz_int` | `Int64` |
| `mark_px_int` | `Int64` |
| `underlying_px_int` | `Int64` |
| `iv` | `Float64` |
| `mark_iv` | `Float64` |
| `delta` | `Float64` |
| `gamma` | `Float64` |
| `vega` | `Float64` |
| `theta` | `Float64` |
| `rho` | `Float64` |
| `open_interest` | `Float64` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `kline_1h`

- Storage path: `silver/kline_1h`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_bucket_start_us` | `Int64` |
| `ts_bucket_end_us` | `Int64` |
| `open_px_int` | `Int64` |
| `high_px_int` | `Int64` |
| `low_px_int` | `Int64` |
| `close_px_int` | `Int64` |
| `volume_qty_int` | `Int64` |
| `quote_volume_int` | `Int64` |
| `trade_count` | `Int64` |
| `taker_buy_base_qty_int` | `Int64` |
| `taker_buy_quote_qty_int` | `Int64` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `kline_1d`

- Storage path: `silver/kline_1d`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_bucket_start_us` | `Int64` |
| `ts_bucket_end_us` | `Int64` |
| `open_px_int` | `Int64` |
| `high_px_int` | `Int64` |
| `low_px_int` | `Int64` |
| `close_px_int` | `Int64` |
| `volume_qty_int` | `Int64` |
| `quote_volume_int` | `Int64` |
| `trade_count` | `Int64` |
| `taker_buy_base_qty_int` | `Int64` |
| `taker_buy_quote_qty_int` | `Int64` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `l3_orders`

- Storage path: `silver/l3_orders`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `appl_seq_num` | `Int64` |
| `side` | `UInt8` |
| `ord_type` | `UInt8` |
| `px_int` | `Int64` |
| `order_qty_int` | `Int64` |
| `channel_no` | `Int32` |
| `trading_phase` | `UInt8` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

### `l3_ticks`

- Storage path: `silver/l3_ticks`
- Partition columns: `['exchange', 'date']`
- Has date column: `True`

| Column | Type |
|---|---|
| `date` | `Date` |
| `exchange` | `String` |
| `symbol` | `String` |
| `ts_local_us` | `Int64` |
| `appl_seq_num` | `Int64` |
| `bid_appl_seq_num` | `Int64` |
| `offer_appl_seq_num` | `Int64` |
| `exec_type` | `UInt8` |
| `px_int` | `Int64` |
| `qty_int` | `Int64` |
| `channel_no` | `Int32` |
| `trading_phase` | `UInt8` |
| `file_id` | `Int32` |
| `file_line_number` | `Int32` |

## Notes

- Market-data tables currently use `symbol` (string) in live schemas.
- `ingest_manifest` includes SCD2 audit fields: `scd2_new`, `scd2_modified`, `scd2_delisted`, `scd2_unchanged`.
- Time columns with `_us` suffix are microseconds.
