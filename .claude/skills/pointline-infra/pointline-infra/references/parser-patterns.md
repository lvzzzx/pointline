# Vendor Parser Development Guide

## Table of Contents
- [Parser Contract](#parser-contract)
- [Tardis Parser Patterns](#tardis-parser-patterns)
- [Quant360 Parser Patterns](#quant360-parser-patterns)
- [Adding a New Vendor](#adding-a-new-vendor)
- [Testing Parsers](#testing-parsers)

## Parser Contract

A parser is a callable: `(BronzeFileMetadata) → pl.DataFrame`.

Requirements:
- Read the Bronze file at `meta.bronze_file_path`
- Return a `pl.DataFrame` with columns mappable to the target `TableSpec`
- Must include `exchange` and `ts_event_us` columns (or vendor equivalents that get canonicalized)
- Should raise on corrupt/unreadable files
- Should NOT add `file_id`, `file_seq`, `trading_date` — these are added by later pipeline stages

## Tardis Parser Patterns

Location: `pointline/vendors/tardis/parsers.py`

Six parsers: `parse_trades`, `parse_book_snapshot`, `parse_incremental_book`, `parse_quotes`, `parse_derivative_ticker`, `parse_liquidations`.

### Helper Functions

```python
# Require specific columns exist in the raw CSV
_require_columns(df, ["timestamp", "price", "amount"], context="trades")

# Resolve ts_event_us: prefer 'timestamp' over 'local_timestamp'
ts_expr = _resolve_ts_event_expr(df)  # returns pl.Expr

# Scale float to Int64: multiply by PRICE_SCALE, cast
price_expr = _scaled_expr("price", PRICE_SCALE)  # col("price") * 1e9 → Int64

# Optional Utf8 column (null if missing)
_optional_utf8(df, "trade_id")  # pl.lit(None).cast(pl.Utf8) if missing

# First present Int64 column from candidates
_first_present_int64(df, ["seq_num", "sequence"])  # first found, or null

# Optional Float64 column
_optional_float64(df, "open_interest")
```

### Typical Parser Structure

```python
def parse_trades(meta: BronzeFileMetadata) -> pl.DataFrame:
    df = pl.read_csv(meta.bronze_file_path)
    _require_columns(df, ["timestamp", "price", "amount", "side"])

    return df.select([
        pl.lit(meta.extra["exchange"]).alias("exchange"),
        pl.lit(meta.extra["symbol"]).alias("symbol"),
        _resolve_ts_event_expr(df).alias("ts_event_us"),
        _scaled_expr("price", PRICE_SCALE).alias("price"),
        _scaled_expr("amount", QTY_SCALE).alias("qty"),
        pl.col("side").str.to_lowercase().alias("side"),
        _optional_utf8(df, "id").alias("trade_id"),
    ])
```

### Key Conventions

- Exchange name: lowercased, hyphenated (e.g., `"binance-futures"`, `"okex-swap"`)
- Timestamps: Tardis provides microsecond UTC timestamps directly
- Prices/quantities: float → scaled Int64 via `_scaled_expr`
- Missing optional fields → `null` with correct type

## Quant360 Parser Patterns

Location: `pointline/vendors/quant360/`

### Dispatch

`dispatch.py` routes based on `meta.data_type`:

```python
_PARSER_BY_DATA_TYPE = {
    "order_new_STK_SH": parse_order_stream,    # SSE orders
    "order_new_STK_SZ": parse_order_stream,     # SZSE orders
    "tick_new_STK_SH": parse_tick_stream,       # SSE ticks
    "tick_new_STK_SZ": parse_tick_stream,        # SZSE ticks
    "L2_new_STK_SZ": parse_l2_snapshot_stream,  # SZSE L2 (SSE has no L2)
}
```

### Exchange Detection

Exchange derived from `data_type` suffix:
- `*_SH` → `"sse"`
- `*_SZ` → `"szse"`

### Column Mapping Differences

Quant360 CSVs have exchange-specific column names. Parsers normalize to pointline canonical names:

**Orders:**
- SSE: `OrderNo` → `order_ref`, `OrderBSFlag` B/S → `side` buy/sell, `OrdType` A/D → `event_kind` ADD/CANCEL
- SZSE: `ApplSeqNum` → `order_ref`, `Side` 1/2 → `side` buy/sell, `OrdType` 1/2 → `order_type` market/limit

**Ticks:**
- SSE: `BuyNo`/`SellNo` → `bid_order_ref`/`ask_order_ref`, `TradeBSFlag` → `aggressor_side`
- SZSE: `BidApplSeqNum`/`OfferApplSeqNum` → `bid_order_ref`/`ask_order_ref`, aggressor inferred from ref comparison

**Timestamps:** `YYYYMMDDHHMMSSmmm` format (CST) → UTC microseconds.

### Symbol from Filename

SZSE CSVs don't have a symbol column — symbol is the CSV filename (e.g., `000001.csv`). Parser extracts from `meta.bronze_file_path`.

## Adding a New Vendor

1. Create module: `pointline/vendors/<vendor_name>/`
2. Implement parser functions: `(BronzeFileMetadata) → pl.DataFrame`
3. Add table alias in `pointline/ingestion/pipeline.py` `_TABLE_ALIASES` if data_type name differs from canonical table name
4. Add exchange timezone mapping in `pointline/ingestion/exchange.py` if new exchanges
5. Add any vendor-specific canonicalization step if column names differ significantly
6. Write tests with sample Bronze files in `tests/fixtures/`

### Checklist for New Parser

- [ ] Output columns match target `TableSpec` (or close enough for normalize step)
- [ ] `exchange` column present and lowercased
- [ ] `ts_event_us` is Int64 UTC microseconds
- [ ] Prices/quantities scaled by `PRICE_SCALE`/`QTY_SCALE`
- [ ] Side values are lowercase: `"buy"`, `"sell"`, `"unknown"`
- [ ] Optional/nullable columns have correct Polars types (not Python None)
- [ ] No `file_id`, `file_seq`, `trading_date` columns (added by pipeline)
- [ ] Handles edge cases: empty files, missing optional columns

## Testing Parsers

```python
# Fixture: minimal valid Bronze file
@pytest.fixture
def sample_trades_csv(tmp_path):
    csv = tmp_path / "trades.csv"
    csv.write_text("timestamp,price,amount,side\n1700000000000000,50000.5,0.1,buy\n")
    return csv

def test_parse_trades(sample_trades_csv):
    meta = BronzeFileMetadata(
        vendor="tardis", data_type="trades",
        bronze_file_path=str(sample_trades_csv),
        file_size_bytes=100, last_modified_ts=0.0, sha256="abc",
        extra={"exchange": "binance-futures", "symbol": "BTCUSDT"},
    )
    df = parse_trades(meta)
    assert df.shape[0] == 1
    assert "ts_event_us" in df.columns
    assert df["price"].dtype == pl.Int64
    assert df["price"][0] == 50000_500_000_000  # 50000.5 * PRICE_SCALE
```

Test edge cases:
- Empty file → should raise or return empty DataFrame (vendor-specific)
- Missing optional columns → null with correct type
- Malformed timestamps → should raise
- Extreme values (very small qty, very large price) → verify scaling precision
