# Ingestion Pipeline Reference

## Table of Contents
- [ingest_file Signature](#ingest_file-signature)
- [Pipeline Stages Detail](#pipeline-stages-detail)
- [Table Aliases](#table-aliases)
- [IngestionResult](#ingestionresult)
- [Error Handling](#error-handling)
- [Testing Ingestion](#testing-ingestion)

## ingest_file Signature

```python
def ingest_file(
    meta: BronzeFileMetadata,
    *,
    parser: Parser,            # Callable[[BronzeFileMetadata], pl.DataFrame]
    manifest_repo: ManifestStore,
    writer: Writer,            # Callable[[str, pl.DataFrame], None]
    dim_symbol_df: pl.DataFrame,
    quarantine_store: QuarantineStore | None = None,
    force: bool = False,       # Skip idempotency check
    dry_run: bool = False,     # Parse + validate but don't write
) -> IngestionResult
```

`BronzeFileMetadata` fields:
- `vendor: str` — e.g., `"tardis"`, `"quant360"`, `"tushare"`
- `data_type: str` — vendor-specific type name (e.g., `"trades"`, `"order_new_STK_SH"`)
- `bronze_file_path: str` — relative path within bronze_root
- `file_size_bytes: int`
- `last_modified_ts: float`
- `sha256: str` — file content hash
- `date: str | None` — optional date hint
- `interval: str | None` — optional interval hint
- `extra: dict | None` — vendor-specific metadata

## Pipeline Stages Detail

### Stage 1: Resolve Table

`_TABLE_ALIASES` maps vendor `data_type` strings to canonical table names:
- `"trades"` → `"trades"`
- `"book_snapshot_*"` → `"orderbook_updates"`
- `"order_new_STK_SH"` → `"cn_order_events"`
- etc.

The canonical table name determines which `TableSpec` governs validation, normalization, and writing.

### Stage 2: Idempotency Check

Identity tuple: `(vendor, data_type, bronze_path, file_hash)`. Built by `build_manifest_identity(meta)`.

- If identity exists in manifest with status `"completed"` → skip (return early)
- `force=True` bypasses this check
- `manifest_repo.resolve_file_id(identity)` returns `file_id` (new or existing)

### Stage 3: Parse

`parser(meta) → pl.DataFrame` — vendor-specific parsing. The parser reads the Bronze file and returns a DataFrame with vendor-native column names. Column mapping to canonical schema happens in later stages.

Parser contract:
- Must return a non-empty DataFrame on valid input
- Should raise on corrupt/unreadable files
- Column names may be vendor-specific (canonicalization happens next)

### Stage 4: Canonicalize

Vendor-specific transforms applied after parsing:
- Quant360: renames columns (`ApplSeqNum` → `order_ref`, `Side` 1/2 → `buy`/`sell`), derives `exchange` from filename
- Tardis: minimal — parsers already output near-canonical columns

### Stage 5: Trading Date Derivation

```python
from pointline.ingestion.timezone import derive_trading_date_frame
df = derive_trading_date_frame(df)
```

Requires `exchange` and `ts_event_us` columns. Looks up timezone via `EXCHANGE_TIMEZONE_MAP`:
- Crypto exchanges → UTC (trading_date = UTC date of ts_event_us)
- CN exchanges → Asia/Shanghai (trading_date = CST date)

### Stage 6: Generic Validation

```python
from pointline.ingestion.event_validation import apply_event_validations
valid_df, quarantined_df = apply_event_validations(df, table_name)
```

Table-specific rules:
- **trades**: `side ∈ {buy, sell, unknown}`, `price > 0`, `qty > 0`
- **quotes**: `bid_price > 0`, `ask_price > 0`, `bid_qty >= 0`, `ask_qty >= 0`, `bid_price <= ask_price` (no crossed)
- **orderbook_updates**: `side ∈ {bid, ask}`, `price > 0`, `qty >= 0`
- **cn_order_events**: `event_kind ∈ {ADD, CANCEL}`, `side ∈ {buy, sell}`
- **cn_tick_events**: `event_kind ∈ {FILL, CANCEL}`

### Stage 7: CN-Specific Validation

```python
from pointline.ingestion.cn_validation import apply_cn_validations
valid_df, quarantined_df = apply_cn_validations(df, table_name)
```

SSE-specific quarantine rules:
- `cn_order_events` from SSE: quarantine rows where `channel_biz_seq` is null OR `symbol_order_seq` is null
- `cn_tick_events` from SSE: quarantine rows where `channel_biz_seq` is null OR `symbol_trade_seq` is null

SZSE rows are not affected (these fields are always present in SZSE feeds).

### Stage 8: PIT Coverage Check

```python
from pointline.ingestion.pit import check_pit_coverage
covered_df, orphan_df = check_pit_coverage(df, dim_symbol_df)
```

As-of join: for each event row, find `dim_symbol` row where `valid_from_ts_us <= ts_event_us < valid_until_ts_us` and `exchange` + `exchange_symbol` match.

Orphans (no matching dim_symbol entry) are quarantined. This ensures every Silver row has a valid `symbol_id`.

### Stage 9: Lineage Assignment

```python
from pointline.ingestion.lineage import assign_lineage
df = assign_lineage(df, file_id=42)
```

Adds two columns:
- `file_id`: constant value identifying the Bronze source file
- `file_seq`: 1-indexed row position within this file (monotonically increasing)

Together `(file_id, file_seq)` uniquely identifies every Silver row's Bronze origin.

### Stage 10: Normalize

```python
from pointline.ingestion.normalize import normalize_to_table_spec
df = normalize_to_table_spec(df, spec)
```

- Casts columns to canonical Polars types from `TableSpec`
- Adds missing nullable columns as `null` literals
- Validates scaled columns are `Int64`
- Selects only columns defined in spec (drops extras)
- Orders columns per spec

### Stages 11-13: Write + Manifest

- **Write**: `writer(table_name, valid_df)` — appends valid rows to Delta Lake, partitioned by `(exchange, trading_date)`
- **Quarantine**: Invalid rows written to `validation_log` via `quarantine_store.append()`
- **Manifest**: `manifest_repo.update_status(file_id, "completed", rows_total=N, rows_written=M, rows_quarantined=Q)`

## Testing Ingestion

```python
# Unit test pattern: test a single stage
def test_trading_date_derivation():
    df = pl.DataFrame({"exchange": ["binance"], "ts_event_us": [1700000000_000000]})
    result = derive_trading_date_frame(df)
    assert result["trading_date"][0] == date(2023, 11, 14)

# Integration test pattern: full pipeline with in-memory stores
def test_ingest_file_end_to_end(tmp_path):
    silver = tmp_path / "silver"
    meta = BronzeFileMetadata(vendor="tardis", data_type="trades", ...)
    result = ingest_file(meta=meta, parser=mock_parser, writer=delta_writer, ...)
    assert result.rows_written > 0
    assert result.rows_quarantined == 0
```

Key test markers: `@pytest.mark.slow` for full pipeline tests, `@pytest.mark.integration` for Delta Lake I/O.
