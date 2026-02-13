# Simplified Ingestion Design v2

**Status:** Proposal
**Scope:** Function-first ingestion design that preserves PIT and determinism guarantees

---

## 1. Philosophy

Keep only what protects correctness. Everything else should stay simple functions.

---

## 2. What Must Stay

These are required and non-negotiable:

1. Manifest-based idempotency (`skip success`, `pending/success/failed/quarantined`).
2. File identity key: `(vendor, data_type, bronze_path, file_hash)`.
3. PIT symbol coverage check against `dim_symbol` (quarantine when uncovered).
4. Lineage columns: `file_id`, `file_seq`.
5. Partitioning by `exchange`, `trading_date` (exchange-timezone-aware).
6. Validation + validation log.

If a simplification removes one of these, it is not accepted.

---

## 3. Minimal Architecture

Three practical parts only:

1. `parser(path) -> df`
2. `ingest_file(meta) -> result`
3. `manifest + delta write helpers`

No framework-heavy orchestration required.

---

## 4. Single-File Flow

```python
def ingest_file(meta, *, force=False, dry_run=False):
    if not force and manifest.is_success(meta):
        return skipped

    file_id = 0 if dry_run else manifest.resolve_file_id(meta)
    df = parser_for(meta)(meta.path)
    df = canonicalize(df, meta)

    # Derive partition key from exchange local timezone.
    df = derive_trading_date(df, exchange_timezone_map)
    ensure_partition_alignment(df, partition_cols=["exchange", "trading_date"])

    df = quarantine_uncovered_symbols(df, dim_symbol)  # may return quarantined status
    df = encode_storage(df)
    df = add_lineage(df, file_id=file_id)
    df = normalize_schema(df)
    df = validate_rows(df)  # may return failed/quarantined status

    if not dry_run:
        write_events(df, partition_by=["exchange", "trading_date"])

    return update_manifest_and_validation_log(file_id, meta, df)
```

Failure paths are explicit and short:

1. Empty parse -> `failed`.
2. Exchange-timezone partition mismatch -> `failed`.
3. No PIT symbol coverage -> `quarantined`.
4. Validation removes all rows -> `quarantined`.

---

## 5. Concrete Simplifications

What we simplify:

- Multi-layer orchestration -> one pipeline function.
- Complex parser framework -> parser modules + direct mapping.
- Heavy write abstraction -> thin `manifest` and `write_events` helpers.

What we do **not** simplify away:

- Manifest semantics.
- PIT quarantine gate.
- Lineage determinism.
- Validation and audit logging.

---

## 6. Runtime Constraints

- Single node only.
- Polars + Delta Lake only.
- No cloud/distributed assumptions.
- No backward compatibility or migration requirement for old contracts.

---

## 7. Acceptance Criteria

v2 is done when all are true:

1. Event partitions are always `(exchange, trading_date)` from exchange timezone conversion.
2. Manifest identity and statuses remain deterministic across reruns.
3. PIT quarantine behavior remains deterministic.
4. Lineage keys remain deterministic (`file_id`, `file_seq`).
5. Ingestion code is materially smaller and easier to read.

---

## 8. Related Docs

- Architecture: `docs/architecture/design.md`
- Schema: `docs/architecture/schema-design-v4.md`
