# Storage Contracts Reference

## Table of Contents
- [Protocol Definitions](#protocol-definitions)
- [Delta Lake Implementations](#delta-lake-implementations)
- [Storage Layout](#storage-layout)
- [Partitioning Strategy](#partitioning-strategy)
- [Compaction and Vacuum](#compaction-and-vacuum)

## Protocol Definitions

All protocols in `pointline/storage/contracts.py`. Implementations are in `pointline/storage/delta/`.

### ManifestStore

```python
class ManifestStore(Protocol):
    def resolve_file_id(
        self, vendor: str, data_type: str, bronze_path: str, file_hash: str
    ) -> int:
        """Return existing or new file_id for the given identity tuple."""

    def filter_pending(self) -> pl.DataFrame:
        """Return manifest rows with status='pending'."""

    def update_status(
        self, file_id: int, status: str, *,
        rows_total: int = 0, rows_written: int = 0, rows_quarantined: int = 0,
        status_reason: str | None = None,
    ) -> None:
        """Update manifest entry after processing."""
```

Idempotency key: `(vendor, data_type, bronze_path, file_hash)`.

### EventStore

```python
class EventStore(Protocol):
    def append(self, table_name: str, df: pl.DataFrame) -> None:
        """Append DataFrame to the named Delta table, partitioned by spec."""
```

Handles Delta Lake write mechanics: partition resolution, schema enforcement, conflict resolution.

### DimensionStore

```python
class DimensionStore(Protocol):
    def load_dim_symbol(self) -> pl.DataFrame:
        """Load full dim_symbol table."""

    def save_dim_symbol(self, df: pl.DataFrame, expected_version: int) -> None:
        """Save dim_symbol with optimistic concurrency control.
        Raises ConflictError if current version != expected_version."""

    def current_version(self) -> int:
        """Return current version number for OCC."""
```

OCC (Optimistic Concurrency Control): read version before modification, pass to `save_dim_symbol`. If another writer committed in between, the save fails.

### QuarantineStore

```python
class QuarantineStore(Protocol):
    def append(
        self, table_name: str, df: pl.DataFrame,
        reason: str, file_id: int
    ) -> None:
        """Write invalid rows to validation_log with quarantine reason."""
```

Quarantine records include: `file_id`, `rule_name`, `severity`, `logged_at_ts_us`, and optional context fields (`file_seq`, `field_name`, `field_value`, `ts_event_us`, `symbol`, `symbol_id`, `message`).

### PartitionOptimizer

```python
class PartitionOptimizer(Protocol):
    def compact_partitions(
        self, table_name: str, *,
        exchange: str | None = None,
        trading_date: date | None = None,
        target_size_mb: int = 128,
    ) -> CompactionReport:
        """Merge small files within partitions."""
```

`CompactionReport` includes: `partitions_processed`, `files_before`, `files_after`, `bytes_before`, `bytes_after`.

### TableVacuum

```python
class TableVacuum(Protocol):
    def vacuum_table(
        self, table_name: str, *, retention_hours: int = 168
    ) -> VacuumReport:
        """Remove tombstoned files older than retention period."""
```

`VacuumReport` includes: `files_removed`, `bytes_freed`.

## Delta Lake Implementations

Located in `pointline/storage/delta/`:

- `DeltaEventStore` — implements `EventStore` + `PartitionOptimizer` + `TableVacuum`
- `DeltaDimensionStore` — implements `DimensionStore`
- `DeltaManifestStore` — implements `ManifestStore`
- `DeltaQuarantineStore` — implements `QuarantineStore`

All take `silver_root: Path` as constructor argument. Table paths resolved via:

```python
from pointline.storage.delta.layout import table_path
path = table_path(silver_root, "trades")  # silver_root / "trades"
```

## Storage Layout

```
silver_root/
├── trades/                    exchange=binance-futures/trading_date=2024-05-01/
├── quotes/                    exchange=binance-futures/trading_date=2024-05-01/
├── orderbook_updates/         exchange=binance-futures/trading_date=2024-05-01/
├── derivative_ticker/         exchange=binance-futures/trading_date=2024-05-01/
├── liquidations/              exchange=binance-futures/trading_date=2024-05-01/
├── options_chain/             exchange=deribit/trading_date=2024-05-01/
├── cn_order_events/           exchange=szse/trading_date=2024-05-01/
├── cn_tick_events/            exchange=szse/trading_date=2024-05-01/
├── cn_l2_snapshots/           exchange=szse/trading_date=2024-05-01/
├── dim_symbol/                (unpartitioned)
├── ingest_manifest/           (unpartitioned)
└── validation_log/            (unpartitioned)
```

## Partitioning Strategy

Event tables: partitioned by `(exchange, trading_date)`.
- Partition pruning on both columns for efficient queries
- Each partition is a directory with Parquet files
- Delta Lake transaction log tracks file-level metadata

Dimension/control tables: unpartitioned (small, frequently full-scanned).

## Compaction and Vacuum

**Compaction** merges small Parquet files within a partition into larger files (target ~128MB). Run after bulk ingestion to improve read performance.

```python
store = DeltaEventStore(silver_root)
report = store.compact_partitions(
    "trades", exchange="binance-futures",
    trading_date=date(2024, 5, 1), target_size_mb=128,
)
```

**Vacuum** removes tombstoned files (from previous compactions or overwrites) older than the retention period.

```python
report = store.vacuum_table("trades", retention_hours=168)  # 7 days
```

Run vacuum only after confirming no active readers depend on old files.
