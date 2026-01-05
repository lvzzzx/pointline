use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use deltalake::arrow::array::{
    BooleanArray, Int32Array, Int64Array, LargeListArray, ListArray, StructArray, UInt8Array,
};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::*;
use deltalake::datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::datafusion::scalar::ScalarValue;
use deltalake::open_table;
use futures::StreamExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Update {
    pub ts_local_us: i64,
    pub ingest_seq: i32,
    pub file_line_number: i32,
    pub is_snapshot: bool,
    pub side: u8, // 0 = bid, 1 = ask
    pub price_int: i64,
    pub size_int: i64,
    pub file_id: i32,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OrderBook {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
}

impl OrderBook {
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn apply_update(&mut self, update: &L2Update) {
        let side_map = match update.side {
            0 => &mut self.bids,
            1 => &mut self.asks,
            _ => {
                debug_assert!(false, "invalid side: {}", update.side);
                return;
            }
        };

        if update.size_int == 0 {
            side_map.remove(&update.price_int);
        } else {
            side_map.insert(update.price_int, update.size_int);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamPos {
    pub ts_local_us: i64,
    pub ingest_seq: i32,
    pub file_line_number: i32,
    pub file_id: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct ReplayConfig {
    pub checkpoint_every_us: Option<i64>,
    pub checkpoint_every_updates: Option<u64>,
    pub validate_monotonic: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            checkpoint_every_us: None,
            checkpoint_every_updates: None,
            validate_monotonic: false,
        }
    }
}

#[derive(Debug, Clone)]
struct Checkpoint {
    ts_local_us: i64,
    file_id: i32,
    ingest_seq: i32,
    file_line_number: i32,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub exchange_id: i16,
    pub symbol_id: i64,
    pub ts_local_us: i64,
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub struct SnapshotWithPos {
    pub exchange_id: i16,
    pub symbol_id: i64,
    pub ts_local_us: i64,
    pub ingest_seq: i32,
    pub file_line_number: i32,
    pub file_id: i32,
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
}

#[derive(Default)]
struct SnapshotReset {
    snapshot_key: Option<(i64, i32)>,
}

impl SnapshotReset {
    fn apply(&mut self, book: &mut OrderBook, update: &L2Update) {
        if update.is_snapshot {
            let key = (update.ts_local_us, update.file_id);
            if self.snapshot_key != Some(key) {
                book.reset();
                self.snapshot_key = Some(key);
            }
        } else {
            self.snapshot_key = None;
        }

        book.apply_update(update);
    }
}

fn ts_to_date(ts_local_us: i64) -> Result<NaiveDate> {
    let seconds = ts_local_us / 1_000_000;
    let micros = ts_local_us % 1_000_000;
    let nanos = (micros as i64 * 1_000) as u32;
    let dt = NaiveDateTime::from_timestamp_opt(seconds, nanos)
        .ok_or_else(|| anyhow!("invalid ts_local_us: {}", ts_local_us))?;
    Ok(dt.date())
}

fn parse_date_opt(value: Option<&str>) -> Result<Option<NaiveDate>> {
    value
        .map(|val| {
            NaiveDate::parse_from_str(val, "%Y-%m-%d")
                .with_context(|| format!("invalid date string: {}", val))
        })
        .transpose()
}

fn date_to_scalar(date: NaiveDate) -> ScalarValue {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let days = (date - epoch).num_days() as i32;
    ScalarValue::Date32(Some(days))
}

fn delta_table_exists(path: &str) -> bool {
    Path::new(path).join("_delta_log").exists()
}

fn after_pos_expr(pos: &StreamPos) -> Expr {
    let ts = lit(pos.ts_local_us);
    let ingest = lit(pos.ingest_seq);
    let line = lit(pos.file_line_number);

    col("ts_local_us")
        .gt(ts.clone())
        .or(col("ts_local_us").eq(ts).and(
            col("ingest_seq")
                .gt(ingest.clone())
                .or(col("ingest_seq").eq(ingest).and(col("file_line_number").gt(line))),
        ))
}

async fn latest_checkpoint(
    checkpoint_path: Option<&str>,
    exchange: Option<&str>,
    exchange_id: i16,
    symbol_id: i64,
    ts_local_us: i64,
) -> Result<Option<Checkpoint>> {
    let checkpoint_path = match checkpoint_path {
        Some(path) if delta_table_exists(path) => path,
        _ => return Ok(None),
    };

    let target_date = ts_to_date(ts_local_us)?;
    let table = open_table(checkpoint_path).await?;
    let ctx = SessionContext::new();
    ctx.register_table("checkpoints", Arc::new(table))?;

    let mut df = ctx.table("checkpoints").await?;
    df = df.filter(col("exchange_id").eq(lit(exchange_id)))?;
    df = df.filter(col("symbol_id").eq(lit(symbol_id)))?;
    if let Some(exchange) = exchange {
        df = df.filter(col("exchange").eq(lit(exchange)))?;
    }
    df = df.filter(col("date").eq(Expr::Literal(date_to_scalar(target_date))))?;
    df = df.filter(col("ts_local_us").lt_eq(lit(ts_local_us)))?;

    df = df.select(vec![
        col("ts_local_us"),
        col("bids"),
        col("asks"),
        col("file_id"),
        col("ingest_seq"),
        col("file_line_number"),
    ])?;
    df = df.sort(vec![
        col("ts_local_us").sort(false, true),
        col("ingest_seq").sort(false, true),
        col("file_line_number").sort(false, true),
    ])?;
    df = df.limit(0, Some(1))?;

    let batches = df.collect().await?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(None);
    }

    let batch = &batches[0];
    let row = 0;
    let ts = get_i64(batch, "ts_local_us", row)?;
    let file_id = get_i32(batch, "file_id", row)?;
    let ingest_seq = get_i32(batch, "ingest_seq", row)?;
    let file_line_number = get_i32(batch, "file_line_number", row)?;

    let bids = get_levels(batch, "bids", row)?;
    let asks = get_levels(batch, "asks", row)?;

    Ok(Some(Checkpoint {
        ts_local_us: ts,
        file_id,
        ingest_seq,
        file_line_number,
        bids,
        asks,
    }))
}

async fn build_updates_df(
    updates_path: &str,
    exchange: Option<&str>,
    exchange_id: i16,
    symbol_id: i64,
    start_date: NaiveDate,
    end_date: NaiveDate,
    max_ts_inclusive: i64,
    min_pos_exclusive: Option<StreamPos>,
) -> Result<DataFrame> {
    let table = open_table(updates_path).await?;
    let ctx = SessionContext::new();
    ctx.register_table("updates", Arc::new(table))?;

    let mut df = ctx.table("updates").await?;
    df = df.filter(col("exchange_id").eq(lit(exchange_id)))?;
    df = df.filter(col("symbol_id").eq(lit(symbol_id)))?;
    if let Some(exchange) = exchange {
        df = df.filter(col("exchange").eq(lit(exchange)))?;
    }

    let start_date_expr = Expr::Literal(date_to_scalar(start_date));
    let end_date_expr = Expr::Literal(date_to_scalar(end_date));
    df = df.filter(col("date").gt_eq(start_date_expr))?;
    df = df.filter(col("date").lt_eq(end_date_expr))?;

    df = df.filter(col("ts_local_us").lt_eq(lit(max_ts_inclusive)))?;

    if let Some(pos) = min_pos_exclusive {
        df = df.filter(after_pos_expr(&pos))?;
    }

    df = df.select(vec![
        col("ts_local_us"),
        col("ingest_seq"),
        col("file_line_number"),
        col("is_snapshot"),
        col("side"),
        col("price_int"),
        col("size_int"),
        col("file_id"),
    ])?;
    df = df.sort(vec![
        col("ts_local_us").sort(true, true),
        col("ingest_seq").sort(true, true),
        col("file_line_number").sort(true, true),
    ])?;

    Ok(df)
}

struct UpdateColumns<'a> {
    ts_local_us: &'a Int64Array,
    ingest_seq: &'a Int32Array,
    file_line_number: &'a Int32Array,
    is_snapshot: &'a BooleanArray,
    side: &'a UInt8Array,
    price_int: &'a Int64Array,
    size_int: &'a Int64Array,
    file_id: &'a Int32Array,
}

fn update_columns<'a>(batch: &'a RecordBatch) -> Result<UpdateColumns<'a>> {
    Ok(UpdateColumns {
        ts_local_us: get_array(batch, "ts_local_us")?,
        ingest_seq: get_array(batch, "ingest_seq")?,
        file_line_number: get_array(batch, "file_line_number")?,
        is_snapshot: get_array(batch, "is_snapshot")?,
        side: get_array(batch, "side")?,
        price_int: get_array(batch, "price_int")?,
        size_int: get_array(batch, "size_int")?,
        file_id: get_array(batch, "file_id")?,
    })
}

fn update_from_columns(cols: &UpdateColumns<'_>, row: usize) -> L2Update {
    L2Update {
        ts_local_us: cols.ts_local_us.value(row),
        ingest_seq: cols.ingest_seq.value(row),
        file_line_number: cols.file_line_number.value(row),
        is_snapshot: cols.is_snapshot.value(row),
        side: cols.side.value(row),
        price_int: cols.price_int.value(row),
        size_int: cols.size_int.value(row),
        file_id: cols.file_id.value(row),
    }
}

async fn for_each_update<F>(mut stream: SendableRecordBatchStream, mut f: F) -> Result<()>
where
    F: FnMut(L2Update),
{
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let cols = update_columns(&batch)?;
        for row in 0..batch.num_rows() {
            f(update_from_columns(&cols, row));
        }
    }
    Ok(())
}

fn get_array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let idx = batch.schema().index_of(name)?;
    let array = batch.column(idx);
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("column {} has unexpected type", name))
}

fn get_i64(batch: &RecordBatch, name: &str, row: usize) -> Result<i64> {
    Ok(get_array::<Int64Array>(batch, name)?.value(row))
}

fn get_i32(batch: &RecordBatch, name: &str, row: usize) -> Result<i32> {
    Ok(get_array::<Int32Array>(batch, name)?.value(row))
}

fn get_levels(batch: &RecordBatch, name: &str, row: usize) -> Result<Vec<(i64, i64)>> {
    let idx = batch.schema().index_of(name)?;
    let array = batch.column(idx);
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return list_levels(list, row);
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        return large_list_levels(list, row);
    }
    Err(anyhow!("column {} has unexpected list type", name))
}

fn list_levels(list: &ListArray, row: usize) -> Result<Vec<(i64, i64)>> {
    if list.is_null(row) {
        return Ok(Vec::new());
    }
    let values = list.value(row);
    let struct_array = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow!("list values are not struct array"))?;
    struct_levels(struct_array)
}

fn large_list_levels(list: &LargeListArray, row: usize) -> Result<Vec<(i64, i64)>> {
    if list.is_null(row) {
        return Ok(Vec::new());
    }
    let values = list.value(row);
    let struct_array = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow!("list values are not struct array"))?;
    struct_levels(struct_array)
}

fn struct_levels(struct_array: &StructArray) -> Result<Vec<(i64, i64)>> {
    let prices = struct_array
        .column_by_name("price_int")
        .ok_or_else(|| anyhow!("missing price_int"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("price_int has unexpected type"))?;
    let sizes = struct_array
        .column_by_name("size_int")
        .ok_or_else(|| anyhow!("missing size_int"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("size_int has unexpected type"))?;

    let mut levels = Vec::with_capacity(struct_array.len());
    for row in 0..struct_array.len() {
        if struct_array.is_null(row) {
            continue;
        }
        levels.push((prices.value(row), sizes.value(row)));
    }
    Ok(levels)
}

fn book_levels(book: &OrderBook) -> (Vec<(i64, i64)>, Vec<(i64, i64)>) {
    let bids = book
        .bids
        .iter()
        .rev()
        .map(|(price, size)| (*price, *size))
        .collect();
    let asks = book
        .asks
        .iter()
        .map(|(price, size)| (*price, *size))
        .collect();
    (bids, asks)
}

pub async fn snapshot_at_delta(
    updates_path: &str,
    checkpoint_path: Option<&str>,
    exchange: Option<&str>,
    exchange_id: i16,
    symbol_id: i64,
    ts_local_us: i64,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<Snapshot> {
    let fallback_date = ts_to_date(ts_local_us)?;
    let start_override = parse_date_opt(start_date)?;
    let end_override = parse_date_opt(end_date)?;
    let (start_date, end_date) = match (start_override, end_override) {
        (Some(start), Some(end)) => (start, end),
        _ => (fallback_date, fallback_date),
    };

    let checkpoint = latest_checkpoint(
        checkpoint_path,
        exchange,
        exchange_id,
        symbol_id,
        ts_local_us,
    )
    .await?;

    let mut book = OrderBook::default();
    let mut min_pos = None;
    if let Some(checkpoint) = &checkpoint {
        book.bids = checkpoint.bids.iter().cloned().collect();
        book.asks = checkpoint.asks.iter().cloned().collect();
        min_pos = Some(StreamPos {
            ts_local_us: checkpoint.ts_local_us,
            ingest_seq: checkpoint.ingest_seq,
            file_line_number: checkpoint.file_line_number,
            file_id: checkpoint.file_id,
        });
    }

    let df = build_updates_df(
        updates_path,
        exchange,
        exchange_id,
        symbol_id,
        start_date,
        end_date,
        ts_local_us,
        min_pos,
    )
    .await?;

    let mut reset = SnapshotReset::default();
    let stream = df.execute_stream().await?;
    for_each_update(stream, |update| reset.apply(&mut book, &update)).await?;

    let (bids, asks) = book_levels(&book);
    Ok(Snapshot {
        exchange_id,
        symbol_id,
        ts_local_us,
        bids,
        asks,
    })
}

pub async fn replay_between_delta(
    updates_path: &str,
    checkpoint_path: Option<&str>,
    exchange: Option<&str>,
    exchange_id: i16,
    symbol_id: i64,
    start_ts_local_us: i64,
    end_ts_local_us: i64,
    every_us: Option<i64>,
    every_updates: Option<u64>,
) -> Result<Vec<SnapshotWithPos>> {
    if every_us.unwrap_or(0) <= 0 && every_updates.unwrap_or(0) == 0 {
        return Err(anyhow!(
            "replay_between: set every_us or every_updates"
        ));
    }

    let start_date = ts_to_date(start_ts_local_us)?;
    let end_date = ts_to_date(end_ts_local_us)?;

    let checkpoint = latest_checkpoint(
        checkpoint_path,
        exchange,
        exchange_id,
        symbol_id,
        start_ts_local_us,
    )
    .await?;

    let mut book = OrderBook::default();
    let mut min_pos = None;
    if let Some(checkpoint) = &checkpoint {
        book.bids = checkpoint.bids.iter().cloned().collect();
        book.asks = checkpoint.asks.iter().cloned().collect();
        min_pos = Some(StreamPos {
            ts_local_us: checkpoint.ts_local_us,
            ingest_seq: checkpoint.ingest_seq,
            file_line_number: checkpoint.file_line_number,
            file_id: checkpoint.file_id,
        });
    }

    let df = build_updates_df(
        updates_path,
        exchange,
        exchange_id,
        symbol_id,
        start_date,
        end_date,
        end_ts_local_us,
        min_pos,
    )
    .await?;

    let mut snapshots = Vec::new();
    let mut reset = SnapshotReset::default();
    let mut last_emit_ts: Option<i64> = None;
    let mut updates_since: u64 = 0;

    let stream = df.execute_stream().await?;
    for_each_update(stream, |update| {
        reset.apply(&mut book, &update);

        if update.ts_local_us < start_ts_local_us {
            return;
        }

        let pos = StreamPos {
            ts_local_us: update.ts_local_us,
            ingest_seq: update.ingest_seq,
            file_line_number: update.file_line_number,
            file_id: update.file_id,
        };

        if last_emit_ts.is_none() {
            last_emit_ts = Some(update.ts_local_us);
        }
        updates_since = updates_since.saturating_add(1);

        let mut emit = false;
        if let (Some(every_us), Some(last_ts)) = (every_us, last_emit_ts) {
            if update.ts_local_us.saturating_sub(last_ts) >= every_us {
                emit = true;
            }
        }
        if let Some(every_updates) = every_updates {
            if every_updates > 0 && updates_since >= every_updates {
                emit = true;
            }
        }

        if emit {
            let (bids, asks) = book_levels(&book);
            snapshots.push(SnapshotWithPos {
                exchange_id,
                symbol_id,
                ts_local_us: pos.ts_local_us,
                ingest_seq: pos.ingest_seq,
                file_line_number: pos.file_line_number,
                file_id: pos.file_id,
                bids,
                asks,
            });
            updates_since = 0;
            last_emit_ts = Some(update.ts_local_us);
        }
    })
    .await?;

    Ok(snapshots)
}

pub fn replay<I, F, G>(
    updates: I,
    config: &ReplayConfig,
    mut on_snapshot: F,
    mut on_checkpoint: G,
) where
    I: IntoIterator<Item = L2Update>,
    F: FnMut(&OrderBook, &StreamPos),
    G: FnMut(&OrderBook, &StreamPos),
{
    let mut book = OrderBook::default();
    let mut prev_key: Option<(i64, i32, i32)> = None;

    let mut snapshot_key: Option<(i64, i32)> = None; // (ts_local_us, file_id)
    let mut snapshot_pos: Option<StreamPos> = None;

    let mut last_checkpoint_ts: Option<i64> = None;
    let mut updates_since: u64 = 0;

    for update in updates {
        let current_key = (update.ts_local_us, update.ingest_seq, update.file_line_number);
        if config.validate_monotonic {
            if let Some(prev) = prev_key {
                if current_key < prev {
                    panic!(
                        "updates out of order: prev={:?}, current={:?}",
                        prev, current_key
                    );
                }
            }
        }
        prev_key = Some(current_key);

        if update.is_snapshot {
            let key = (update.ts_local_us, update.file_id);
            if snapshot_key != Some(key) {
                if snapshot_key.is_some() {
                    if let Some(pos) = snapshot_pos {
                        on_snapshot(&book, &pos);
                    }
                }
                book.reset();
                snapshot_key = Some(key);
                snapshot_pos = None;
            }
        } else if snapshot_key.is_some() {
            if let Some(pos) = snapshot_pos {
                on_snapshot(&book, &pos);
            }
            snapshot_key = None;
            snapshot_pos = None;
        }

        book.apply_update(&update);

        let pos = StreamPos {
            ts_local_us: update.ts_local_us,
            ingest_seq: update.ingest_seq,
            file_line_number: update.file_line_number,
            file_id: update.file_id,
        };

        if update.is_snapshot {
            snapshot_pos = Some(pos);
        }

        updates_since = updates_since.saturating_add(1);
        if last_checkpoint_ts.is_none() {
            last_checkpoint_ts = Some(update.ts_local_us);
        }

        let mut emit_checkpoint = false;
        if let (Some(every_us), Some(last_ts)) = (config.checkpoint_every_us, last_checkpoint_ts) {
            if update.ts_local_us.saturating_sub(last_ts) >= every_us {
                emit_checkpoint = true;
            }
        }
        if let Some(every_updates) = config.checkpoint_every_updates {
            if every_updates > 0 && updates_since >= every_updates {
                emit_checkpoint = true;
            }
        }

        if emit_checkpoint {
            on_checkpoint(&book, &pos);
            updates_since = 0;
            last_checkpoint_ts = Some(update.ts_local_us);
        }
    }

    if snapshot_key.is_some() {
        if let Some(pos) = snapshot_pos {
            on_snapshot(&book, &pos);
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{replay_between_delta, snapshot_at_delta, L2Update, OrderBook, Snapshot, SnapshotWithPos};
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};
    use std::sync::OnceLock;
    use tokio::runtime::Runtime;

    fn runtime() -> &'static Runtime {
        static RUNTIME: OnceLock<Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| Runtime::new().expect("tokio runtime"))
    }

    fn levels_to_py(py: Python<'_>, levels: &[(i64, i64)]) -> PyObject {
        let list = PyList::empty(py);
        for (price, size) in levels {
            let dict = PyDict::new(py);
            dict.set_item("price_int", *price).ok();
            dict.set_item("size_int", *size).ok();
            list.append(dict).ok();
        }
        list.into()
    }

    fn snapshot_to_py(py: Python<'_>, snapshot: Snapshot) -> PyObject {
        let dict = PyDict::new(py);
        dict.set_item("exchange_id", snapshot.exchange_id).ok();
        dict.set_item("symbol_id", snapshot.symbol_id).ok();
        dict.set_item("ts_local_us", snapshot.ts_local_us).ok();
        dict.set_item("bids", levels_to_py(py, &snapshot.bids)).ok();
        dict.set_item("asks", levels_to_py(py, &snapshot.asks)).ok();
        dict.into()
    }

    fn snapshot_with_pos_to_py(py: Python<'_>, snapshot: SnapshotWithPos) -> PyObject {
        let dict = PyDict::new(py);
        dict.set_item("exchange_id", snapshot.exchange_id).ok();
        dict.set_item("symbol_id", snapshot.symbol_id).ok();
        dict.set_item("ts_local_us", snapshot.ts_local_us).ok();
        dict.set_item("ingest_seq", snapshot.ingest_seq).ok();
        dict.set_item("file_line_number", snapshot.file_line_number).ok();
        dict.set_item("file_id", snapshot.file_id).ok();
        dict.set_item("bids", levels_to_py(py, &snapshot.bids)).ok();
        dict.set_item("asks", levels_to_py(py, &snapshot.asks)).ok();
        dict.into()
    }

    #[pyfunction]
    fn snapshot_at(
        py: Python<'_>,
        updates_path: String,
        checkpoint_path: Option<String>,
        exchange: Option<String>,
        exchange_id: i16,
        symbol_id: i64,
        ts_local_us: i64,
        start_date: Option<String>,
        end_date: Option<String>,
    ) -> PyResult<PyObject> {
        let result = py.allow_threads(|| {
            runtime().block_on(snapshot_at_delta(
                &updates_path,
                checkpoint_path.as_deref(),
                exchange.as_deref(),
                exchange_id,
                symbol_id,
                ts_local_us,
                start_date.as_deref(),
                end_date.as_deref(),
            ))
        });

        match result {
            Ok(snapshot) => Ok(snapshot_to_py(py, snapshot)),
            Err(err) => Err(PyRuntimeError::new_err(err.to_string())),
        }
    }

    #[pyfunction]
    fn replay_between(
        py: Python<'_>,
        updates_path: String,
        checkpoint_path: Option<String>,
        exchange: Option<String>,
        exchange_id: i16,
        symbol_id: i64,
        start_ts_local_us: i64,
        end_ts_local_us: i64,
        every_us: Option<i64>,
        every_updates: Option<u64>,
    ) -> PyResult<Vec<PyObject>> {
        let result = py.allow_threads(|| {
            runtime().block_on(replay_between_delta(
                &updates_path,
                checkpoint_path.as_deref(),
                exchange.as_deref(),
                exchange_id,
                symbol_id,
                start_ts_local_us,
                end_ts_local_us,
                every_us,
                every_updates,
            ))
        });

        match result {
            Ok(snapshots) => Ok(snapshots
                .into_iter()
                .map(|snapshot| snapshot_with_pos_to_py(py, snapshot))
                .collect()),
            Err(err) => Err(PyRuntimeError::new_err(err.to_string())),
        }
    }

    #[derive(FromPyObject)]
    struct PyUpdate {
        ts_local_us: i64,
        ingest_seq: i32,
        file_line_number: i32,
        is_snapshot: bool,
        side: u8,
        price_int: i64,
        size_int: i64,
        file_id: i32,
    }

    impl From<PyUpdate> for L2Update {
        fn from(update: PyUpdate) -> Self {
            L2Update {
                ts_local_us: update.ts_local_us,
                ingest_seq: update.ingest_seq,
                file_line_number: update.file_line_number,
                is_snapshot: update.is_snapshot,
                side: update.side,
                price_int: update.price_int,
                size_int: update.size_int,
                file_id: update.file_id,
            }
        }
    }

    #[pyclass]
    struct Engine {
        exchange_id: i16,
        symbol_id: i64,
        book: OrderBook,
        snapshot_key: Option<(i64, i32)>,
    }

    #[pymethods]
    impl Engine {
        #[new]
        fn new(exchange_id: i16, symbol_id: i64) -> Self {
            Self {
                exchange_id,
                symbol_id,
                book: OrderBook::default(),
                snapshot_key: None,
            }
        }

        fn _seed_book(&mut self, bids: Vec<(i64, i64)>, asks: Vec<(i64, i64)>) {
            self.book.bids.clear();
            self.book.asks.clear();
            for (price, size) in bids {
                self.book.bids.insert(price, size);
            }
            for (price, size) in asks {
                self.book.asks.insert(price, size);
            }
            self.snapshot_key = None;
        }

        fn _apply_batch(&mut self, updates: Vec<PyUpdate>) {
            for update in updates {
                let update = L2Update::from(update);
                if update.is_snapshot {
                    let key = (update.ts_local_us, update.file_id);
                    if self.snapshot_key != Some(key) {
                        self.book.reset();
                        self.snapshot_key = Some(key);
                    }
                } else {
                    self.snapshot_key = None;
                }

                self.book.apply_update(&update);
            }
        }

        fn snapshot(&self, py: Python<'_>) -> PyObject {
            let bids: Vec<(i64, i64)> = self
                .book
                .bids
                .iter()
                .rev()
                .map(|(price, size)| (*price, *size))
                .collect();
            let asks: Vec<(i64, i64)> = self
                .book
                .asks
                .iter()
                .map(|(price, size)| (*price, *size))
                .collect();

            let snapshot = PyDict::new(py);
            snapshot.set_item("exchange_id", self.exchange_id).ok();
            snapshot.set_item("symbol_id", self.symbol_id).ok();
            snapshot.set_item("bids", levels_to_py(py, &bids)).ok();
            snapshot.set_item("asks", levels_to_py(py, &asks)).ok();
            snapshot.into()
        }
    }

    #[pymodule]
    fn l2_replay(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
        module.add_class::<Engine>()?;
        module.add_function(wrap_pyfunction!(snapshot_at, module)?)?;
        module.add_function(wrap_pyfunction!(replay_between, module)?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{replay, L2Update, OrderBook, ReplayConfig, StreamPos};
    use std::panic::catch_unwind;

    fn update(
        ts_local_us: i64,
        ingest_seq: i32,
        file_line_number: i32,
        is_snapshot: bool,
        side: u8,
        price_int: i64,
        size_int: i64,
        file_id: i32,
    ) -> L2Update {
        L2Update {
            ts_local_us,
            ingest_seq,
            file_line_number,
            is_snapshot,
            side,
            price_int,
            size_int,
            file_id,
        }
    }

    #[test]
    fn snapshot_groups_emit_once() {
        let updates = vec![
            update(1, 1, 1, false, 0, 100, 5, 10),
            update(2, 2, 2, false, 1, 101, 3, 10),
            update(3, 3, 3, true, 0, 99, 1, 10),
            update(3, 4, 4, true, 1, 102, 2, 10),
            update(4, 5, 5, false, 0, 100, 4, 10),
        ];

        let mut snapshots: Vec<(OrderBook, StreamPos)> = Vec::new();
        replay(
            updates,
            &ReplayConfig::default(),
            |book, pos| snapshots.push((book.clone(), *pos)),
            |_book, _pos| {},
        );

        assert_eq!(snapshots.len(), 1);
        let (book, _pos) = &snapshots[0];
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.bids.get(&99), Some(&1));
        assert_eq!(book.asks.get(&102), Some(&2));
    }

    #[test]
    fn checkpoints_emit_on_update_cadence() {
        let updates = vec![
            update(1, 1, 1, false, 0, 100, 5, 10),
            update(2, 2, 2, false, 1, 101, 3, 10),
            update(3, 3, 3, false, 0, 99, 1, 10),
            update(4, 4, 4, false, 1, 102, 2, 10),
            update(5, 5, 5, false, 0, 100, 4, 10),
        ];

        let config = ReplayConfig {
            checkpoint_every_updates: Some(2),
            ..ReplayConfig::default()
        };

        let mut checkpoints: Vec<StreamPos> = Vec::new();
        replay(
            updates,
            &config,
            |_book, _pos| {},
            |_book, pos| checkpoints.push(*pos),
        );

        assert_eq!(checkpoints.len(), 2);
        assert_eq!(checkpoints[0].ts_local_us, 2);
        assert_eq!(checkpoints[1].ts_local_us, 4);
    }

    #[test]
    fn checkpoints_emit_on_time_cadence() {
        let updates = vec![
            update(0, 1, 1, false, 0, 100, 5, 10),
            update(3, 2, 2, false, 1, 101, 3, 10),
            update(10, 3, 3, false, 0, 99, 1, 10),
        ];

        let config = ReplayConfig {
            checkpoint_every_us: Some(5),
            ..ReplayConfig::default()
        };

        let mut checkpoints: Vec<StreamPos> = Vec::new();
        replay(
            updates,
            &config,
            |_book, _pos| {},
            |_book, pos| checkpoints.push(*pos),
        );

        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].ts_local_us, 10);
    }

    #[test]
    fn monotonic_validation_panics_on_out_of_order() {
        let updates = vec![
            update(2, 2, 2, false, 0, 100, 5, 10),
            update(1, 1, 1, false, 1, 101, 3, 10),
        ];

        let config = ReplayConfig {
            validate_monotonic: true,
            ..ReplayConfig::default()
        };

        let result = catch_unwind(|| {
            replay(
                updates,
                &config,
                |_book, _pos| {},
                |_book, _pos| {},
            );
        });

        assert!(result.is_err());
    }
}
