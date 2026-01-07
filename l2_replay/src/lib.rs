use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use deltalake::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Builder, Int16Array, Int16Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, Int8Array, LargeListArray, ListArray, ListBuilder,
    StringBuilder, StructArray, StructBuilder, UInt8Array,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::logical_expr::ExprSchemable;
use deltalake::datafusion::prelude::*;
use deltalake::datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::datafusion::scalar::ScalarValue;
use deltalake::datafusion::execution::context::SessionConfig;
use deltalake::open_table;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::SaveMode;
use deltalake::schema::partitions::{PartitionFilter, PartitionValue};
use deltalake::DeltaOps;
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

#[derive(Debug, Clone)]
struct CheckpointMeta {
    exchange_id: i16,
    symbol_id: i64,
}

#[derive(Debug, Clone)]
struct CheckpointRow {
    exchange: String,
    exchange_id: i16,
    symbol_id: i64,
    date: i32,
    ts_local_us: i64,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
    file_id: i32,
    ingest_seq: i32,
    file_line_number: i32,
    checkpoint_kind: String,
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

fn date_to_days(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (date - epoch).num_days() as i32
}

fn days_to_date(days: i32) -> Result<NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    epoch
        .checked_add_signed(Duration::days(days as i64))
        .ok_or_else(|| anyhow!("invalid date days: {}", days))
}

fn date_to_ts_local_us(date: NaiveDate, end_of_day: bool) -> i64 {
    let dt = if end_of_day {
        date.and_hms_micro_opt(23, 59, 59, 999_999)
    } else {
        date.and_hms_micro_opt(0, 0, 0, 0)
    }
    .expect("valid date time");
    dt.timestamp_micros()
}

fn date_to_scalar(date: NaiveDate) -> ScalarValue {
    ScalarValue::Date32(Some(date_to_days(date)))
}

fn delta_table_exists(path: &str) -> bool {
    Path::new(path).join("_delta_log").exists()
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn after_pos_expr(pos: &StreamPos) -> Expr {
    let ts = lit(pos.ts_local_us);
    let ingest = lit(pos.ingest_seq);
    let file_id = lit(pos.file_id);
    let line = lit(pos.file_line_number);

    col("ts_local_us")
        .gt(ts.clone())
        .or(col("ts_local_us").eq(ts).and(
            col("ingest_seq").gt(ingest.clone()).or(col("ingest_seq").eq(ingest).and(
                col("file_id").gt(file_id.clone()).or(
                    col("file_id")
                        .eq(file_id)
                        .and(col("file_line_number").gt(line)),
                ),
            )),
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
    let mut partition_filters = Vec::new();
    if let Some(exchange) = exchange {
        partition_filters.push(PartitionFilter {
            key: "exchange".to_string(),
            value: PartitionValue::Equal(exchange.to_string()),
        });
    }
    partition_filters.push(PartitionFilter {
        key: "date".to_string(),
        value: PartitionValue::Equal(target_date.to_string()),
    });

    let file_uris = table
        .get_file_uris_by_partitions(&partition_filters)
        .context("fetch checkpoint files for partition filters")?;
    if file_uris.is_empty() {
        return Ok(None);
    }

    let config = SessionConfig::new()
        .with_parquet_pruning(false)
        .with_parquet_bloom_filter_pruning(false)
        .with_parquet_page_index_pruning(false);
    let ctx = SessionContext::new_with_config(config);
    let read_options = ParquetReadOptions::default().parquet_pruning(false);
    let mut df = ctx.read_parquet(file_uris, read_options).await?;
    df = df.filter(col("exchange_id").eq(lit(exchange_id)))?;
    df = df.filter(col("symbol_id").eq(lit(symbol_id)))?;
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
        col("file_id").sort(false, true),
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
    let mut partition_filters = Vec::new();
    if let Some(exchange) = exchange {
        partition_filters.push(PartitionFilter {
            key: "exchange".to_string(),
            value: PartitionValue::Equal(exchange.to_string()),
        });
    }
    partition_filters.push(PartitionFilter {
        key: "date".to_string(),
        value: PartitionValue::GreaterThanOrEqual(start_date.to_string()),
    });
    partition_filters.push(PartitionFilter {
        key: "date".to_string(),
        value: PartitionValue::LessThanOrEqual(end_date.to_string()),
    });

    let file_uris = table
        .get_file_uris_by_partitions(&partition_filters)
        .context("fetch delta files for partition filters")?;

    let ctx = SessionContext::new();
    if file_uris.is_empty() {
        return ctx.read_empty().context("create empty updates dataframe");
    }

    let read_options = ParquetReadOptions::default().parquet_pruning(false);
    let mut df = ctx.read_parquet(file_uris, read_options).await?;
    if std::env::var("POINTLINE_L2_DEBUG").is_ok() {
        println!("l2_updates schema: {:?}", df.schema());
    }

    df = df.filter(col("exchange_id").eq(lit(exchange_id)))?;
    df = df.filter(col("symbol_id").eq(lit(symbol_id)))?;
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
        col("file_id").sort(true, true),
        col("file_line_number").sort(true, true),
    ])?;

    Ok(df)
}

async fn build_checkpoint_updates_df(
    updates_path: &str,
    exchange: Option<&str>,
    exchange_id: Option<i16>,
    symbol_ids: Option<&[i64]>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    assume_sorted: bool,
) -> Result<DataFrame> {
    let table = open_table(updates_path).await?;
    let mut partition_filters = Vec::new();
    if let Some(exchange) = exchange {
        partition_filters.push(PartitionFilter {
            key: "exchange".to_string(),
            value: PartitionValue::Equal(exchange.to_string()),
        });
    }
    partition_filters.push(PartitionFilter {
        key: "date".to_string(),
        value: PartitionValue::GreaterThanOrEqual(start_date.to_string()),
    });
    partition_filters.push(PartitionFilter {
        key: "date".to_string(),
        value: PartitionValue::LessThanOrEqual(end_date.to_string()),
    });

    let file_uris = table
        .get_file_uris_by_partitions(&partition_filters)
        .context("fetch delta files for partition filters")?;

    let ctx = SessionContext::new();
    if file_uris.is_empty() {
        return ctx
            .read_empty()
            .context("create empty checkpoint updates dataframe");
    }
    let read_options = ParquetReadOptions::default().parquet_pruning(false);
    let mut df = ctx.read_parquet(file_uris, read_options).await?;
    let df_schema = df.schema().clone();
    if let Some(exchange_id) = exchange_id {
        let exchange_expr = col("exchange_id").cast_to(&DataType::Int64, &df_schema)?;
        df = df.filter(exchange_expr.eq(lit(i64::from(exchange_id))))?;
    }

    if let Some(symbol_ids) = symbol_ids {
        if symbol_ids.len() == 1 {
            let symbol_expr = col("symbol_id").cast_to(&DataType::Int64, &df_schema)?;
            df = df.filter(symbol_expr.eq(lit(symbol_ids[0])))?;
        } else if !symbol_ids.is_empty() {
            let values: Vec<Expr> = symbol_ids.iter().map(|id| lit(*id)).collect();
            let symbol_expr = col("symbol_id").cast_to(&DataType::Int64, &df_schema)?;
            df = df.filter(symbol_expr.in_list(values, false))?;
        }
    }

    df = df.select(vec![
        col("exchange_id"),
        col("symbol_id"),
        col("ts_local_us"),
        col("ingest_seq"),
        col("file_line_number"),
        col("is_snapshot"),
        col("side"),
        col("price_int"),
        col("size_int"),
        col("file_id"),
    ])?;
    if !assume_sorted {
        df = df.sort(vec![
            col("ts_local_us").sort(true, true),
            col("ingest_seq").sort(true, true),
            col("file_id").sort(true, true),
            col("file_line_number").sort(true, true),
        ])?;
    }

    Ok(df)
}

struct UpdateColumns<'a> {
    ts_local_us: &'a Int64Array,
    ingest_seq: &'a Int32Array,
    file_line_number: &'a Int32Array,
    is_snapshot: &'a BooleanArray,
    side: ArrayRef,
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
        side: batch.column(batch.schema().index_of("side")?).clone(),
        price_int: get_array(batch, "price_int")?,
        size_int: get_array(batch, "size_int")?,
        file_id: get_array(batch, "file_id")?,
    })
}

fn update_from_columns(cols: &UpdateColumns<'_>, row: usize) -> Result<L2Update> {
    Ok(L2Update {
        ts_local_us: cols.ts_local_us.value(row),
        ingest_seq: cols.ingest_seq.value(row),
        file_line_number: cols.file_line_number.value(row),
        is_snapshot: cols.is_snapshot.value(row),
        side: get_u8_value(&cols.side, row)?,
        price_int: cols.price_int.value(row),
        size_int: cols.size_int.value(row),
        file_id: cols.file_id.value(row),
    })
}

struct CheckpointUpdateColumns<'a> {
    exchange_id: &'a Int16Array,
    symbol_id: &'a Int64Array,
    ts_local_us: &'a Int64Array,
    ingest_seq: &'a Int32Array,
    file_line_number: &'a Int32Array,
    is_snapshot: &'a BooleanArray,
    side: ArrayRef,
    price_int: &'a Int64Array,
    size_int: &'a Int64Array,
    file_id: &'a Int32Array,
}

fn checkpoint_update_columns<'a>(batch: &'a RecordBatch) -> Result<CheckpointUpdateColumns<'a>> {
    Ok(CheckpointUpdateColumns {
        exchange_id: get_array(batch, "exchange_id")?,
        symbol_id: get_array(batch, "symbol_id")?,
        ts_local_us: get_array(batch, "ts_local_us")?,
        ingest_seq: get_array(batch, "ingest_seq")?,
        file_line_number: get_array(batch, "file_line_number")?,
        is_snapshot: get_array(batch, "is_snapshot")?,
        side: batch.column(batch.schema().index_of("side")?).clone(),
        price_int: get_array(batch, "price_int")?,
        size_int: get_array(batch, "size_int")?,
        file_id: get_array(batch, "file_id")?,
    })
}

fn checkpoint_update_from_columns(
    cols: &CheckpointUpdateColumns<'_>,
    row: usize,
) -> Result<(CheckpointMeta, L2Update)> {
    let meta = CheckpointMeta {
        exchange_id: cols.exchange_id.value(row),
        symbol_id: cols.symbol_id.value(row),
    };
    let update = L2Update {
        ts_local_us: cols.ts_local_us.value(row),
        ingest_seq: cols.ingest_seq.value(row),
        file_line_number: cols.file_line_number.value(row),
        is_snapshot: cols.is_snapshot.value(row),
        side: get_u8_value(&cols.side, row)?,
        price_int: cols.price_int.value(row),
        size_int: cols.size_int.value(row),
        file_id: cols.file_id.value(row),
    };
    Ok((meta, update))
}

async fn for_each_update<F>(mut stream: SendableRecordBatchStream, mut f: F) -> Result<()>
where
    F: FnMut(L2Update) -> Result<()>,
{
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let cols = update_columns(&batch)?;
        for row in 0..batch.num_rows() {
            f(update_from_columns(&cols, row)?)?;
        }
    }
    Ok(())
}

async fn for_each_checkpoint_update<F>(
    mut stream: SendableRecordBatchStream,
    mut f: F,
) -> Result<()>
where
    F: FnMut(CheckpointMeta, L2Update) -> Result<()>,
{
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let cols = checkpoint_update_columns(&batch)?;
        for row in 0..batch.num_rows() {
            let (meta, update) = checkpoint_update_from_columns(&cols, row)?;
            f(meta, update)?;
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

fn get_u8_value(array: &ArrayRef, row: usize) -> Result<u8> {
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        let value = values.value(row);
        if value < 0 {
            return Err(anyhow!("column side has negative value {}", value));
        }
        return Ok(value as u8);
    }
    Err(anyhow!("column side has unexpected type"))
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

fn append_levels(builder: &mut ListBuilder<StructBuilder>, levels: &[(i64, i64)]) {
    let struct_builder = builder.values();
    for (price, size) in levels {
        {
            let price_builder = struct_builder
                .field_builder::<Int64Builder>(0)
                .expect("price builder");
            price_builder.append_value(*price);
        }
        {
            let size_builder = struct_builder
                .field_builder::<Int64Builder>(1)
                .expect("size builder");
            size_builder.append_value(*size);
        }
        struct_builder.append(true);
    }
    builder.append(true);
}

fn build_checkpoint_batch(rows: &[CheckpointRow]) -> Result<RecordBatch> {
    let level_fields = vec![
        Field::new("price_int", DataType::Int64, false),
        Field::new("size_int", DataType::Int64, false),
    ];
    let level_struct = DataType::Struct(level_fields.clone().into());
    let list_field = Field::new("item", level_struct, true);

    let schema = Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("exchange_id", DataType::Int16, false),
        Field::new("symbol_id", DataType::Int64, false),
        Field::new("date", DataType::Date32, false),
        Field::new("ts_local_us", DataType::Int64, false),
        Field::new("bids", DataType::List(Arc::new(list_field.clone())), true),
        Field::new("asks", DataType::List(Arc::new(list_field)), true),
        Field::new("file_id", DataType::Int32, false),
        Field::new("ingest_seq", DataType::Int32, false),
        Field::new("file_line_number", DataType::Int32, false),
        Field::new("checkpoint_kind", DataType::Utf8, false),
    ]));

    let mut exchange_builder = StringBuilder::new();
    let mut exchange_id_builder = Int16Builder::new();
    let mut symbol_id_builder = Int64Builder::new();
    let mut date_builder = Date32Builder::new();
    let mut ts_builder = Int64Builder::new();

    let mut bids_builder = ListBuilder::new(StructBuilder::new(
        level_fields.clone(),
        vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
    ));
    let mut asks_builder = ListBuilder::new(StructBuilder::new(
        level_fields,
        vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
    ));

    let mut file_id_builder = Int32Builder::new();
    let mut ingest_seq_builder = Int32Builder::new();
    let mut file_line_builder = Int32Builder::new();
    let mut checkpoint_kind_builder = StringBuilder::new();

    for row in rows {
        exchange_builder.append_value(&row.exchange);
        exchange_id_builder.append_value(row.exchange_id);
        symbol_id_builder.append_value(row.symbol_id);
        date_builder.append_value(row.date);
        ts_builder.append_value(row.ts_local_us);
        append_levels(&mut bids_builder, &row.bids);
        append_levels(&mut asks_builder, &row.asks);
        file_id_builder.append_value(row.file_id);
        ingest_seq_builder.append_value(row.ingest_seq);
        file_line_builder.append_value(row.file_line_number);
        checkpoint_kind_builder.append_value(&row.checkpoint_kind);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(exchange_builder.finish()),
        Arc::new(exchange_id_builder.finish()),
        Arc::new(symbol_id_builder.finish()),
        Arc::new(date_builder.finish()),
        Arc::new(ts_builder.finish()),
        Arc::new(bids_builder.finish()),
        Arc::new(asks_builder.finish()),
        Arc::new(file_id_builder.finish()),
        Arc::new(ingest_seq_builder.finish()),
        Arc::new(file_line_builder.finish()),
        Arc::new(checkpoint_kind_builder.finish()),
    ];

    RecordBatch::try_new(schema, arrays).map_err(|err| anyhow!(err.to_string()))
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
    let start_date = start_override.unwrap_or(fallback_date);
    let end_date = end_override.unwrap_or(fallback_date);
    if start_date > end_date {
        return Err(anyhow!(
            "snapshot_at: start_date {} is after end_date {}",
            start_date,
            end_date
        ));
    }

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
    for_each_update(stream, |update| {
        reset.apply(&mut book, &update);
        Ok(())
    })
    .await?;

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
            return Ok(());
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
        Ok(())
    })
    .await?;

    Ok(snapshots)
}

pub async fn build_state_checkpoints_delta(
    updates_path: &str,
    output_path: &str,
    exchange: Option<&str>,
    exchange_id: Option<i16>,
    symbol_ids: Option<Vec<i64>>,
    start_date: &str,
    end_date: &str,
    checkpoint_every_us: Option<i64>,
    checkpoint_every_updates: Option<u64>,
    validate_monotonic: bool,
    assume_sorted: bool,
) -> Result<usize> {
    let every_us = checkpoint_every_us.filter(|value| *value > 0);
    let every_updates = checkpoint_every_updates.filter(|value| *value > 0);
    if every_us.is_none() && every_updates.is_none() {
        return Err(anyhow!(
            "build_state_checkpoints: at least one cadence must be set"
        ));
    }

    let start_date =
        NaiveDate::parse_from_str(start_date, "%Y-%m-%d").with_context(|| {
            format!("invalid start_date string: {}", start_date)
        })?;
    let end_date = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
        .with_context(|| format!("invalid end_date string: {}", end_date))?;

    let exchange = exchange
        .ok_or_else(|| anyhow!("build_state_checkpoints: exchange is required"))?;
    let exchange = exchange.to_string();

    let start_ts = date_to_ts_local_us(start_date, false);
    let end_ts = date_to_ts_local_us(end_date, true);

    let df = build_checkpoint_updates_df(
        updates_path,
        Some(exchange.as_str()),
        exchange_id,
        symbol_ids.as_deref(),
        start_date,
        end_date,
        assume_sorted,
    )
    .await
    .context("build checkpoint updates dataframe")?;

    let mut rows: Vec<CheckpointRow> = Vec::new();
    let mut book = OrderBook::default();
    let mut reset = SnapshotReset::default();
    let mut last_checkpoint_ts: Option<i64> = None;
    let mut updates_since: u64 = 0;
    let mut prev_key: Option<(i64, i32, i32, i32)> = None;

    let stream = df
        .execute_stream()
        .await
        .context("execute checkpoint updates stream")?;
    for_each_checkpoint_update(stream, |meta, update| {
        if update.ts_local_us < start_ts || update.ts_local_us > end_ts {
            return Ok(());
        }

        if validate_monotonic {
            let key = (
                update.ts_local_us,
                update.ingest_seq,
                update.file_id,
                update.file_line_number,
            );
            if let Some(prev) = prev_key {
                if key < prev {
                    panic!(
                        "build_state_checkpoints: updates out of order: {:?} < {:?}",
                        key, prev
                    );
                }
            }
            prev_key = Some(key);
        }

        reset.apply(&mut book, &update);

        if last_checkpoint_ts.is_none() {
            last_checkpoint_ts = Some(update.ts_local_us);
        }

        updates_since = updates_since.saturating_add(1);

        let mut emit = false;
        if let (Some(every_us), Some(last_ts)) = (every_us, last_checkpoint_ts) {
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
            let date = ts_to_date(update.ts_local_us)?;
            let date_days = date_to_days(date);
            let (bids, asks) = book_levels(&book);
            rows.push(CheckpointRow {
                exchange: exchange.clone(),
                exchange_id: meta.exchange_id,
                symbol_id: meta.symbol_id,
                date: date_days,
                ts_local_us: update.ts_local_us,
                bids,
                asks,
                file_id: update.file_id,
                ingest_seq: update.ingest_seq,
                file_line_number: update.file_line_number,
                checkpoint_kind: "periodic".to_string(),
            });
            updates_since = 0;
            last_checkpoint_ts = Some(update.ts_local_us);
        }
        Ok(())
    })
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let batch = build_checkpoint_batch(&rows).context("build checkpoint record batch")?;

    let mut table = if delta_table_exists(output_path) {
        Some(open_table(output_path).await?)
    } else {
        None
    };

    if let Some(current) = table.take() {
        let mut current = current;
        let mut partitions: HashSet<(String, i32)> = HashSet::new();
        for row in &rows {
            partitions.insert((row.exchange.clone(), row.date));
        }

        for (exchange, date) in partitions {
            let date = days_to_date(date)?;
            let predicate = format!(
                "exchange = '{}' AND date = '{}'",
                escape_sql_string(&exchange),
                date.format("%Y-%m-%d")
            );
            let (next, _) = DeltaOps::from(current)
                .delete()
                .with_predicate(predicate)
                .await
                .context("delete existing checkpoint partitions")?;
            current = next;
        }
        table = Some(current);
    }

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).unwrap_or_else(|_| ZstdLevel::default()),
        ))
        .build();

    let ops = if let Some(table) = table {
        DeltaOps::from(table)
    } else {
        DeltaOps::try_from_uri(output_path).await?
    };

    ops.write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .with_partition_columns(["exchange", "date"])
        .with_writer_properties(writer_properties)
        .await
        .context("write checkpoint batch")?;

    Ok(rows.len())
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
    let mut prev_key: Option<(i64, i32, i32, i32)> = None;

    let mut snapshot_key: Option<(i64, i32)> = None; // (ts_local_us, file_id)
    let mut snapshot_pos: Option<StreamPos> = None;

    let mut last_checkpoint_ts: Option<i64> = None;
    let mut updates_since: u64 = 0;

    for update in updates {
        let current_key = (
            update.ts_local_us,
            update.ingest_seq,
            update.file_id,
            update.file_line_number,
        );
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
    use super::{
        build_state_checkpoints_delta, replay_between_delta, snapshot_at_delta, L2Update,
        OrderBook, Snapshot, SnapshotWithPos,
    };
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    use pyo3::wrap_pyfunction;
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

    #[pyfunction(
        signature = (
            updates_path,
            exchange_id,
            symbol_id,
            ts_local_us,
            *,
            checkpoint_path=None,
            exchange=None,
            start_date=None,
            end_date=None
        )
    )]
    fn snapshot_at(
        py: Python<'_>,
        updates_path: String,
        exchange_id: i16,
        symbol_id: i64,
        ts_local_us: i64,
        checkpoint_path: Option<String>,
        exchange: Option<String>,
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
            Err(err) => Err(PyRuntimeError::new_err(format!("{err:?}"))),
        }
    }

    #[pyfunction(
        signature = (
            updates_path,
            exchange_id,
            symbol_id,
            start_ts_local_us,
            end_ts_local_us,
            *,
            checkpoint_path=None,
            exchange=None,
            every_us=None,
            every_updates=None
        )
    )]
    fn replay_between(
        py: Python<'_>,
        updates_path: String,
        exchange_id: i16,
        symbol_id: i64,
        start_ts_local_us: i64,
        end_ts_local_us: i64,
        checkpoint_path: Option<String>,
        exchange: Option<String>,
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
            Err(err) => Err(PyRuntimeError::new_err(format!("{err:?}"))),
        }
    }

    #[derive(FromPyObject)]
    enum SymbolIdArg {
        One(i64),
        Many(Vec<i64>),
    }

    fn normalize_symbol_ids(symbol_id: Option<SymbolIdArg>) -> Option<Vec<i64>> {
        match symbol_id {
            Some(SymbolIdArg::One(id)) => Some(vec![id]),
            Some(SymbolIdArg::Many(ids)) => Some(ids),
            None => None,
        }
    }

    #[pyfunction(
        signature = (
            updates_path,
            output_path,
            start_date,
            end_date,
            *,
            exchange=None,
            exchange_id=None,
            symbol_id=None,
            checkpoint_every_us=None,
            checkpoint_every_updates=None,
            validate_monotonic=false,
            assume_sorted=false
        )
    )]
    fn build_state_checkpoints(
        py: Python<'_>,
        updates_path: String,
        output_path: String,
        start_date: String,
        end_date: String,
        exchange: Option<String>,
        exchange_id: Option<i16>,
        symbol_id: Option<SymbolIdArg>,
        checkpoint_every_us: Option<i64>,
        checkpoint_every_updates: Option<u64>,
        validate_monotonic: bool,
        assume_sorted: bool,
    ) -> PyResult<usize> {
        let symbol_ids = normalize_symbol_ids(symbol_id);
        let result = py.allow_threads(|| {
            runtime().block_on(build_state_checkpoints_delta(
                &updates_path,
                &output_path,
                exchange.as_deref(),
                exchange_id,
                symbol_ids,
                &start_date,
                &end_date,
                checkpoint_every_us,
                checkpoint_every_updates,
                validate_monotonic,
                assume_sorted,
            ))
        });

        match result {
            Ok(count) => Ok(count),
            Err(err) => Err(PyRuntimeError::new_err(format!("{err:?}"))),
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
        module.add_function(wrap_pyfunction!(build_state_checkpoints, module)?)?;
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
