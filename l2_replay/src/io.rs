use std::path::Path;
use std::collections::HashSet;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use deltalake::arrow::datatypes::DataType;
use deltalake::datafusion::execution::context::SessionConfig;
use deltalake::datafusion::logical_expr::ExprSchemable;
use deltalake::datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::datafusion::prelude::*;
use deltalake::open_table;
use deltalake::schema::partitions::{PartitionFilter, PartitionValue};
use futures::StreamExt;

use crate::arrow_utils::{get_i32, get_i64, get_levels, update_columns, update_from_columns};
use crate::types::{Checkpoint, L2Update, StreamPos};
use crate::utils::ts_to_date;

fn timing_enabled() -> bool {
    std::env::var("POINTLINE_L2_TIMING").is_ok()
}

fn log_timing(label: &str, start: std::time::Instant) {
    if timing_enabled() {
        eprintln!("timing/io {}: {:?}", label, start.elapsed());
    }
}

pub fn delta_table_exists(path: &str) -> bool {
    Path::new(path).join("_delta_log").exists()
}

pub fn parquet_read_session_config() -> SessionConfig {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_repartition_file_scans(false)
        .with_repartition_sorts(false)
        .with_prefer_existing_sort(true)
        .with_parquet_pruning(true)
        .with_parquet_bloom_filter_pruning(true);
    // Parquet page index metadata can be corrupted in some datasets; disable to avoid read errors.
    config.options_mut().execution.parquet.enable_page_index = false;
    config
}

pub fn after_pos_expr(pos: &StreamPos) -> Expr {
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

pub async fn latest_checkpoint(
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
    let t0 = std::time::Instant::now();
    let table = open_table(checkpoint_path).await?;
    log_timing("latest_checkpoint open_table", t0);
    let t1 = std::time::Instant::now();
    let metadata = table.metadata()?;
    log_timing("latest_checkpoint metadata", t1);
    let partition_cols: HashSet<&String> = metadata.partition_columns.iter().collect();

    let mut partition_filters = Vec::new();
    if let Some(exchange) = exchange {
        if partition_cols.contains(&"exchange".to_string()) {
            partition_filters.push(PartitionFilter {
                key: "exchange".to_string(),
                value: PartitionValue::Equal(exchange.to_string()),
            });
        }
    }
    if partition_cols.contains(&"date".to_string()) {
        partition_filters.push(PartitionFilter {
            key: "date".to_string(),
            value: PartitionValue::Equal(target_date.to_string()),
        });
    }

    let t2 = std::time::Instant::now();
    let file_uris = table
        .get_file_uris_by_partitions(&partition_filters)
        .context("fetch checkpoint files for partition filters")?;
    log_timing("latest_checkpoint get_file_uris", t2);
    if file_uris.is_empty() {
        return Ok(None);
    }

    let ctx = SessionContext::new_with_config(parquet_read_session_config());
    let read_options = ParquetReadOptions::default().parquet_pruning(true);
    let t3 = std::time::Instant::now();
    let mut df = ctx.read_parquet(file_uris, read_options).await?;
    log_timing("latest_checkpoint read_parquet_df", t3);
    let df_schema = df.schema().clone();
    if let Some(exchange) = exchange {
        if df_schema.field_with_unqualified_name("exchange").is_ok() {
            let exchange_expr = col("exchange").cast_to(&DataType::Utf8, &df_schema)?;
            df = df.filter(exchange_expr.eq(lit(exchange)))?;
        }
    }
    if df_schema.field_with_unqualified_name("date").is_ok() {
        let date_expr = col("date").cast_to(&DataType::Utf8, &df_schema)?;
        df = df.filter(date_expr.eq(lit(target_date.to_string())))?;
    }
    df = df.filter(col("exchange_id").eq(lit(exchange_id)))?;
    df = df.filter(col("symbol_id").eq(lit(symbol_id)))?;
    df = df.filter(col("ts_local_us").lt_eq(lit(ts_local_us)))?;

    let t4 = std::time::Instant::now();
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
    log_timing("latest_checkpoint plan_filters_select", t4);

    let t5 = std::time::Instant::now();
    let batches = df.collect().await?;
    log_timing("latest_checkpoint collect", t5);
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

pub async fn build_updates_df(
    updates_path: &str,
    exchange: Option<&str>,
    exchange_id: i16,
    symbol_id: i64,
    start_date: NaiveDate,
    end_date: NaiveDate,
    max_ts_inclusive: i64,
    min_pos_exclusive: Option<StreamPos>,
) -> Result<DataFrame> {
    let t0 = std::time::Instant::now();
    let table = open_table(updates_path).await?;
    log_timing("build_updates_df open_table", t0);
    let t1 = std::time::Instant::now();
    let metadata = table.metadata()?;
    log_timing("build_updates_df metadata", t1);
    let partition_cols: HashSet<&String> = metadata.partition_columns.iter().collect();

    let mut partition_filters = Vec::new();
    if let Some(exchange) = exchange {
        if partition_cols.contains(&"exchange".to_string()) {
            partition_filters.push(PartitionFilter {
                key: "exchange".to_string(),
                value: PartitionValue::Equal(exchange.to_string()),
            });
        }
    }
    if partition_cols.contains(&"date".to_string()) {
        partition_filters.push(PartitionFilter {
            key: "date".to_string(),
            value: PartitionValue::GreaterThanOrEqual(start_date.to_string()),
        });
        partition_filters.push(PartitionFilter {
            key: "date".to_string(),
            value: PartitionValue::LessThanOrEqual(end_date.to_string()),
        });
    }
    if partition_cols.contains(&"symbol_id".to_string()) {
        partition_filters.push(PartitionFilter {
            key: "symbol_id".to_string(),
            value: PartitionValue::Equal(symbol_id.to_string()),
        });
    }

    let t2 = std::time::Instant::now();
    let file_uris = table
        .get_file_uris_by_partitions(&partition_filters)
        .context("fetch delta files for partition filters")?;
    log_timing("build_updates_df get_file_uris", t2);

    let ctx = SessionContext::new_with_config(parquet_read_session_config());
    if file_uris.is_empty() {
        return ctx.read_empty().context("create empty updates dataframe");
    }

    let read_options = ParquetReadOptions::default().parquet_pruning(true);
    let t3 = std::time::Instant::now();
    let mut df = ctx.read_parquet(file_uris, read_options).await?;
    log_timing("build_updates_df read_parquet_df", t3);
    let df_schema = df.schema().clone();
    if let Some(exchange) = exchange {
        if df_schema.field_with_unqualified_name("exchange").is_ok() {
            let exchange_expr = col("exchange").cast_to(&DataType::Utf8, &df_schema)?;
            df = df.filter(exchange_expr.eq(lit(exchange)))?;
        }
    }
    if df_schema.field_with_unqualified_name("date").is_ok() {
        let date_expr = col("date").cast_to(&DataType::Utf8, &df_schema)?;
        df = df.filter(date_expr.clone().gt_eq(lit(start_date.to_string())))?;
        df = df.filter(date_expr.lt_eq(lit(end_date.to_string())))?;
    }
    if std::env::var("POINTLINE_L2_DEBUG").is_ok() {
        println!("l2_updates schema: {:?}", df.schema());
    }

    let t4 = std::time::Instant::now();
    df = df.filter(col("exchange_id").eq(lit(exchange_id)))?;
    // symbol_id is a partition column, not stored in Parquet files
    // Partition filtering above already ensures we only read files for this symbol_id
    if df_schema.field_with_unqualified_name("symbol_id").is_ok() {
        df = df.filter(col("symbol_id").eq(lit(symbol_id)))?;
    }
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
    log_timing("build_updates_df plan_filters_select", t4);
    Ok(df)
}

pub async fn build_checkpoint_updates_df(
    updates_path: &str,
    exchange: Option<&str>,
    exchange_id: Option<i16>,
    symbol_id: i64,
    start_date: NaiveDate,
    end_date: NaiveDate,
    assume_sorted: bool,
) -> Result<DataFrame> {
    let t0 = std::time::Instant::now();
    let table = open_table(updates_path).await?;
    log_timing("build_checkpoint_updates_df open_table", t0);
    let t1 = std::time::Instant::now();
    let metadata = table.metadata()?;
    log_timing("build_checkpoint_updates_df metadata", t1);
    let partition_cols: HashSet<&String> = metadata.partition_columns.iter().collect();

    let mut partition_filters = Vec::new();
    if let Some(exchange) = exchange {
        if partition_cols.contains(&"exchange".to_string()) {
            partition_filters.push(PartitionFilter {
                key: "exchange".to_string(),
                value: PartitionValue::Equal(exchange.to_string()),
            });
        }
    }
    if partition_cols.contains(&"date".to_string()) {
        partition_filters.push(PartitionFilter {
            key: "date".to_string(),
            value: PartitionValue::GreaterThanOrEqual(start_date.to_string()),
        });
        partition_filters.push(PartitionFilter {
            key: "date".to_string(),
            value: PartitionValue::LessThanOrEqual(end_date.to_string()),
        });
    }
    if partition_cols.contains(&"symbol_id".to_string()) {
        partition_filters.push(PartitionFilter {
            key: "symbol_id".to_string(),
            value: PartitionValue::Equal(symbol_id.to_string()),
        });
    }

    let t2 = std::time::Instant::now();
    let file_uris = table
        .get_file_uris_by_partitions(&partition_filters)
        .context("fetch delta files for partition filters")?;
    log_timing("build_checkpoint_updates_df get_file_uris", t2);

    let ctx = SessionContext::new_with_config(parquet_read_session_config());
    if file_uris.is_empty() {
        return ctx
            .read_empty()
            .context("create empty checkpoint updates dataframe");
    }
    let read_options = ParquetReadOptions::default().parquet_pruning(true);
    let t3 = std::time::Instant::now();
    let mut df = ctx.read_parquet(file_uris, read_options).await?;
    log_timing("build_checkpoint_updates_df read_parquet_df", t3);
    let df_schema = df.schema().clone();
    if let Some(exchange) = exchange {
        if df_schema.field_with_unqualified_name("exchange").is_ok() {
            let exchange_expr = col("exchange").cast_to(&DataType::Utf8, &df_schema)?;
            df = df.filter(exchange_expr.eq(lit(exchange)))?;
        }
    }
    if df_schema.field_with_unqualified_name("date").is_ok() {
        let date_expr = col("date").cast_to(&DataType::Utf8, &df_schema)?;
        df = df.filter(date_expr.clone().gt_eq(lit(start_date.to_string())))?;
        df = df.filter(date_expr.lt_eq(lit(end_date.to_string())))?;
    }
    if let Some(exchange_id) = exchange_id {
        let exchange_expr = col("exchange_id").cast_to(&DataType::Int64, &df_schema)?;
        df = df.filter(exchange_expr.eq(lit(i64::from(exchange_id))))?;
    }
    let symbol_in_schema = df_schema.field_with_unqualified_name("symbol_id").is_ok();
    // symbol_id is a partition column in some tables, so it may not be stored in Parquet files
    if symbol_in_schema {
        let symbol_expr = col("symbol_id").cast_to(&DataType::Int64, &df_schema)?;
        df = df.filter(symbol_expr.eq(lit(symbol_id)))?;
    }

    let t4 = std::time::Instant::now();
    df = df.select(vec![
        col("exchange_id"),
        if symbol_in_schema {
            col("symbol_id")
        } else {
            lit(symbol_id).alias("symbol_id")
        },
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
    log_timing("build_checkpoint_updates_df plan_filters_select", t4);

    Ok(df)
}

pub async fn for_each_update<F>(mut stream: SendableRecordBatchStream, mut f: F) -> Result<()>
where
    F: FnMut(L2Update) -> Result<()>,
{
    let mut batches: u64 = 0;
    let mut rows: u64 = 0;
    while let Some(batch) = stream.next().await {
        let t0 = std::time::Instant::now();
        let batch = batch?;
        log_timing("for_each_update stream_next", t0);
        let t1 = std::time::Instant::now();
        let cols = update_columns(&batch)?;
        log_timing("for_each_update update_columns", t1);
        let t2 = std::time::Instant::now();
        for row in 0..batch.num_rows() {
            f(update_from_columns(&cols, row)?)?;
        }
        log_timing("for_each_update update_from_columns(batch)", t2);
        batches = batches.saturating_add(1);
        rows = rows.saturating_add(batch.num_rows() as u64);
    }
    if timing_enabled() {
        eprintln!("timing/io for_each_update done batches={} rows={}", batches, rows);
    }
    Ok(())
}
