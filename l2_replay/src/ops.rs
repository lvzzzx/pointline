use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use deltalake::arrow::array::ArrayRef;
use deltalake::arrow::array::{Int16Builder, Int32Builder, Int64Builder, ListBuilder, StructBuilder};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::open_table;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use futures::StreamExt;

use crate::arrow_utils::{
    append_levels, book_levels, build_checkpoint_batch, checkpoint_update_columns,
    checkpoint_update_from_columns, update_columns, update_from_columns,
};
use crate::io::{
    build_checkpoint_updates_df, build_updates_df, delta_table_exists, for_each_update,
    latest_checkpoint,
};
use crate::replay::{CadenceState, SnapshotReset};
use crate::types::{CheckpointMeta, CheckpointRow, OrderBook, Snapshot, StreamPos};
use crate::utils::{date_to_days, date_to_ts_local_us, days_to_date, escape_sql_string, parse_date_opt, ts_to_date};

fn timing_enabled() -> bool {
    std::env::var("POINTLINE_L2_TIMING").is_ok()
}

fn log_timing(label: &str, start: Instant) {
    if timing_enabled() {
        eprintln!("timing/replay {}: {:?}", label, start.elapsed());
    }
}

fn build_order_book(
    dense_price_min: Option<i64>,
    dense_price_max: Option<i64>,
    dense_tick_size: Option<i64>,
) -> Result<OrderBook> {
    match (dense_price_min, dense_price_max, dense_tick_size) {
        (Some(min), Some(max), Some(tick)) => OrderBook::new_dense(min, max, tick)
            .map_err(|err| anyhow!(err)),
        (None, None, None) => Ok(OrderBook::default()),
        _ => Err(anyhow!(
            "dense_price_min, dense_price_max, dense_tick_size must be all set or all None"
        )),
    }
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
        book.seed_from_levels(&checkpoint.bids, &checkpoint.asks);
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
    dense_price_min: Option<i64>,
    dense_price_max: Option<i64>,
    dense_tick_size: Option<i64>,
) -> Result<RecordBatch> {
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

    let mut book =
        build_order_book(dense_price_min, dense_price_max, dense_tick_size)?;
    let mut min_pos = None;
    if let Some(checkpoint) = &checkpoint {
        book.seed_from_levels(&checkpoint.bids, &checkpoint.asks);
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

    // Builder Setup
    let level_fields = vec![
        Field::new("price_int", DataType::Int64, false),
        Field::new("size_int", DataType::Int64, false),
    ];
    let level_struct = DataType::Struct(level_fields.clone().into());
    let list_field = Field::new("item", level_struct, true);

    let schema = Arc::new(Schema::new(vec![
        Field::new("exchange_id", DataType::Int16, false),
        Field::new("symbol_id", DataType::Int64, false),
        Field::new("ts_local_us", DataType::Int64, false),
        Field::new("ingest_seq", DataType::Int32, false),
        Field::new("file_line_number", DataType::Int32, false),
        Field::new("file_id", DataType::Int32, false),
        Field::new("bids", DataType::List(Arc::new(list_field.clone())), true),
        Field::new("asks", DataType::List(Arc::new(list_field)), true),
    ]));

    let mut exchange_id_builder = Int16Builder::new();
    let mut symbol_id_builder = Int64Builder::new();
    let mut ts_builder = Int64Builder::new();
    let mut ingest_seq_builder = Int32Builder::new();
    let mut file_line_builder = Int32Builder::new();
    let mut file_id_builder = Int32Builder::new();

    let mut bids_builder = ListBuilder::new(StructBuilder::new(
        level_fields.clone(),
        vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
    ));
    let mut asks_builder = ListBuilder::new(StructBuilder::new(
        level_fields,
        vec![Box::new(Int64Builder::new()), Box::new(Int64Builder::new())],
    ));

    let mut reset = SnapshotReset::default();
    let mut cadence = CadenceState::default();

    // State for atomic processing
    let mut last_pos: Option<StreamPos> = None;

    let total_start = Instant::now();
    let timing = timing_enabled();
    let mut decode_time = Duration::from_secs(0);
    let mut apply_time = Duration::from_secs(0);
    let mut emit_time = Duration::from_secs(0);
    let t_stream = Instant::now();
    let mut stream = df.execute_stream().await?;
    log_timing("replay_between execute_stream", t_stream);
    let mut batches: u64 = 0;
    let mut rows: u64 = 0;

    while let Some(batch) = {
        let t_next = Instant::now();
        let next = stream.next().await;
        log_timing("replay_between stream_next", t_next);
        next
    } {
        let batch = batch?;
        let num_rows = batch.num_rows();
        let t_cols = Instant::now();
        let cols = update_columns(&batch)?;
        log_timing("replay_between update_columns", t_cols);
        let t_rows = Instant::now();
        for row in 0..batch.num_rows() {
            let update = if timing {
                let t_decode = Instant::now();
                let update = update_from_columns(&cols, row)?;
                decode_time += t_decode.elapsed();
                update
            } else {
                update_from_columns(&cols, row)?
            };

            // Check for timestamp boundary
            if let Some(pos) = last_pos {
                if update.ts_local_us != pos.ts_local_us {
                    // Boundary crossed: evaluate emission for the completed timestamp group (pos)
                    if pos.ts_local_us >= start_ts_local_us {
                        let emit =
                            cadence.should_emit(pos.ts_local_us, every_us, every_updates);
                        if emit {
                            let t_emit = Instant::now();
                            let (bids, asks) = book_levels(&book);

                            // Append to builders
                            exchange_id_builder.append_value(exchange_id);
                            symbol_id_builder.append_value(symbol_id);
                            ts_builder.append_value(pos.ts_local_us);
                            ingest_seq_builder.append_value(pos.ingest_seq);
                            file_line_builder.append_value(pos.file_line_number);
                            file_id_builder.append_value(pos.file_id);
                            append_levels(&mut bids_builder, &bids);
                            append_levels(&mut asks_builder, &asks);
                            if timing {
                                emit_time += t_emit.elapsed();
                            }
                        }
                    }
                }
            }

            // Apply current update
            if timing {
                let t_apply = Instant::now();
                reset.apply(&mut book, &update);
                apply_time += t_apply.elapsed();
            } else {
                reset.apply(&mut book, &update);
            }

            cadence.record_update(update.ts_local_us >= start_ts_local_us);

            last_pos = Some(StreamPos {
                ts_local_us: update.ts_local_us,
                ingest_seq: update.ingest_seq,
                file_line_number: update.file_line_number,
                file_id: update.file_id,
            });
        }
        log_timing("replay_between process_rows(batch)", t_rows);
        batches = batches.saturating_add(1);
        rows = rows.saturating_add(num_rows as u64);
    }

    // Handle final group after stream ends
    if let Some(pos) = last_pos {
        if pos.ts_local_us >= start_ts_local_us {
            let emit = cadence.should_emit(pos.ts_local_us, every_us, every_updates);
            if emit {
                let t_emit = Instant::now();
                let (bids, asks) = book_levels(&book);

                // Append to builders
                exchange_id_builder.append_value(exchange_id);
                symbol_id_builder.append_value(symbol_id);
                ts_builder.append_value(pos.ts_local_us);
                ingest_seq_builder.append_value(pos.ingest_seq);
                file_line_builder.append_value(pos.file_line_number);
                file_id_builder.append_value(pos.file_id);
                append_levels(&mut bids_builder, &bids);
                append_levels(&mut asks_builder, &asks);
                if timing {
                    emit_time += t_emit.elapsed();
                }
            }
        }
    }
    if timing_enabled() {
        eprintln!(
            "timing/replay replay_between done batches={} rows={} total={:?}",
            batches,
            rows,
            total_start.elapsed()
        );
        if rows > 0 {
            let rows_f = rows as f64;
            eprintln!(
                "timing/replay replay_between breakdown decode={:?} ({:.3} ns/row) apply={:?} ({:.3} ns/row) emit={:?}",
                decode_time,
                (decode_time.as_secs_f64() * 1e9) / rows_f,
                apply_time,
                (apply_time.as_secs_f64() * 1e9) / rows_f,
                emit_time
            );
        } else {
            eprintln!(
                "timing/replay replay_between breakdown decode={:?} apply={:?} emit={:?}",
                decode_time, apply_time, emit_time
            );
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(exchange_id_builder.finish()),
        Arc::new(symbol_id_builder.finish()),
        Arc::new(ts_builder.finish()),
        Arc::new(ingest_seq_builder.finish()),
        Arc::new(file_line_builder.finish()),
        Arc::new(file_id_builder.finish()),
        Arc::new(bids_builder.finish()),
        Arc::new(asks_builder.finish()),
    ];

    RecordBatch::try_new(schema, arrays).map_err(|err| anyhow!(err.to_string()))
}

pub async fn build_state_checkpoints_delta(
    updates_path: &str,
    output_path: &str,
    exchange: Option<&str>,
    exchange_id: Option<i16>,
    symbol_id: i64,
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
        symbol_id,
        start_date,
        end_date,
        assume_sorted,
    )
    .await
    .context("build checkpoint updates dataframe")?;

    let mut rows: Vec<CheckpointRow> = Vec::new();
    let mut book = OrderBook::default();
    let mut reset = SnapshotReset::default();
    let mut cadence = CadenceState::default();
    let mut prev_key: Option<(i64, i32, i32, i32)> = None;

    // State for atomic processing
    let mut last_pos: Option<StreamPos> = None;
    let mut last_meta: Option<CheckpointMeta> = None;

    let mut stream = df
        .execute_stream()
        .await
        .context("execute checkpoint updates stream")?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let cols = checkpoint_update_columns(&batch)?;
        for row in 0..batch.num_rows() {
            let (meta, update) = checkpoint_update_from_columns(&cols, row)?;

            if update.ts_local_us < start_ts || update.ts_local_us > end_ts {
                continue;
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

            // Check for timestamp boundary
            if let Some(pos) = last_pos {
                if update.ts_local_us != pos.ts_local_us {
                    // Boundary crossed. Evaluate emission for 'pos'
                    let emit =
                        cadence.should_emit(pos.ts_local_us, every_us, every_updates);
                    if emit {
                        let date = ts_to_date(pos.ts_local_us)?;
                        let date_days = date_to_days(date);
                        let (bids, asks) = book_levels(&book);

                        let meta_ref = last_meta.as_ref().expect("last_meta missing");

                        rows.push(CheckpointRow {
                            exchange: exchange.clone(),
                            exchange_id: meta_ref.exchange_id,
                            symbol_id: meta_ref.symbol_id,
                            date: date_days,
                            ts_local_us: pos.ts_local_us,
                            bids,
                            asks,
                            file_id: pos.file_id,
                            ingest_seq: pos.ingest_seq,
                            file_line_number: pos.file_line_number,
                            checkpoint_kind: "periodic".to_string(),
                        });
                    }
                }
            }

            reset.apply(&mut book, &update);

            cadence.record_update(true);

            last_pos = Some(StreamPos {
                ts_local_us: update.ts_local_us,
                ingest_seq: update.ingest_seq,
                file_line_number: update.file_line_number,
                file_id: update.file_id,
            });
            last_meta = Some(meta);
        }
    }

    // Handle final group
    if let Some(pos) = last_pos {
        let emit = cadence.should_emit(pos.ts_local_us, every_us, every_updates);
        if emit {
            let date = ts_to_date(pos.ts_local_us)?;
            let date_days = date_to_days(date);
            let (bids, asks) = book_levels(&book);
            let meta_ref = last_meta.as_ref().expect("last_meta missing");

            rows.push(CheckpointRow {
                exchange: exchange.clone(),
                exchange_id: meta_ref.exchange_id,
                symbol_id: meta_ref.symbol_id,
                date: date_days,
                ts_local_us: pos.ts_local_us,
                bids,
                asks,
                file_id: pos.file_id,
                ingest_seq: pos.ingest_seq,
                file_line_number: pos.file_line_number,
                checkpoint_kind: "periodic".to_string(),
            });
        }
    }

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
        let mut partitions: HashSet<(String, i32, i64)> = HashSet::new();
        for row in &rows {
            partitions.insert((row.exchange.clone(), row.date, row.symbol_id));
        }

        for (exchange, date, symbol_id) in partitions {
            let date = days_to_date(date)?;
            let predicate = format!(
                "exchange = '{}' AND date = '{}' AND symbol_id = {}",
                escape_sql_string(&exchange),
                date.format("%Y-%m-%d"),
                symbol_id
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
