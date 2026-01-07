use std::sync::Arc;

use anyhow::Result;
use chrono::NaiveDate;
use deltalake::arrow::array::{
    Array, BooleanArray, Date32Array, Int16Array, Int32Array, Int64Array, ListArray, StringArray,
    UInt8Array,
};
use deltalake::arrow::compute::{cast, concat_batches};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use deltalake::open_table;
use deltalake::datafusion::prelude::SessionContext;
use tempfile::tempdir;

use l2_replay::build_state_checkpoints_delta;

fn date32_days(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    (date - epoch).num_days() as i32
}

fn date_start_ts(date: NaiveDate) -> i64 {
    date.and_hms_micro_opt(0, 0, 0, 0)
        .expect("valid date time")
        .timestamp_micros()
}

async fn write_updates_table(path: &str) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("exchange_id", DataType::Int16, false),
        Field::new("symbol_id", DataType::Int64, false),
        Field::new("date", DataType::Date32, false),
        Field::new("ts_local_us", DataType::Int64, false),
        Field::new("ingest_seq", DataType::Int32, false),
        Field::new("file_line_number", DataType::Int32, false),
        Field::new("is_snapshot", DataType::Boolean, false),
        Field::new("side", DataType::UInt8, false),
        Field::new("price_int", DataType::Int64, false),
        Field::new("size_int", DataType::Int64, false),
        Field::new("file_id", DataType::Int32, false),
    ]));

    let base_date = NaiveDate::from_ymd_opt(2025, 1, 1).expect("date");
    let date = date32_days(base_date);
    let base_ts = date_start_ts(base_date);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["deribit"; 5])),
            Arc::new(Int16Array::from(vec![21; 5])),
            Arc::new(Int64Array::from(vec![1; 5])),
            Arc::new(Date32Array::from(vec![date; 5])),
            Arc::new(Int64Array::from(vec![
                base_ts + 1,
                base_ts + 1,
                base_ts + 2,
                base_ts + 3,
                base_ts + 4,
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(BooleanArray::from(vec![true, true, false, false, false])),
            Arc::new(UInt8Array::from(vec![0, 1, 0, 1, 0])),
            Arc::new(Int64Array::from(vec![100, 101, 100, 102, 99])),
            Arc::new(Int64Array::from(vec![10, 5, 0, 3, 7])),
            Arc::new(Int32Array::from(vec![10; 5])),
        ],
    )?;

    DeltaOps::try_from_uri(path)
        .await?
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .with_partition_columns(["exchange", "date"])
        .await?;

    Ok(())
}

#[test]
fn test_build_state_checkpoints_delta_end_to_end() -> Result<()> {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let dir = tempdir()?;
        let updates_path = dir.path().join("silver.l2_updates");
        let output_path = dir.path().join("gold.l2_state_checkpoint");

        write_updates_table(
            updates_path
                .to_str()
                .expect("updates path"),
        )
        .await?;

        let rows_written = build_state_checkpoints_delta(
            updates_path.to_str().expect("updates path"),
            output_path.to_str().expect("output path"),
            Some("deribit"),
            Some(21),
            Some(vec![1]),
            "2025-01-01",
            "2025-01-01",
            None,
            Some(2),
            false,
            false,
        )
        .await?;
        assert_eq!(rows_written, 2);

        let table = open_table(output_path.to_str().expect("output path")).await?;
        let ctx = SessionContext::new();
        ctx.register_table("checkpoints", Arc::new(table))?;
        let df = ctx.table("checkpoints").await?;
        let batches = df.collect().await?;
        assert!(!batches.is_empty());

        let batch = concat_batches(&batches[0].schema(), &batches)?;
        assert_eq!(batch.num_rows(), 2);

        let base_ts = date_start_ts(NaiveDate::from_ymd_opt(2025, 1, 1).expect("date"));
        let ts = batch
            .column_by_name("ts_local_us")
            .expect("ts_local_us")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts_local_us array");
        assert_eq!(ts.values(), &[base_ts + 1, base_ts + 3]);

        let exchange = batch.column_by_name("exchange").expect("exchange");
        let exchange = if matches!(exchange.data_type(), DataType::Utf8) {
            exchange.clone()
        } else {
            cast(exchange.as_ref(), &DataType::Utf8)?
        };
        let exchange = exchange
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("exchange array");
        assert_eq!(exchange.value(0), "deribit");

        let bids = batch
            .column_by_name("bids")
            .expect("bids")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("bids array");
        assert_eq!(bids.value_length(0), 1);
        assert_eq!(bids.value_length(1), 0);

        let asks = batch
            .column_by_name("asks")
            .expect("asks")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("asks array");
        assert_eq!(asks.value_length(0), 1);
        assert_eq!(asks.value_length(1), 2);

        let kind = batch
            .column_by_name("checkpoint_kind")
            .expect("checkpoint_kind")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("checkpoint_kind array");
        assert_eq!(kind.value(0), "periodic");

        Ok(())
    })
}
