use std::sync::Arc;

use anyhow::{anyhow, Result};
use deltalake::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Builder, Int16Array, Int16Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, Int8Array, LargeListArray, ListArray, ListBuilder,
    StringBuilder, StructArray, StructBuilder, UInt8Array,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;

use crate::types::{
    CheckpointMeta, CheckpointRow, L2Update, OrderBook,
};

pub struct UpdateColumns<'a> {
    pub ts_local_us: &'a Int64Array,
    pub ingest_seq: &'a Int32Array,
    pub file_line_number: &'a Int32Array,
    pub is_snapshot: &'a BooleanArray,
    pub side: ArrayRef,
    pub price_int: &'a Int64Array,
    pub size_int: &'a Int64Array,
    pub file_id: &'a Int32Array,
}

pub struct CheckpointUpdateColumns<'a> {
    pub exchange_id: &'a Int16Array,
    pub symbol_id: &'a Int64Array,
    pub ts_local_us: &'a Int64Array,
    pub ingest_seq: &'a Int32Array,
    pub file_line_number: &'a Int32Array,
    pub is_snapshot: &'a BooleanArray,
    pub side: ArrayRef,
    pub price_int: &'a Int64Array,
    pub size_int: &'a Int64Array,
    pub file_id: &'a Int32Array,
}

pub fn get_array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let idx = batch.schema().index_of(name)?;
    let array = batch.column(idx);
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("column {} has unexpected type", name))
}

pub fn get_u8_value(array: &ArrayRef, row: usize) -> Result<u8> {
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

pub fn get_i64(batch: &RecordBatch, name: &str, row: usize) -> Result<i64> {
    Ok(get_array::<Int64Array>(batch, name)?.value(row))
}

pub fn get_i32(batch: &RecordBatch, name: &str, row: usize) -> Result<i32> {
    Ok(get_array::<Int32Array>(batch, name)?.value(row))
}

pub fn get_levels(batch: &RecordBatch, name: &str, row: usize) -> Result<Vec<(i64, i64)>> {
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

pub fn list_levels(list: &ListArray, row: usize) -> Result<Vec<(i64, i64)>> {
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

pub fn large_list_levels(list: &LargeListArray, row: usize) -> Result<Vec<(i64, i64)>> {
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

pub fn struct_levels(struct_array: &StructArray) -> Result<Vec<(i64, i64)>> {
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

pub fn book_levels(book: &OrderBook) -> (Vec<(i64, i64)>, Vec<(i64, i64)>) {
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

pub fn append_levels(builder: &mut ListBuilder<StructBuilder>, levels: &[(i64, i64)]) {
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

pub fn build_checkpoint_batch(rows: &[CheckpointRow]) -> Result<RecordBatch> {
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

pub fn update_columns<'a>(batch: &'a RecordBatch) -> Result<UpdateColumns<'a>> {
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

pub fn update_from_columns(cols: &UpdateColumns<'_>, row: usize) -> Result<L2Update> {
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

pub fn checkpoint_update_columns<'a>(batch: &'a RecordBatch) -> Result<CheckpointUpdateColumns<'a>> {
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

pub fn checkpoint_update_from_columns(
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
