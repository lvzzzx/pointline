use arrow::ffi;
use deltalake::arrow::array::{Array, StructArray};
use deltalake::arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::wrap_pyfunction;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

use crate::ops::{build_state_checkpoints_delta, replay_between_delta, snapshot_at_delta};
use crate::types::{L2Update, OrderBook, Snapshot, SnapshotWithPos};

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

#[allow(dead_code)]
fn snapshot_with_pos_to_py(py: Python<'_>, snapshot: SnapshotWithPos) -> PyObject {
    let dict = PyDict::new(py);
    dict.set_item("exchange_id", snapshot.exchange_id).ok();
    dict.set_item("symbol_id", snapshot.symbol_id).ok();
    dict.set_item("ts_local_us", snapshot.ts_local_us).ok();
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

fn to_pyarrow(py: Python, batch: RecordBatch) -> PyResult<PyObject> {
    let struct_array: StructArray = batch.into();
    let array_data = struct_array.to_data();
    let (mut array_ffi, mut schema_ffi) = ffi::to_ffi(&array_data)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let pa = py.import("pyarrow")?;
    let array_class = pa.getattr("Array")?;
    let array = array_class.call_method1(
        "_import_from_c",
        (
            std::ptr::addr_of_mut!(array_ffi) as usize,
            std::ptr::addr_of_mut!(schema_ffi) as usize,
        ),
    )?;

    let batch_class = pa.getattr("RecordBatch")?;
    let batch = batch_class.call_method1("from_struct_array", (array,))?;
    Ok(batch.into())
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
        every_updates=None,
        dense_price_min=None,
        dense_price_max=None,
        dense_tick_size=None
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
    dense_price_min: Option<i64>,
    dense_price_max: Option<i64>,
    dense_tick_size: Option<i64>,
) -> PyResult<PyObject> {
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
            dense_price_min,
            dense_price_max,
            dense_tick_size,
        ))
    });

    match result {
        Ok(batch) => to_pyarrow(py, batch),
        Err(err) => Err(PyRuntimeError::new_err(format!("{err:?}"))),
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
        symbol_id,
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
    symbol_id: i64,
    checkpoint_every_us: Option<i64>,
    checkpoint_every_updates: Option<u64>,
    validate_monotonic: bool,
    assume_sorted: bool,
) -> PyResult<usize> {
    let result = py.allow_threads(|| {
        runtime().block_on(build_state_checkpoints_delta(
            &updates_path,
            &output_path,
            exchange.as_deref(),
            exchange_id,
            symbol_id,
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
        self.book.seed_from_levels(&bids, &asks);
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
        let (bids, asks) = self.book.levels();

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
