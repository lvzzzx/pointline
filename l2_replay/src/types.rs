use std::collections::BTreeMap;

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
pub struct Checkpoint {
    pub ts_local_us: i64,
    pub file_id: i32,
    pub ingest_seq: i32,
    pub file_line_number: i32,
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
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
pub struct CheckpointMeta {
    pub exchange_id: i16,
    pub symbol_id: i64,
}

#[derive(Debug, Clone)]
pub struct CheckpointRow {
    pub exchange: String,
    pub exchange_id: i16,
    pub symbol_id: i64,
    pub date: i32,
    pub ts_local_us: i64,
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
    pub file_id: i32,
    pub ingest_seq: i32,
    pub file_line_number: i32,
    pub checkpoint_kind: String,
}
