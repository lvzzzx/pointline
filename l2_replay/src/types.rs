use std::collections::BTreeMap;
use std::fmt;

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

#[derive(Clone)]
pub struct OrderBook {
    storage: OrderBookStorage,
}

#[derive(Clone)]
enum OrderBookStorage {
    BTree {
        bids: BTreeMap<i64, i64>,
        asks: BTreeMap<i64, i64>,
    },
    Dense(DenseBook),
}

#[derive(Clone)]
struct DenseBook {
    min_price_int: i64,
    max_price_int: i64,
    tick_size_int: i64,
    bids: Vec<i64>,
    asks: Vec<i64>,
    bids_len: usize,
    asks_len: usize,
    bid_min_idx: Option<usize>,
    bid_max_idx: Option<usize>,
    ask_min_idx: Option<usize>,
    ask_max_idx: Option<usize>,
    warned_bid_oob: bool,
    warned_ask_oob: bool,
}

impl OrderBook {
    pub fn new_dense(
        min_price_int: i64,
        max_price_int: i64,
        tick_size_int: i64,
    ) -> Result<Self, String> {
        DenseBook::new(min_price_int, max_price_int, tick_size_int)
            .map(|dense| Self {
                storage: OrderBookStorage::Dense(dense),
            })
    }

    pub fn reset(&mut self) {
        match &mut self.storage {
            OrderBookStorage::BTree { bids, asks } => {
                bids.clear();
                asks.clear();
            }
            OrderBookStorage::Dense(dense) => dense.reset(),
        }
    }

    pub fn apply_update(&mut self, update: &L2Update) {
        match &mut self.storage {
            OrderBookStorage::BTree { bids, asks } => {
                let side_map = match update.side {
                    0 => bids,
                    1 => asks,
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
            OrderBookStorage::Dense(dense) => dense.apply_update(update),
        }
    }

    pub fn seed_from_levels(&mut self, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        match &mut self.storage {
            OrderBookStorage::BTree { bids: bid_map, asks: ask_map } => {
                bid_map.clear();
                ask_map.clear();
                bid_map.extend(bids.iter().cloned());
                ask_map.extend(asks.iter().cloned());
            }
            OrderBookStorage::Dense(dense) => dense.seed_from_levels(bids, asks),
        }
    }

    pub fn levels(&self) -> (Vec<(i64, i64)>, Vec<(i64, i64)>) {
        match &self.storage {
            OrderBookStorage::BTree { bids, asks } => {
                let bids = bids
                    .iter()
                    .rev()
                    .map(|(price, size)| (*price, *size))
                    .collect();
                let asks = asks.iter().map(|(price, size)| (*price, *size)).collect();
                (bids, asks)
            }
            OrderBookStorage::Dense(dense) => dense.levels(),
        }
    }

    pub fn bids_len(&self) -> usize {
        match &self.storage {
            OrderBookStorage::BTree { bids, .. } => bids.len(),
            OrderBookStorage::Dense(dense) => dense.bids_len,
        }
    }

    pub fn asks_len(&self) -> usize {
        match &self.storage {
            OrderBookStorage::BTree { asks, .. } => asks.len(),
            OrderBookStorage::Dense(dense) => dense.asks_len,
        }
    }

    pub fn bid_at(&self, price_int: i64) -> Option<i64> {
        match &self.storage {
            OrderBookStorage::BTree { bids, .. } => bids.get(&price_int).copied(),
            OrderBookStorage::Dense(dense) => dense.get_bid(price_int),
        }
    }

    pub fn ask_at(&self, price_int: i64) -> Option<i64> {
        match &self.storage {
            OrderBookStorage::BTree { asks, .. } => asks.get(&price_int).copied(),
            OrderBookStorage::Dense(dense) => dense.get_ask(price_int),
        }
    }
}

impl DenseBook {
    fn new(min_price_int: i64, max_price_int: i64, tick_size_int: i64) -> Result<Self, String> {
        if tick_size_int <= 0 {
            return Err("tick_size_int must be > 0".to_string());
        }
        if max_price_int < min_price_int {
            return Err("max_price_int must be >= min_price_int".to_string());
        }
        let range = max_price_int
            .checked_sub(min_price_int)
            .ok_or_else(|| "price range overflow".to_string())?;
        if range % tick_size_int != 0 {
            return Err("price range must align with tick_size_int".to_string());
        }
        let len_i64 = range / tick_size_int + 1;
        let len: usize = len_i64
            .try_into()
            .map_err(|_| "price range too large for usize".to_string())?;

        Ok(Self {
            min_price_int,
            max_price_int,
            tick_size_int,
            bids: vec![0; len],
            asks: vec![0; len],
            bids_len: 0,
            asks_len: 0,
            bid_min_idx: None,
            bid_max_idx: None,
            ask_min_idx: None,
            ask_max_idx: None,
            warned_bid_oob: false,
            warned_ask_oob: false,
        })
    }

    fn reset(&mut self) {
        self.bids.fill(0);
        self.asks.fill(0);
        self.bids_len = 0;
        self.asks_len = 0;
        self.bid_min_idx = None;
        self.bid_max_idx = None;
        self.ask_min_idx = None;
        self.ask_max_idx = None;
    }

    fn price_to_index(&self, price_int: i64) -> Option<usize> {
        Self::price_to_index_static(
            price_int,
            self.min_price_int,
            self.max_price_int,
            self.tick_size_int,
        )
    }

    fn price_to_index_static(
        price_int: i64,
        min_price_int: i64,
        max_price_int: i64,
        tick_size_int: i64,
    ) -> Option<usize> {
        if price_int < min_price_int || price_int > max_price_int {
            return None;
        }
        let offset = price_int - min_price_int;
        if offset % tick_size_int != 0 {
            return None;
        }
        let idx = offset / tick_size_int;
        usize::try_from(idx).ok()
    }

    fn apply_update(&mut self, update: &L2Update) {
        let (min_price_int, max_price_int, tick_size_int) = (
            self.min_price_int,
            self.max_price_int,
            self.tick_size_int,
        );
        let idx = match DenseBook::price_to_index_static(
            update.price_int,
            min_price_int,
            max_price_int,
            tick_size_int,
        ) {
            Some(idx) => idx,
            None => {
                let warn_flag = match update.side {
                    0 => &mut self.warned_bid_oob,
                    1 => &mut self.warned_ask_oob,
                    _ => {
                        debug_assert!(false, "invalid side: {}", update.side);
                        return;
                    }
                };
                warn_out_of_range(
                    update.price_int,
                    min_price_int,
                    max_price_int,
                    tick_size_int,
                    warn_flag,
                );
                return;
            }
        };

        let (side, len_counter, min_idx, max_idx) = match update.side {
            0 => (
                &mut self.bids,
                &mut self.bids_len,
                &mut self.bid_min_idx,
                &mut self.bid_max_idx,
            ),
            1 => (
                &mut self.asks,
                &mut self.asks_len,
                &mut self.ask_min_idx,
                &mut self.ask_max_idx,
            ),
            _ => {
                debug_assert!(false, "invalid side: {}", update.side);
                return;
            }
        };

        let prev = side[idx];
        if update.size_int == 0 {
            if prev != 0 {
                *len_counter = len_counter.saturating_sub(1);
            }
            side[idx] = 0;
            if prev != 0 {
                if min_idx.map_or(false, |v| v == idx) {
                    *min_idx = find_next_nonzero(side, idx, 1);
                }
                if max_idx.map_or(false, |v| v == idx) {
                    *max_idx = find_next_nonzero(side, idx, -1);
                }
            }
        } else {
            if prev == 0 {
                *len_counter = len_counter.saturating_add(1);
                match min_idx {
                    Some(current) => {
                        if idx < *current {
                            *min_idx = Some(idx);
                        }
                    }
                    None => *min_idx = Some(idx),
                }
                match max_idx {
                    Some(current) => {
                        if idx > *current {
                            *max_idx = Some(idx);
                        }
                    }
                    None => *max_idx = Some(idx),
                }
            }
            side[idx] = update.size_int;
        }
    }

    fn seed_from_levels(&mut self, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        self.reset();
        for (price, size) in bids {
            let idx = match DenseBook::price_to_index_static(
                *price,
                self.min_price_int,
                self.max_price_int,
                self.tick_size_int,
            ) {
                Some(idx) => idx,
                None => {
                    warn_out_of_range(
                        *price,
                        self.min_price_int,
                        self.max_price_int,
                        self.tick_size_int,
                        &mut self.warned_bid_oob,
                    );
                    continue;
                }
            };
            if self.bids[idx] == 0 {
                self.bids_len = self.bids_len.saturating_add(1);
                self.bid_min_idx = Some(self.bid_min_idx.map_or(idx, |v| v.min(idx)));
                self.bid_max_idx = Some(self.bid_max_idx.map_or(idx, |v| v.max(idx)));
            }
            self.bids[idx] = *size;
        }
        for (price, size) in asks {
            let idx = match DenseBook::price_to_index_static(
                *price,
                self.min_price_int,
                self.max_price_int,
                self.tick_size_int,
            ) {
                Some(idx) => idx,
                None => {
                    warn_out_of_range(
                        *price,
                        self.min_price_int,
                        self.max_price_int,
                        self.tick_size_int,
                        &mut self.warned_ask_oob,
                    );
                    continue;
                }
            };
            if self.asks[idx] == 0 {
                self.asks_len = self.asks_len.saturating_add(1);
                self.ask_min_idx = Some(self.ask_min_idx.map_or(idx, |v| v.min(idx)));
                self.ask_max_idx = Some(self.ask_max_idx.map_or(idx, |v| v.max(idx)));
            }
            self.asks[idx] = *size;
        }
    }

    fn levels(&self) -> (Vec<(i64, i64)>, Vec<(i64, i64)>) {
        let mut bids = Vec::with_capacity(self.bids_len);
        if let (Some(min), Some(max)) = (self.bid_min_idx, self.bid_max_idx) {
            for idx in (min..=max).rev() {
                let size = self.bids[idx];
                if size != 0 {
                    let price = self.min_price_int + (idx as i64 * self.tick_size_int);
                    bids.push((price, size));
                }
            }
        }

        let mut asks = Vec::with_capacity(self.asks_len);
        if let (Some(min), Some(max)) = (self.ask_min_idx, self.ask_max_idx) {
            for idx in min..=max {
                let size = self.asks[idx];
                if size != 0 {
                    let price = self.min_price_int + (idx as i64 * self.tick_size_int);
                    asks.push((price, size));
                }
            }
        }

        (bids, asks)
    }

    fn get_bid(&self, price_int: i64) -> Option<i64> {
        let idx = self.price_to_index(price_int)?;
        let size = self.bids[idx];
        if size == 0 { None } else { Some(size) }
    }

    fn get_ask(&self, price_int: i64) -> Option<i64> {
        let idx = self.price_to_index(price_int)?;
        let size = self.asks[idx];
        if size == 0 { None } else { Some(size) }
    }

}

fn warn_out_of_range(
    price_int: i64,
    min_price_int: i64,
    max_price_int: i64,
    tick_size_int: i64,
    warn_flag: &mut bool,
) {
    if *warn_flag {
        return;
    }
    *warn_flag = true;
    eprintln!(
        "dense book ignoring out-of-range price: price={} min={} max={} tick={}",
        price_int, min_price_int, max_price_int, tick_size_int
    );
}

fn find_next_nonzero(side: &[i64], start: usize, direction: i8) -> Option<usize> {
    if direction >= 0 {
        let mut idx = start.saturating_add(1);
        while idx < side.len() {
            if side[idx] != 0 {
                return Some(idx);
            }
            idx += 1;
        }
    } else if start > 0 {
        let mut idx = start - 1;
        loop {
            if side[idx] != 0 {
                return Some(idx);
            }
            if idx == 0 {
                break;
            }
            idx -= 1;
        }
    }
    None
}

impl Default for OrderBook {
    fn default() -> Self {
        Self {
            storage: OrderBookStorage::BTree {
                bids: BTreeMap::new(),
                asks: BTreeMap::new(),
            },
        }
    }
}

impl PartialEq for OrderBook {
    fn eq(&self, other: &Self) -> bool {
        match (&self.storage, &other.storage) {
            (
                OrderBookStorage::BTree { bids: lhs_bids, asks: lhs_asks },
                OrderBookStorage::BTree { bids: rhs_bids, asks: rhs_asks },
            ) => lhs_bids == rhs_bids && lhs_asks == rhs_asks,
            _ => self.levels() == other.levels(),
        }
    }
}

impl Eq for OrderBook {}

impl fmt::Debug for OrderBook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.storage {
            OrderBookStorage::BTree { bids, asks } => f
                .debug_struct("OrderBook")
                .field("bids", bids)
                .field("asks", asks)
                .finish(),
            OrderBookStorage::Dense(dense) => f
                .debug_struct("OrderBook")
                .field("min_price_int", &dense.min_price_int)
                .field("max_price_int", &dense.max_price_int)
                .field("tick_size_int", &dense.tick_size_int)
                .field("bids_len", &dense.bids_len)
                .field("asks_len", &dense.asks_len)
                .finish(),
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
