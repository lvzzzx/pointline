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
