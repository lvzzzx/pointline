use crate::types::{L2Update, OrderBook, ReplayConfig, StreamPos};

#[derive(Default)]
pub struct SnapshotReset {
    pub snapshot_key: Option<(i64, i32)>,
}

impl SnapshotReset {
    pub fn apply(&mut self, book: &mut OrderBook, update: &L2Update) {
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

#[derive(Debug, Default)]
pub struct CadenceState {
    pub last_emit_ts: Option<i64>,
    pub updates_since_emit: u64,
}

impl CadenceState {
    pub fn record_update(&mut self, should_count: bool) {
        if should_count {
            self.updates_since_emit = self.updates_since_emit.saturating_add(1);
        }
    }

    pub fn should_emit(
        &mut self,
        pos_ts: i64,
        every_us: Option<i64>,
        every_updates: Option<u64>,
    ) -> bool {
        if self.last_emit_ts.is_none() {
            self.last_emit_ts = Some(pos_ts);
        }

        let mut emit = false;
        if let (Some(every_us), Some(last_ts)) = (every_us, self.last_emit_ts) {
            if pos_ts.saturating_sub(last_ts) >= every_us {
                emit = true;
            }
        }
        if let Some(every_updates_val) = every_updates {
            if every_updates_val > 0 && self.updates_since_emit >= every_updates_val {
                emit = true;
            }
        }

        if emit {
            self.last_emit_ts = Some(pos_ts);
            self.updates_since_emit = 0;
        }

        emit
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
    let mut prev_key: Option<(i64, i32, i32, i32)> = None;

    let mut snapshot_key: Option<(i64, i32)> = None; // (ts_local_us, file_id)
    let mut snapshot_pos: Option<StreamPos> = None;

    let mut cadence = CadenceState::default();

    // Atomic processing state
    let mut last_pos: Option<StreamPos> = None;

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

        // 1. Check boundary
        if let Some(pos) = last_pos {
            if update.ts_local_us != pos.ts_local_us {
                // Emit snapshots/checkpoints for 'pos'
                if snapshot_key.is_some() {
                    if let Some(snap_pos) = snapshot_pos {
                        on_snapshot(&book, &snap_pos);
                    }
                }

                let emit_checkpoint = cadence.should_emit(
                    pos.ts_local_us,
                    config.checkpoint_every_us,
                    config.checkpoint_every_updates,
                );
                if emit_checkpoint {
                    on_checkpoint(&book, &pos);
                }

                // Clear snapshot state for new group
                snapshot_key = None;
                snapshot_pos = None;
            }
        }

        // 2. Apply update
        if update.is_snapshot {
            let key = (update.ts_local_us, update.file_id);
            if snapshot_key != Some(key) {
                book.reset();
                snapshot_key = Some(key);
            }
        } else {
            snapshot_key = None;
            snapshot_pos = None;
        }

        book.apply_update(&update);

        // 3. Update state
        let pos = StreamPos {
            ts_local_us: update.ts_local_us,
            ingest_seq: update.ingest_seq,
            file_line_number: update.file_line_number,
            file_id: update.file_id,
        };

        if update.is_snapshot {
            snapshot_pos = Some(pos);
        }

        cadence.record_update(true);
        last_pos = Some(pos);
    }

    // Handle final group
    if let Some(pos) = last_pos {
        if snapshot_key.is_some() {
            if let Some(snap_pos) = snapshot_pos {
                on_snapshot(&book, &snap_pos);
            }
        }

        let emit_checkpoint = cadence.should_emit(
            pos.ts_local_us,
            config.checkpoint_every_us,
            config.checkpoint_every_updates,
        );
        if emit_checkpoint {
            on_checkpoint(&book, &pos);
        }
    }
}
