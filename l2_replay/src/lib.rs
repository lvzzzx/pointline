mod arrow_utils;
mod io;
mod ops;
mod replay;
mod types;
mod utils;

#[cfg(feature = "python")]
mod python_binding;

pub use ops::*;
pub use replay::replay;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
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
    fn snapshot_group_ends_on_non_snapshot() {
        let updates = vec![
            update(1, 1, 1, true, 0, 100, 1, 10),
            update(1, 2, 2, false, 1, 101, 2, 10),
            update(2, 3, 3, false, 0, 99, 1, 10),
        ];

        let mut snapshots: Vec<StreamPos> = Vec::new();
        replay(
            updates,
            &ReplayConfig::default(),
            |_book, pos| snapshots.push(*pos),
            |_book, _pos| {},
        );

        assert_eq!(snapshots.len(), 0);
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
            replay(updates, &config, |_book, _pos| {}, |_book, _pos| {});
        });

        assert!(result.is_err());
    }
}