use std::time::Instant;

use l2_replay::{L2Update, OrderBook};

fn build_updates(count: usize) -> Vec<L2Update> {
    let mut updates = Vec::with_capacity(count);
    for i in 0..count {
        let side = if i % 2 == 0 { 0 } else { 1 };
        let price_int = 100_000 + (i as i64 % 1_000);
        let size_int = if i % 10 == 0 { 0 } else { 1 + (i as i64 % 100) };
        updates.push(L2Update {
            ts_local_us: i as i64,
            ingest_seq: (i % i32::MAX as usize) as i32,
            file_line_number: (i % i32::MAX as usize) as i32,
            is_snapshot: false,
            side,
            price_int,
            size_int,
            file_id: 1,
        });
    }
    updates
}

#[test]
#[ignore]
fn perf_book_apply_updates() {
    let updates = build_updates(1_000_000);
    let mut book = OrderBook::default();

    let start = Instant::now();
    for update in &updates {
        book.apply_update(update);
    }
    let elapsed = start.elapsed();

    let updates_per_sec = updates.len() as f64 / elapsed.as_secs_f64();
    let mups = updates_per_sec / 1_000_000.0;

    eprintln!(
        "perf_book_apply_updates: {} updates in {:?} -> {:.3} MUPS",
        updates.len(),
        elapsed,
        mups
    );

    if let Ok(min_mups) = std::env::var("POINTLINE_L2_PERF_MIN_MUPS") {
        let min: f64 = min_mups
            .parse()
            .expect("POINTLINE_L2_PERF_MIN_MUPS must be a float");
        assert!(
            mups >= min,
            "book apply performance below threshold: {:.3} < {:.3} MUPS",
            mups,
            min
        );
    }
}
