//! Long-running stress tests for global capacity invariants.
//!
//! These tests intentionally run many operations from many threads. They are more
//! expensive than normal unit tests, but they caught the important distinction
//! between sequential correctness and concurrently observable correctness.

use scaffold_cache::Cache;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

#[test]
fn runtime_stress_invariants_hold() {
    const THREADS: usize = 16;
    const OPERATIONS_PER_THREAD: usize = 200_000;
    const MAX_ENTRIES: usize = 1_000;

    let cache = Arc::new(
        Cache::builder()
            .max_entries(MAX_ENTRIES)
            .shards(64)
            .build::<u64, Vec<u8>>(),
    );

    let failed = Arc::new(AtomicBool::new(false));

    let start = Instant::now();

    let handles: Vec<_> = (0..THREADS)
        .map(|thread_id| {
            let cache = Arc::clone(&cache);
            let failed = Arc::clone(&failed);

            thread::spawn(move || {
                for i in 0..OPERATIONS_PER_THREAD {
                    let key = ((thread_id * OPERATIONS_PER_THREAD) + i) as u64 % 10_000;

                    match i % 5 {
                        0 => {
                            cache.insert(key, vec![0u8; (key % 128 + 1) as usize], None);
                        }
                        1 => {
                            let _ = cache.get(&key);
                        }
                        2 => {
                            cache.remove(&key);
                        }
                        3 => {
                            cache.contains_key(&key);
                        }
                        _ => {
                            cache.insert(key, vec![1u8; 64], None);
                            let _ = cache.get(&key);
                        }
                    }

                    let len = cache.len();

                    if len > MAX_ENTRIES {
                        eprintln!(
                            "capacity invariant violated: len={} max={}",
                            len, MAX_ENTRIES
                        );

                        failed.store(true, Ordering::Relaxed);
                        return;
                    }

                    if i % 10_000 == 0 {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(
        !failed.load(Ordering::Relaxed),
        "stress invariant failure detected"
    );

    println!(
        "stress test completed in {:?}, final len={}",
        start.elapsed(),
        cache.len()
    );
}

#[test]
fn weighted_runtime_stress_invariants_hold() {
    #[derive(Clone)]
    struct ByteWeigher;

    impl scaffold_cache::Weigher<u64, Vec<u8>> for ByteWeigher {
        fn weight(&self, _key: &u64, value: &Vec<u8>) -> u64 {
            value.len() as u64
        }
    }

    const MAX_WEIGHT: u64 = 32 * 1024;

    let cache = Arc::new(
        Cache::builder()
            .max_entries(10_000)
            .max_weight(MAX_WEIGHT)
            .shards(64)
            .build_with_weigher::<u64, Vec<u8>, ByteWeigher>(ByteWeigher),
    );

    let failed = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..12)
        .map(|thread_id| {
            let cache = Arc::clone(&cache);
            let failed = Arc::clone(&failed);

            thread::spawn(move || {
                for i in 0..100_000 {
                    let key = ((thread_id * 100_000) + i) as u64 % 20_000;

                    let size = ((key % 2048) + 1) as usize;

                    cache.insert(key, vec![0u8; size], None);

                    if i % 3 == 0 {
                        let _ = cache.get(&key);
                    }

                    let weight = cache.weight();

                    if weight > MAX_WEIGHT {
                        eprintln!(
                            "weight invariant violated: weight={} max={}",
                            weight, MAX_WEIGHT
                        );

                        failed.store(true, Ordering::Relaxed);
                        return;
                    }

                    if i % 5000 == 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(
        !failed.load(Ordering::Relaxed),
        "weighted stress invariant failure detected"
    );
}
