//! Small concurrent smoke test.
//!
//! The heavier invariant tests live in `stress_test.rs`; this file keeps a fast
//! sanity check in the normal test suite.

use scaffold_cache::Cache;
use std::sync::Arc;
use std::thread;

#[test]
fn concurrent_insert_and_get() {
    let cache = Arc::new(
        Cache::builder()
            .shards(16)
            .max_entries(10_000)
            .janitor_interval(None)
            .metrics_log_interval(None)
            .build(),
    );

    let mut handles = Vec::new();
    for t in 0..8 {
        let cache = cache.clone();
        handles.push(thread::spawn(move || {
            for i in 0..1_000 {
                let key = t * 1_000 + i;
                cache.insert(key, key, None);
                assert!(cache.get(&key).is_some());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(cache.len() <= 10_000);
}
