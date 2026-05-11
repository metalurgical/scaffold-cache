//! Regression tests for edge cases found while hardening the cache.
//!
//! These are intentionally specific. They protect global capacity, weighted
//! capacity, fallible admission, stale windows, replacement semantics, and simple
//! concurrent maintenance behavior.

use scaffold_cache::{Cache, CacheValue, Weigher};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Clone)]
struct StringLen;

impl Weigher<String, String> for StringLen {
    fn weight(&self, _key: &String, value: &String) -> u64 {
        value.len() as u64
    }
}

#[test]
fn stale_values_are_reported_then_expire_after_stale_window() {
    let cache = Cache::builder()
        .max_entries(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .stale_while_revalidate(Some(Duration::from_millis(100)))
        .build();

    cache.insert("a".to_string(), 1usize, Some(Duration::from_millis(25)));
    thread::sleep(Duration::from_millis(40));

    match cache.get_value(&"a".to_string()) {
        Some(CacheValue::Stale(value)) => assert_eq!(*value, 1),
        _ => panic!("expected stale value"),
    }

    thread::sleep(Duration::from_millis(110));
    assert!(cache.get(&"a".to_string()).is_none());
}

#[test]
fn try_insert_returns_rejected_value() {
    let cache: Cache<String, String, StringLen> = Cache::builder()
        .shards(1)
        .max_entries(10)
        .max_weight(2)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build_with_weigher(StringLen);

    let rejected = cache
        .try_insert("too-heavy".to_string(), "123".to_string(), None)
        .expect_err("entry should exceed weight budget");
    assert_eq!(rejected, "123");
    assert_eq!(cache.len(), 0);
}

#[test]
fn replacement_is_allowed_even_when_cache_is_full() {
    let cache = Cache::builder()
        .shards(1)
        .max_entries(1)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build();

    assert!(cache.insert("a".to_string(), 1usize, None));
    assert!(cache.insert("a".to_string(), 2usize, None));
    assert_eq!(*cache.get(&"a".to_string()).unwrap(), 2);
    assert_eq!(cache.len(), 1);
}

#[test]
fn stress_insert_get_clear_under_contention() {
    let cache = Arc::new(
        Cache::builder()
            .shards(16)
            .max_entries(2_000)
            .janitor_interval(None)
            .metrics_log_interval(None)
            .tinylfu(false)
            .build(),
    );

    let mut handles = Vec::new();
    for t in 0..8 {
        let cache = cache.clone();
        handles.push(thread::spawn(move || {
            for i in 0..2_000 {
                let key = (t * 2_000) + i;
                cache.insert(key, key, Some(Duration::from_secs(5)));
                let _ = cache.get(&key);
                if i % 250 == 0 {
                    cache.run_maintenance();
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(cache.len() <= 2_000);
    cache.clear();
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.weight(), 0);
}

#[test]
fn default_sharding_respects_global_entry_capacity() {
    let cache = Cache::builder()
        .max_entries(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .tinylfu(false)
        .build();

    for i in 0..100 {
        cache.insert(i, i, None);
    }

    assert!(
        cache.len() <= 10,
        "len exceeded global capacity: {}",
        cache.len()
    );
}

#[test]
fn default_sharding_respects_global_weight_capacity() {
    let cache: Cache<String, String, StringLen> = Cache::builder()
        .max_entries(100)
        .max_weight(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .tinylfu(false)
        .build_with_weigher(StringLen);

    for i in 0..100 {
        cache.insert(format!("k{i}"), "x".to_string(), None);
    }

    assert!(
        cache.weight() <= 10,
        "weight exceeded global capacity: {}",
        cache.weight()
    );
}

#[test]
fn weighted_entry_can_use_full_global_budget_with_default_sharding() {
    let cache: Cache<String, String, StringLen> = Cache::builder()
        .max_entries(100)
        .max_weight(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .tinylfu(false)
        .build_with_weigher(StringLen);

    assert!(cache.insert("large".to_string(), "1234567890".to_string(), None));
    assert_eq!(cache.weight(), 10);
    assert_eq!(cache.get(&"large".to_string()).map(|v| v.len()), Some(10));
}

#[test]
fn contains_key_does_not_count_as_hit_or_miss() {
    let cache = Cache::builder()
        .max_entries(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build();

    cache.insert("a".to_string(), 1usize, None);
    let before = cache.stats();
    assert!(cache.contains_key(&"a".to_string()));
    assert!(!cache.contains_key(&"b".to_string()));
    let after = cache.stats();
    assert_eq!(after.hits, before.hits);
    assert_eq!(after.misses, before.misses);
}

#[test]
fn accepted_insert_remains_visible_after_eviction() {
    let cache = Cache::builder()
        .shards(8)
        .max_entries(1)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .tinylfu(false)
        .build();

    assert!(cache.insert("old".to_string(), 1usize, None));
    assert!(cache.insert("new".to_string(), 2usize, None));
    assert_eq!(*cache.get(&"new".to_string()).unwrap(), 2);
    assert!(cache.len() <= 1);
}
