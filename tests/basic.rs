//! Basic behavioral tests for the synchronous cache API.
//!
//! These tests cover the simple contract first: insert, read, remove, expiry,
//! capacity, and stats.

use scaffold_cache::Cache;
use std::time::Duration;

#[test]
fn insert_get_remove() {
    let cache = Cache::new(100);
    assert!(cache.insert("a".to_string(), 1usize, None));
    assert_eq!(*cache.get(&"a".to_string()).unwrap(), 1);
    assert_eq!(*cache.remove(&"a".to_string()).unwrap(), 1);
    assert!(cache.get(&"a".to_string()).is_none());
}

#[test]
fn ttl_expires() {
    let cache = Cache::builder()
        .max_entries(100)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build();
    cache.insert("a".to_string(), 1usize, Some(Duration::from_millis(25)));
    assert_eq!(*cache.get(&"a".to_string()).unwrap(), 1);
    std::thread::sleep(Duration::from_millis(40));
    assert!(cache.get(&"a".to_string()).is_none());
    assert!(cache.stats().expirations >= 1);
}

#[test]
fn bounded_capacity_evicts() {
    let cache = Cache::builder()
        .shards(1)
        .max_entries(2)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .tinylfu(false)
        .build();
    cache.insert(1, 10, None);
    cache.insert(2, 20, None);
    cache.insert(3, 30, None);
    assert!(cache.len() <= 2);
    assert!(cache.stats().evictions >= 1);
}

#[test]
fn stats_count_hits_and_misses() {
    let cache = Cache::builder()
        .max_entries(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build();
    cache.insert("a".to_string(), 1usize, None);
    assert!(cache.get(&"a".to_string()).is_some());
    assert!(cache.get(&"b".to_string()).is_none());
    let stats = cache.stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
}
