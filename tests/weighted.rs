//! Focused tests for custom weighted capacity.

use scaffold_cache::{Cache, Weigher};

#[derive(Clone)]
struct StringLen;

impl Weigher<String, String> for StringLen {
    fn weight(&self, _key: &String, value: &String) -> u64 {
        value.len() as u64
    }
}

#[test]
fn weighted_capacity_is_enforced() {
    let cache: Cache<String, String, StringLen> = Cache::builder()
        .shards(1)
        .max_entries(100)
        .max_weight(10)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .tinylfu(false)
        .build_with_weigher(StringLen);

    cache.insert("a".to_string(), "12345".to_string(), None);
    cache.insert("b".to_string(), "67890".to_string(), None);
    cache.insert("c".to_string(), "xx".to_string(), None);
    assert!(cache.weight() <= 10);
}
