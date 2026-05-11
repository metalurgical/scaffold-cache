//! Shard-local storage.
//!
//! A shard is just a locked hash map plus local weight accounting and a rotating
//! sampler cursor. It does not know the global cache policy; `Cache` owns global
//! accounting and decides when eviction is needed.

use crate::entry::Entry;
use crate::frequency::FrequencySketch;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// One lock-partition of the cache.
pub(crate) struct Shard<K, V> {
    pub map: RwLock<HashMap<K, Entry<V>, ahash::RandomState>>,
    pub current_weight: parking_lot::Mutex<u64>,
    sample_cursor: AtomicUsize,
}

impl<K, V> Shard<K, V>
where
    K: Eq + Hash + Clone,
{
    /// Create an empty shard.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::with_hasher(ahash::RandomState::new())),
            current_weight: parking_lot::Mutex::new(0),
            sample_cursor: AtomicUsize::new(0),
        }
    }

    /// Remove everything and return `(entry_count, removed_weight)`.
    pub fn clear(&self) -> (usize, u64) {
        let mut map = self.map.write();
        let removed = map.len();

        let mut weight = self.current_weight.lock();
        let removed_weight = *weight;

        map.clear();
        *weight = 0;

        (removed, removed_weight)
    }

    /// Remove dead entries and return `(entry_count, removed_weight)`.
    pub fn remove_expired(&self, now: Instant) -> (usize, u64) {
        let mut map = self.map.write();
        let mut removed = 0usize;
        let mut removed_weight = 0u64;

        map.retain(|_, entry| {
            let keep = !entry.is_dead(now);

            if !keep {
                removed += 1;
                removed_weight = removed_weight.saturating_add(entry.weight);
            }

            keep
        });

        if removed_weight > 0 {
            let mut weight = self.current_weight.lock();
            *weight = weight.saturating_sub(removed_weight);
        }

        (removed, removed_weight)
    }

    /// Check for a key without touching stats or frequency.
    pub fn contains_live_or_stale(&self, key: &K, now: Instant) -> bool {
        let map = self.map.read();
        map.get(key).is_some_and(|entry| !entry.is_dead(now))
    }

    /// Return the current weight for a key, used before replacement.
    pub fn entry_weight(&self, key: &K) -> Option<u64> {
        let map = self.map.read();
        map.get(key).map(|entry| entry.weight)
    }

    /// Remove a key only if it is already dead.
    pub fn remove_entry_if_dead(&self, key: &K, now: Instant) -> Option<Entry<V>> {
        let mut map = self.map.write();
        if !map.get(key).is_some_and(|entry| entry.is_dead(now)) {
            return None;
        }
        let removed = map.remove(key)?;
        let mut weight = self.current_weight.lock();
        *weight = weight.saturating_sub(removed.weight);
        Some(removed)
    }

    /// Insert or replace an entry, returning the previous entry when present.
    pub fn insert_entry(&self, key: K, entry: Entry<V>) -> Option<Entry<V>> {
        let new_weight = entry.weight;
        let mut map = self.map.write();
        let previous = map.insert(key, entry);
        let mut weight = self.current_weight.lock();
        *weight = weight.saturating_add(new_weight);
        if let Some(old) = &previous {
            *weight = weight.saturating_sub(old.weight);
        }
        previous
    }

    /// Remove one entry from this shard.
    pub fn remove_entry(&self, key: &K) -> Option<Entry<V>> {
        let mut map = self.map.write();
        let removed = map.remove(key)?;
        let mut weight = self.current_weight.lock();
        *weight = weight.saturating_sub(removed.weight);
        Some(removed)
    }

    /// Return a weak eviction candidate while optionally excluding one key.
    pub fn sampled_victim_excluding(
        &self,
        sample_size: usize,
        frequency: &FrequencySketch,
        excluded: Option<&K>,
    ) -> Option<K> {
        let map = self.map.read();
        self.sampled_victim_from_map(&map, sample_size, frequency, excluded)
    }

    /// Implementation detail for rotating sampled eviction.
    fn sampled_victim_from_map(
        &self,
        map: &HashMap<K, Entry<V>, ahash::RandomState>,
        sample_size: usize,
        frequency: &FrequencySketch,
        excluded: Option<&K>,
    ) -> Option<K> {
        if map.is_empty() {
            return None;
        }

        let len = map.len();
        let sample_size = sample_size.max(1).min(len);
        let start = self.sample_cursor.fetch_add(sample_size, Ordering::Relaxed) % len;

        map.iter()
            .cycle()
            .skip(start)
            .take(sample_size)
            .filter(|(key, _)| excluded.is_none_or(|excluded| *key != excluded))
            .min_by_key(|(key, entry)| (frequency.estimate(*key), entry.access_tick()))
            .map(|(key, _)| key.clone())
    }
}
