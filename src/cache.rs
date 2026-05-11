use crate::config::CacheConfig;
use crate::entry::Entry;
use crate::frequency::FrequencySketch;
use crate::shard::Shard;
use crate::stats::{CacheStats, CacheStatsSnapshot, LocalMetricsLogger};
use crate::weight::{DefaultWeigher, Weigher};
use ahash::RandomState;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Result returned by [`Cache::get_value`].
///
/// A value is `Fresh` while its TTL is still valid. It is `Stale` after the TTL
/// has expired but before the stale-while-revalidate window has closed.
///
/// Plain [`Cache::get`] deliberately hides this distinction and returns the value
/// for both states. Use `get_value` when the caller needs to know whether a
/// refresh should be scheduled.
pub enum CacheValue<V> {
    Fresh(Arc<V>),
    Stale(Arc<V>),
}

/// A bounded, sharded, in-process cache.
///
/// `Cache` is safe to share between threads. Values are stored behind `Arc`, so
/// reads can return cheap shared handles without cloning the value itself.
///
/// Capacity and weight are enforced globally. Shards reduce lock contention, but
/// they are not fixed capacity buckets. A hot shard can use more than its equal
/// share as long as the whole cache remains within the global limits.
pub struct Cache<K, V, W = DefaultWeigher> {
    // Shard-local maps. The vector length is always a power of two so shard selection can use a mask.
    shards: Arc<Vec<Shard<K, V>>>,
    config: CacheConfig,
    stats: Arc<CacheStats>,
    tick: AtomicU64,
    // Authoritative global entry count. This avoids summing every shard on the hot path.
    global_len: Arc<AtomicUsize>,
    // Authoritative global weighted size. Updated whenever entries are inserted, removed, or expired.
    global_weight: Arc<AtomicU64>,
    hash_builder: RandomState,
    frequency: Arc<FrequencySketch>,
    weigher: W,
    // Serializes capacity-changing writes so admission, eviction, and accounting stay strict.
    capacity_lock: parking_lot::Mutex<()>,
    shutdown: Arc<AtomicBool>,
    janitor: Option<JoinHandle<()>>,
    metrics_logger: Option<LocalMetricsLogger>,
}

impl<K, V> Cache<K, V, DefaultWeigher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Create a cache with the given entry capacity and default configuration.
    ///
    /// This is the shortest path for simple caches. Use [`Cache::builder`] when
    /// you need TTLs, weights, shard counts, metrics, or stale windows.
    pub fn new(max_entries: usize) -> Self {
        crate::CacheBuilder::new().max_entries(max_entries).build()
    }
}

impl Cache<(), (), DefaultWeigher> {
    /// Start building a cache.
    ///
    /// This associated function lives on `Cache<(), ()>` so callers can write
    /// `Cache::builder()` without having to provide generic type arguments first.
    pub fn builder() -> crate::CacheBuilder {
        crate::CacheBuilder::new()
    }
}

impl<K, V, W> Cache<K, V, W>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    W: Weigher<K, V> + Clone + Send + Sync + 'static,
{
    /// Build a cache from a complete config and a custom weigher.
    ///
    /// Most callers should use [`CacheBuilder`]. This function is kept public so
    /// advanced users and tests can assemble the exact configuration directly.
    pub fn with_config_and_weigher(config: CacheConfig, weigher: W) -> Self {
        let shard_count = config.shards.next_power_of_two().max(1);
        let shards = Arc::new((0..shard_count).map(|_| Shard::new()).collect::<Vec<_>>());
        let stats = Arc::new(CacheStats::default());
        let frequency = Arc::new(FrequencySketch::new(
            config.max_entries.saturating_mul(4).max(1024),
        ));
        let global_len = Arc::new(AtomicUsize::new(0));
        let global_weight = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let janitor = config.janitor_interval.map(|interval| {
            let worker_shards = shards.clone();
            let worker_stats = stats.clone();
            let worker_shutdown = shutdown.clone();
            let worker_global_len = global_len.clone();
            let worker_global_weight = global_weight.clone();

            thread::spawn(move || {
                while !worker_shutdown.load(Ordering::Relaxed) {
                    thread::park_timeout(interval);

                    if worker_shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    let now = Instant::now();

                    for shard in worker_shards.iter() {
                        let (removed, removed_weight) = shard.remove_expired(now);

                        if removed > 0 {
                            worker_global_len.fetch_sub(removed, Ordering::AcqRel);
                            worker_global_weight.fetch_sub(removed_weight, Ordering::AcqRel);
                            worker_stats
                                .expirations
                                .fetch_add(removed as u64, Ordering::Relaxed);
                        }
                    }
                }
            })
        });

        let metrics_logger = config.metrics_log_interval.map(|interval| {
            LocalMetricsLogger::start("default".to_string(), stats.clone(), interval)
        });

        Self {
            shards,
            config,
            stats,
            tick: AtomicU64::new(1),
            global_len,
            global_weight,
            hash_builder: RandomState::new(),
            frequency,
            weigher,
            capacity_lock: parking_lot::Mutex::new(()),
            shutdown,
            janitor,
            metrics_logger,
        }
    }

    /// Get a value whether it is fresh or still inside the stale window.
    ///
    /// This is the convenient read API. It treats stale-but-servable values as a
    /// hit. Use [`Cache::get_value`] if the caller needs to distinguish fresh
    /// from stale.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        match self.get_value(key) {
            Some(CacheValue::Fresh(v)) | Some(CacheValue::Stale(v)) => Some(v),
            None => None,
        }
    }

    /// Get a value and report whether it is fresh or stale.
    ///
    /// Dead entries are removed lazily here. That keeps normal operation simple:
    /// the janitor cleans in the background, but correctness does not depend on
    /// the janitor running at exactly the right moment.
    pub fn get_value(&self, key: &K) -> Option<CacheValue<V>> {
        self.frequency.increment(key);
        let idx = self.shard_index(key);
        let now = Instant::now();
        let tick = self.next_tick();
        {
            let map = self.shards[idx].map.read();
            if let Some(entry) = map.get(key) {
                if entry.is_fresh(now) {
                    entry.touch(tick);
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(CacheValue::Fresh(entry.value.clone()));
                }
                if entry.is_stale_but_servable(now) {
                    entry.touch(tick);
                    self.stats.stale_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(CacheValue::Stale(entry.value.clone()));
                }
            }
        }

        if let Some(removed) = self.shards[idx].remove_entry_if_dead(key, now) {
            self.global_len.fetch_sub(1, Ordering::AcqRel);
            self.global_weight
                .fetch_sub(removed.weight, Ordering::AcqRel);

            self.stats.expirations.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert a value and return whether it was admitted.
    ///
    /// `ttl` is per-entry. Passing `None` stores the value without an expiry
    /// deadline.
    pub fn insert(&self, key: K, value: V, ttl: Option<Duration>) -> bool {
        self.try_insert(key, value, ttl).is_ok()
    }

    /// Insert a value, returning it to the caller when admission rejects it.
    ///
    /// This is useful when the value is expensive to build and the caller wants
    /// to handle rejected values directly instead of dropping them.
    pub fn try_insert(&self, key: K, value: V, ttl: Option<Duration>) -> Result<(), V> {
        match self.try_insert_arc(key, Arc::new(value), ttl) {
            Ok(_) => Ok(()),
            Err(value) => Err(Arc::try_unwrap(value).unwrap_or_else(|_| {
                unreachable!("rejected value cannot have other strong references")
            })),
        }
    }

    // Check whether a hypothetical insert/replacement would break the global
    // limits. The caller must hold `capacity_lock` when this is used as part of a
    // mutation; otherwise another writer could change the totals between the
    // check and the insert.
    fn would_exceed_capacity(
        &self,
        additional_entries: usize,
        added_weight: u64,
        replaced_weight: u64,
    ) -> bool {
        let projected_len = self
            .global_len
            .load(Ordering::Acquire)
            .saturating_add(additional_entries);

        if projected_len > self.config.max_entries {
            return true;
        }

        let projected_weight = self
            .global_weight
            .load(Ordering::Acquire)
            .saturating_sub(replaced_weight)
            .saturating_add(added_weight);

        self.config
            .max_weight
            .is_some_and(|max_weight| projected_weight > max_weight)
    }

    // Remove an entry and update the authoritative global counters in the same
    // helper. All eviction paths go through this so accounting cannot drift.
    fn remove_entry_accounted(&self, key: &K) -> Option<Entry<V>> {
        let idx = self.shard_index(key);
        let removed = self.shards[idx].remove_entry(key)?;

        self.global_len.fetch_sub(1, Ordering::AcqRel);
        self.global_weight
            .fetch_sub(removed.weight, Ordering::AcqRel);

        Some(removed)
    }

    // Evict enough entries so the pending insert can fit without publishing a
    // temporary capacity overshoot. The `excluded` key is usually the key being
    // inserted, so a successful insert remains visible after the call.
    fn evict_global_to_fit(
        &self,
        excluded: Option<&K>,
        additional_entries: usize,
        added_weight: u64,
        replaced_weight: u64,
    ) -> usize {
        let mut evicted = 0usize;

        while self.would_exceed_capacity(additional_entries, added_weight, replaced_weight) {
            let Some(victim) = self.sampled_global_victim(excluded) else {
                break;
            };

            if self.remove_entry_accounted(&victim).is_some() {
                evicted += 1;
            } else {
                break;
            }
        }

        evicted
    }

    /// Shared insertion path used by both the normal cache API and the async loader.
    ///
    /// The function returns the `Arc` back on rejection so the loader can still
    /// hand the freshly loaded value to the caller even if the cache refuses to
    /// store it.
    pub(crate) fn try_insert_arc(
        &self,
        key: K,
        value: Arc<V>,
        ttl: Option<Duration>,
    ) -> Result<Arc<V>, Arc<V>> {
        let _capacity_guard = self.capacity_lock.lock();

        self.frequency.increment(&key);

        let weight = self.weigher.weight(&key, value.as_ref()).max(1);

        if self.config.max_weight.is_some_and(|max| weight > max) {
            self.stats
                .rejected_admissions
                .fetch_add(1, Ordering::Relaxed);
            return Err(value);
        }

        let idx = self.shard_index(&key);
        let old_weight = self.shards[idx].entry_weight(&key);
        let replacing = old_weight.is_some();

        let projected_len = self
            .global_len
            .load(Ordering::Acquire)
            .saturating_add(usize::from(!replacing));

        let projected_weight = self
            .global_weight
            .load(Ordering::Acquire)
            .saturating_sub(old_weight.unwrap_or(0))
            .saturating_add(weight);

        if !replacing
            && self.config.enable_tinylfu
            && self.should_reject_admission(&key, projected_len, projected_weight)
        {
            self.stats
                .rejected_admissions
                .fetch_add(1, Ordering::Relaxed);
            return Err(value);
        }

        let evicted = self.evict_global_to_fit(
            Some(&key),
            usize::from(!replacing),
            weight,
            old_weight.unwrap_or(0),
        );

        if evicted > 0 {
            self.stats
                .evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }

        if self.would_exceed_capacity(usize::from(!replacing), weight, old_weight.unwrap_or(0)) {
            self.stats
                .rejected_admissions
                .fetch_add(1, Ordering::Relaxed);
            return Err(value);
        }

        let entry = Entry::from_arc(
            value.clone(),
            weight,
            ttl,
            self.config.stale_while_revalidate,
            self.next_tick(),
        );

        let previous = self.shards[idx].insert_entry(key, entry);

        match previous {
            Some(old) => {
                self.global_weight.fetch_sub(old.weight, Ordering::AcqRel);
                self.global_weight.fetch_add(weight, Ordering::AcqRel);

                self.stats.replacements.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.global_len.fetch_add(1, Ordering::AcqRel);
                self.global_weight.fetch_add(weight, Ordering::AcqRel);

                self.stats.inserts.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(value)
    }

    /// Remove a key and return its value when present.
    pub fn remove(&self, key: &K) -> Option<Arc<V>> {
        let _capacity_guard = self.capacity_lock.lock();

        let idx = self.shard_index(key);
        let removed = self.shards[idx].remove_entry(key)?;

        self.global_len.fetch_sub(1, Ordering::AcqRel);
        self.global_weight
            .fetch_sub(removed.weight, Ordering::AcqRel);

        self.stats.removals.fetch_add(1, Ordering::Relaxed);

        Some(removed.value)
    }

    /// Check whether a key has a live or stale value without updating hit/miss stats.
    pub fn contains_key(&self, key: &K) -> bool {
        self.contains_key_quiet(key)
    }

    /// Return the current global entry count.
    pub fn len(&self) -> usize {
        self.global_len.load(Ordering::Acquire)
    }

    /// Return true when the cache currently holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the current global weight.
    ///
    /// For the default weigher this is the same as the entry count.
    pub fn weight(&self) -> u64 {
        self.global_weight.load(Ordering::Acquire)
    }

    /// Remove all entries from all shards.
    pub fn clear(&self) {
        let _capacity_guard = self.capacity_lock.lock();

        let mut removed = 0usize;

        for shard in self.shards.iter() {
            let (count, _weight) = shard.clear();
            removed = removed.saturating_add(count);
        }

        self.global_len.store(0, Ordering::Release);
        self.global_weight.store(0, Ordering::Release);

        self.stats
            .removals
            .fetch_add(removed as u64, Ordering::Relaxed);
    }

    /// Run expiration cleanup and capacity repair immediately.
    ///
    /// This is mostly useful for tests, benchmarks, and applications that want a
    /// deterministic maintenance point instead of waiting for the janitor thread.
    pub fn run_maintenance(&self) {
        let now = Instant::now();

        for shard in self.shards.iter() {
            let (expired, expired_weight) = shard.remove_expired(now);

            if expired > 0 {
                self.global_len.fetch_sub(expired, Ordering::AcqRel);
                self.global_weight
                    .fetch_sub(expired_weight, Ordering::AcqRel);

                self.stats
                    .expirations
                    .fetch_add(expired as u64, Ordering::Relaxed);
            }
        }

        let _capacity_guard = self.capacity_lock.lock();

        let evicted = self.evict_global_until(None);

        if evicted > 0 {
            self.stats
                .evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }
    }

    /// Return an atomic snapshot of the current cache counters.
    pub fn stats(&self) -> CacheStatsSnapshot {
        self.stats.snapshot()
    }

    #[cfg(feature = "async-loader")]
    pub(crate) fn mark_refresh(&self) {
        self.stats.refreshes.fetch_add(1, Ordering::Relaxed);
    }

    fn contains_key_quiet(&self, key: &K) -> bool {
        let idx = self.shard_index(key);
        self.shards[idx].contains_live_or_stale(key, Instant::now())
    }

    // TinyLFU-style admission gate. When projected capacity is already fine, we
    // accept immediately. When it is not, we compare the candidate with a sampled
    // victim and reject weak candidates before doing eviction work.
    fn should_reject_admission(
        &self,
        key: &K,
        projected_len: usize,
        projected_weight: u64,
    ) -> bool {
        let projected_over_capacity = projected_len > self.config.max_entries
            || self
                .config
                .max_weight
                .is_some_and(|max| projected_weight > max);
        if !projected_over_capacity {
            return false;
        }

        let candidate = self.frequency.estimate(key);
        let victim_score = self
            .sampled_global_victim(None)
            .map(|victim| self.frequency.estimate(&victim))
            .unwrap_or(0);

        candidate < victim_score
    }

    fn global_eviction_required(&self) -> bool {
        self.global_len.load(Ordering::Acquire) > self.config.max_entries
            || self
                .config
                .max_weight
                .is_some_and(|max| self.global_weight.load(Ordering::Acquire) > max)
    }

    fn evict_global_until(&self, excluded: Option<&K>) -> usize {
        let mut evicted = 0usize;
        while self.global_eviction_required() {
            let Some(victim) = self.sampled_global_victim(excluded) else {
                break;
            };

            if self.remove_entry_accounted(&victim).is_some() {
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }

    // Ask each shard for one weak candidate and pick the globally weakest one.
    // This is sampled eviction: cheaper than scanning the whole cache, but still
    // good enough to avoid obviously bad victims.
    fn sampled_global_victim(&self, excluded: Option<&K>) -> Option<K> {
        self.shards
            .iter()
            .filter_map(|shard| {
                shard.sampled_victim_excluding(
                    self.config.eviction_sample_size,
                    &self.frequency,
                    excluded,
                )
            })
            .min_by_key(|key| self.frequency.estimate(key))
    }

    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed)
    }

    // The shard count is a power of two, so a mask is faster than modulo.
    fn shard_index(&self, key: &K) -> usize {
        (self.hash_builder.hash_one(key) as usize) & (self.shards.len() - 1)
    }
}

impl<K, V, W> Drop for Cache<K, V, W> {
    // Stop background helpers before the cache storage disappears. The join is
    // best-effort: panics in worker threads should not panic during `Drop`.
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.metrics_logger.take();
        if let Some(handle) = self.janitor.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}
