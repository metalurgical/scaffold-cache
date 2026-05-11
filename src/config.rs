//! Configuration and builder API for `ScaffoldCache`.
//!
//! The builder is intentionally explicit. The defaults are useful for normal
//! development, but production callers should usually set capacity, metrics, TTL,
//! and shard count based on their workload.

use std::time::Duration;

#[derive(Debug, Clone)]
/// Complete cache configuration.
///
/// Most code should not construct this directly. Use [`CacheBuilder`] unless you
/// deliberately want to pass around a full config value.
pub struct CacheConfig {
    /// Number of storage shards. Rounded up to a power of two by the builder.
    pub shards: usize,
    /// Maximum number of entries allowed across the whole cache.
    pub max_entries: usize,
    /// Optional global weight budget across all entries.
    pub max_weight: Option<u64>,
    /// Number of entries each shard considers when proposing an eviction victim.
    pub eviction_sample_size: usize,
    /// Background expiration interval. `None` disables the janitor thread.
    pub janitor_interval: Option<Duration>,
    /// Optional stderr metrics logging interval. Disable this in benchmarks.
    pub metrics_log_interval: Option<Duration>,
    /// How long expired entries remain servable as stale values.
    pub stale_while_revalidate: Option<Duration>,
    /// Whether TinyLFU-style admission checks are enabled.
    pub enable_tinylfu: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            shards: 64,
            max_entries: 100_000,
            max_weight: None,
            eviction_sample_size: 16,
            janitor_interval: Some(Duration::from_secs(30)),
            metrics_log_interval: Some(Duration::from_secs(60)),
            stale_while_revalidate: None,
            enable_tinylfu: true,
        }
    }
}

#[derive(Default)]
/// Fluent builder for [`crate::Cache`].
///
/// The builder consumes and returns `self` for each setting, so it is cheap to
/// chain and hard to partially mutate by accident.
pub struct CacheBuilder {
    config: CacheConfig,
}

impl CacheBuilder {
    /// Start with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the requested shard count.
    ///
    /// The value is rounded up to the next power of two because shard lookup uses
    /// a bit mask.
    pub fn shards(mut self, shards: usize) -> Self {
        assert!(shards > 0, "shards must be greater than zero");
        self.config.shards = shards.next_power_of_two();
        self
    }

    /// Set the global entry capacity.
    pub fn max_entries(mut self, max_entries: usize) -> Self {
        assert!(max_entries > 0, "max_entries must be greater than zero");
        self.config.max_entries = max_entries;
        self
    }

    /// Set the optional global weight capacity.
    pub fn max_weight(mut self, max_weight: u64) -> Self {
        assert!(max_weight > 0, "max_weight must be greater than zero");
        self.config.max_weight = Some(max_weight);
        self
    }

    /// Set how many entries a shard samples when looking for a victim.
    pub fn eviction_sample_size(mut self, sample_size: usize) -> Self {
        assert!(sample_size > 0, "sample size must be greater than zero");
        self.config.eviction_sample_size = sample_size;
        self
    }

    /// Configure the background expiration janitor.
    pub fn janitor_interval(mut self, interval: Option<Duration>) -> Self {
        self.config.janitor_interval = interval;
        self
    }

    /// Configure local stderr metrics logging.
    pub fn metrics_log_interval(mut self, interval: Option<Duration>) -> Self {
        self.config.metrics_log_interval = interval;
        self
    }

    /// Configure how long expired entries may still be returned as stale.
    pub fn stale_while_revalidate(mut self, window: Option<Duration>) -> Self {
        self.config.stale_while_revalidate = window;
        self
    }

    /// Enable or disable TinyLFU-style admission checks.
    pub fn tinylfu(mut self, enabled: bool) -> Self {
        self.config.enable_tinylfu = enabled;
        self
    }

    /// Build a cache using the default unit weigher.
    pub fn build<K, V>(self) -> crate::Cache<K, V>
    where
        K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        crate::Cache::with_config_and_weigher(self.config, crate::DefaultWeigher)
    }

    /// Build a cache using a custom weigher.
    pub fn build_with_weigher<K, V, W>(self, weigher: W) -> crate::Cache<K, V, W>
    where
        K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
        W: crate::Weigher<K, V> + Clone + Send + Sync + 'static,
    {
        crate::Cache::with_config_and_weigher(self.config, weigher)
    }
}
