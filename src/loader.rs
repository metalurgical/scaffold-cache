//! Async loading layer for `Cache`.
//!
//! This module is enabled by the `async-loader` feature. It adds the behavior most
//! services want around a local cache: miss coalescing, stale refresh, and optional
//! backpressure against the backend loader.

use crate::{Cache, CacheValue, DefaultWeigher, Weigher};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

#[async_trait]
/// Backend loader used by [`LoadingCache`].
///
/// Implement this for whatever knows how to fetch a value when the cache misses.
pub trait AsyncCacheLoader<K, V>: Clone + Send + Sync + 'static {
    /// Error returned when the backend cannot load the value.
    type Error: Send + Sync + 'static;
    /// Load one value for one key.
    async fn load(&self, key: K) -> Result<V, Self::Error>;
}

type KeyLock = Arc<Mutex<()>>;
type InflightMap<K> = Arc<Mutex<HashMap<K, KeyLock, ahash::RandomState>>>;

/// Cache wrapper that loads missing values asynchronously.
///
/// Reads first consult the underlying cache. Fresh values return immediately.
/// Stale values also return immediately, while one background refresh is started
/// for the key. Misses are deduplicated per key so concurrent callers wait on a
/// single backend load.
pub struct LoadingCache<K, V, L, W = DefaultWeigher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    L: AsyncCacheLoader<K, V>,
    W: Weigher<K, V> + Clone + Send + Sync + 'static,
{
    cache: Arc<Cache<K, V, W>>,
    loader: L,
    ttl: Option<Duration>,
    inflight: InflightMap<K>,
    refreshing: Arc<parking_lot::Mutex<HashSet<K, ahash::RandomState>>>,
    load_permits: Option<Arc<Semaphore>>,
}

impl<K, V, L> LoadingCache<K, V, L, DefaultWeigher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    L: AsyncCacheLoader<K, V>,
{
    /// Wrap a default-weighed cache with an async loader.
    pub fn new(cache: Cache<K, V>, loader: L, ttl: Option<Duration>) -> Self {
        Self::with_cache(cache, loader, ttl)
    }
}

impl<K, V, L, W> LoadingCache<K, V, L, W>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    L: AsyncCacheLoader<K, V>,
    W: Weigher<K, V> + Clone + Send + Sync + 'static,
{
    /// Wrap any cache, including one with a custom weigher.
    pub fn with_cache(cache: Cache<K, V, W>, loader: L, ttl: Option<Duration>) -> Self {
        Self {
            cache: Arc::new(cache),
            loader,
            ttl,
            inflight: Arc::new(Mutex::new(HashMap::with_hasher(ahash::RandomState::new()))),
            refreshing: Arc::new(parking_lot::Mutex::new(HashSet::with_hasher(
                ahash::RandomState::new(),
            ))),
            load_permits: None,
        }
    }

    /// Limit the number of backend loads that may run at the same time.
    pub fn with_max_concurrent_loads(mut self, max_concurrent_loads: usize) -> Self {
        assert!(
            max_concurrent_loads > 0,
            "max_concurrent_loads must be greater than zero"
        );
        self.load_permits = Some(Arc::new(Semaphore::new(max_concurrent_loads)));
        self
    }

    /// Get a value, loading it when the cache does not currently have one.
    pub async fn get(&self, key: K) -> Result<Arc<V>, L::Error> {
        match self.cache.get_value(&key) {
            Some(CacheValue::Fresh(value)) => return Ok(value),
            Some(CacheValue::Stale(value)) => {
                self.spawn_refresh(key.clone());
                return Ok(value);
            }
            None => {}
        }

        self.load_deduped(key, false).await
    }

    /// Access the underlying cache.
    pub fn cache(&self) -> Arc<Cache<K, V, W>> {
        self.cache.clone()
    }

    // Run or join the single in-flight load for a key.
    async fn load_deduped(&self, key: K, refresh: bool) -> Result<Arc<V>, L::Error> {
        let lock = self.inflight_lock(key.clone()).await;
        let _guard = lock.lock().await;

        if !refresh {
            match self.cache.get_value(&key) {
                Some(CacheValue::Fresh(value)) | Some(CacheValue::Stale(value)) => {
                    self.remove_inflight_if_current(&key, &lock).await;
                    return Ok(value);
                }
                None => {}
            }
        } else if let Some(CacheValue::Fresh(value)) = self.cache.get_value(&key) {
            self.remove_inflight_if_current(&key, &lock).await;
            return Ok(value);
        }

        let loaded = if let Some(permits) = &self.load_permits {
            let _permit = permits
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            self.loader.load(key.clone()).await
        } else {
            self.loader.load(key.clone()).await
        };

        let loaded = match loaded {
            Ok(value) => Arc::new(value),
            Err(err) => {
                self.remove_inflight_if_current(&key, &lock).await;
                return Err(err);
            }
        };

        let value = match self.cache.try_insert_arc(key.clone(), loaded, self.ttl) {
            Ok(value) | Err(value) => value,
        };
        if refresh {
            self.cache.mark_refresh();
        }
        self.remove_inflight_if_current(&key, &lock).await;
        Ok(value)
    }

    async fn inflight_lock(&self, key: K) -> Arc<Mutex<()>> {
        let mut inflight = self.inflight.lock().await;
        inflight
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn remove_inflight_if_current(&self, key: &K, lock: &Arc<Mutex<()>>) {
        let mut inflight = self.inflight.lock().await;
        if inflight
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, lock))
        {
            inflight.remove(key);
        }
    }

    // Start one background refresh for a stale key. Duplicate stale hits return
    // immediately and do not spawn extra tasks.
    fn spawn_refresh(&self, key: K) {
        {
            let mut refreshing = self.refreshing.lock();
            if !refreshing.insert(key.clone()) {
                return;
            }
        }

        let this = Self {
            cache: self.cache.clone(),
            loader: self.loader.clone(),
            ttl: self.ttl,
            inflight: self.inflight.clone(),
            refreshing: self.refreshing.clone(),
            load_permits: self.load_permits.clone(),
        };
        tokio::spawn(async move {
            let _ = this.load_deduped(key.clone(), true).await;
            this.refreshing.lock().remove(&key);
        });
    }
}
