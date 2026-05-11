//! Async loader tests.
//!
//! These cover request coalescing, stale refresh single-flight behavior,
//! backpressure-safe loading, and rejected-admission behavior.

#![cfg(feature = "async-loader")]

use async_trait::async_trait;
use scaffold_cache::{AsyncCacheLoader, Cache, LoadingCache, Weigher};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[derive(Clone)]
struct Loader {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncCacheLoader<String, usize> for Loader {
    type Error = ();
    async fn load(&self, key: String) -> Result<usize, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(key.len())
    }
}

#[tokio::test]
async fn loader_deduplicates_cached_reads() {
    let calls = Arc::new(AtomicUsize::new(0));
    let loader = Loader {
        calls: calls.clone(),
    };
    let cache = Cache::builder()
        .max_entries(100)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build();
    let loading = LoadingCache::new(cache, loader, Some(Duration::from_secs(60)));

    assert_eq!(*loading.get("hello".to_string()).await.unwrap(), 5);
    assert_eq!(*loading.get("hello".to_string()).await.unwrap(), 5);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct SlowLoader {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncCacheLoader<String, usize> for SlowLoader {
    type Error = ();
    async fn load(&self, key: String) -> Result<usize, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        Ok(key.len())
    }
}

#[tokio::test]
async fn loader_deduplicates_concurrent_misses() {
    let calls = Arc::new(AtomicUsize::new(0));
    let loader = SlowLoader {
        calls: calls.clone(),
    };
    let cache = Cache::builder()
        .max_entries(100)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build();
    let loading = Arc::new(LoadingCache::new(
        cache,
        loader,
        Some(Duration::from_secs(60)),
    ));

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let loading = loading.clone();
        tasks.push(tokio::spawn(async move {
            *loading.get("hello".to_string()).await.unwrap()
        }));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), 5);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stale_hit_triggers_only_one_refresh_for_same_key() {
    let calls = Arc::new(AtomicUsize::new(0));
    let loader = SlowLoader {
        calls: calls.clone(),
    };
    let cache = Cache::builder()
        .max_entries(100)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .stale_while_revalidate(Some(Duration::from_secs(1)))
        .build();
    let loading = Arc::new(LoadingCache::new(
        cache,
        loader,
        Some(Duration::from_millis(20)),
    ));

    assert_eq!(*loading.get("hello".to_string()).await.unwrap(), 5);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let loading = loading.clone();
        tasks.push(tokio::spawn(async move {
            *loading.get("hello".to_string()).await.unwrap()
        }));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), 5);
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(loading.cache().stats().refreshes, 1);
}

#[derive(Clone)]
struct StringLen;

impl Weigher<String, String> for StringLen {
    fn weight(&self, _key: &String, value: &String) -> u64 {
        value.len() as u64
    }
}

#[derive(Clone)]
struct StringLoader;

#[async_trait]
impl AsyncCacheLoader<String, String> for StringLoader {
    type Error = ();
    async fn load(&self, key: String) -> Result<String, Self::Error> {
        Ok(key)
    }
}

#[tokio::test]
async fn rejected_loaded_value_is_returned_without_panic() {
    let cache: scaffold_cache::Cache<String, String, StringLen> = scaffold_cache::Cache::builder()
        .max_entries(10)
        .max_weight(2)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .build_with_weigher(StringLen);
    let loading = LoadingCache::with_cache(cache, StringLoader, Some(Duration::from_secs(60)));

    let value = loading.get("too-heavy".to_string()).await.unwrap();
    assert_eq!(value.as_str(), "too-heavy");
    assert_eq!(loading.cache().len(), 0);
}

#[derive(Clone)]
struct FailingSlowLoader {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncCacheLoader<String, usize> for FailingSlowLoader {
    type Error = ();
    async fn load(&self, _key: String) -> Result<usize, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        Err(())
    }
}

#[tokio::test]
async fn stale_refresh_failure_is_not_retried_by_duplicate_stale_hits() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = Cache::builder()
        .max_entries(100)
        .janitor_interval(None)
        .metrics_log_interval(None)
        .stale_while_revalidate(Some(Duration::from_secs(1)))
        .build();
    cache.insert("hello".to_string(), 5usize, Some(Duration::from_millis(20)));
    let loading = Arc::new(LoadingCache::new(
        cache,
        FailingSlowLoader {
            calls: calls.clone(),
        },
        Some(Duration::from_millis(20)),
    ));

    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let loading = loading.clone();
        tasks.push(tokio::spawn(async move {
            *loading.get("hello".to_string()).await.unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 5);
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
