//! Cache metrics and the optional local logger.
//!
//! Metrics are atomic counters. They are intentionally cheap and approximate in
//! the normal concurrent-counter sense: useful for observing behavior, not for
//! financial-grade accounting.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Default)]
/// Internal mutable metrics counters.
pub(crate) struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserts: AtomicU64,
    pub replacements: AtomicU64,
    pub removals: AtomicU64,
    pub evictions: AtomicU64,
    pub expirations: AtomicU64,
    pub rejected_admissions: AtomicU64,
    pub stale_hits: AtomicU64,
    pub refreshes: AtomicU64,
}

impl CacheStats {
    /// Copy all counters into a stable snapshot.
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            replacements: self.replacements.load(Ordering::Relaxed),
            removals: self.removals.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
            rejected_admissions: self.rejected_admissions.load(Ordering::Relaxed),
            stale_hits: self.stale_hits.load(Ordering::Relaxed),
            refreshes: self.refreshes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// User-facing view of the cache metrics.
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub replacements: u64,
    pub removals: u64,
    pub evictions: u64,
    pub expirations: u64,
    pub rejected_admissions: u64,
    pub stale_hits: u64,
    pub refreshes: u64,
}

/// Very small stderr metrics logger for local operation and debugging.
pub struct LocalMetricsLogger {
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LocalMetricsLogger {
    /// Start a background logger that prints snapshots until dropped.
    pub(crate) fn start(name: String, stats: Arc<CacheStats>, interval: Duration) -> Self {
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_shutdown = shutdown.clone();
        let handle = thread::spawn(move || {
            while !worker_shutdown.load(Ordering::Relaxed) {
                thread::park_timeout(interval);
                if worker_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let s = stats.snapshot();
                eprintln!(
                    "[scaffold-cache:{name}] hits={} misses={} inserts={} replacements={} removals={} evictions={} expirations={} rejected_admissions={} stale_hits={} refreshes={}",
                    s.hits, s.misses, s.inserts, s.replacements, s.removals, s.evictions, s.expirations, s.rejected_admissions, s.stale_hits, s.refreshes
                );
            }
        });
        Self {
            shutdown,
            handle: Some(handle),
        }
    }
}

impl Drop for LocalMetricsLogger {
    // Wake and join the logger so it does not outlive the cache.
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}
