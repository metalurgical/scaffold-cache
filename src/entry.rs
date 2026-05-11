//! Entry metadata stored inside each shard.
//!
//! Entries keep the value plus the small amount of state needed for expiry, stale
//! serving, weighted capacity, and recency-aware victim selection.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

/// One cached value and its metadata.
pub(crate) struct Entry<V> {
    /// Shared cached value. Returning `Arc<V>` makes reads cheap.
    pub value: Arc<V>,
    /// Entry cost used by weighted capacity. The default weight is one.
    pub weight: u64,
    /// Freshness deadline. `None` means the entry does not expire.
    pub expires_at: Option<Instant>,
    /// Last instant at which an expired entry may still be served as stale.
    pub stale_until: Option<Instant>,
    /// Monotonic logical access tick used as a cheap recency signal.
    pub last_access: AtomicU64,
}

impl<V> Entry<V> {
    /// Build an entry around an already shared value.
    pub fn from_arc(
        value: Arc<V>,
        weight: u64,
        ttl: Option<Duration>,
        stale_window: Option<Duration>,
        tick: u64,
    ) -> Self {
        let now = Instant::now();
        let expires_at = ttl.map(|ttl| now + ttl);
        let stale_until = match (expires_at, stale_window) {
            (Some(exp), Some(window)) => Some(exp + window),
            _ => None,
        };
        Self {
            value,
            weight,
            expires_at,
            stale_until,
            last_access: AtomicU64::new(tick),
        }
    }

    /// True while the entry has no TTL or the TTL has not expired yet.
    pub fn is_fresh(&self, now: Instant) -> bool {
        self.expires_at.is_none_or(|deadline| now < deadline)
    }

    /// True after TTL expiry but before the stale window ends.
    pub fn is_stale_but_servable(&self, now: Instant) -> bool {
        match (self.expires_at, self.stale_until) {
            (Some(exp), Some(stale_until)) => now >= exp && now < stale_until,
            _ => false,
        }
    }

    /// True when the entry should no longer be returned at all.
    pub fn is_dead(&self, now: Instant) -> bool {
        if let Some(stale_until) = self.stale_until {
            now >= stale_until
        } else if let Some(expires_at) = self.expires_at {
            now >= expires_at
        } else {
            false
        }
    }

    /// Update the recency tick after a successful read.
    pub fn touch(&self, tick: u64) {
        self.last_access.store(tick, Ordering::Relaxed);
    }

    /// Read the last access tick for victim scoring.
    pub fn access_tick(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }
}
