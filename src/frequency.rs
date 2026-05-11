//! Approximate frequency tracking for admission and eviction.
//!
//! This is intentionally small and approximate. It is not meant to count exactly;
//! it is meant to distinguish obviously hot keys from obviously cold keys without
//! storing a counter per key.

use ahash::RandomState;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// Saturating Count-Min-style sketch with periodic aging.
pub(crate) struct FrequencySketch {
    counters: Vec<AtomicU8>,
    mask: u64,
    ops: AtomicU64,
    hash_builder: RandomState,
}

impl FrequencySketch {
    /// Create a sketch with at least `size` counters.
    pub fn new(size: usize) -> Self {
        let len = size.next_power_of_two().max(1024);
        Self {
            counters: (0..len).map(|_| AtomicU8::new(0)).collect(),
            mask: (len as u64) - 1,
            ops: AtomicU64::new(0),
            hash_builder: RandomState::new(),
        }
    }

    /// Record that a key was accessed.
    pub fn increment<K: Hash>(&self, key: &K) {
        let h = self.hash(key);
        for i in 0..4 {
            let idx = self.index(h, i);
            let c = &self.counters[idx];
            let old = c.load(Ordering::Relaxed);
            if old < 15 {
                c.store(old + 1, Ordering::Relaxed);
            }
        }
        let ops = self.ops.fetch_add(1, Ordering::Relaxed);
        if ops > (self.counters.len() as u64 * 10) {
            self.age();
            self.ops.store(0, Ordering::Relaxed);
        }
    }

    /// Estimate a key frequency on a small saturating scale.
    pub fn estimate<K: Hash>(&self, key: &K) -> u8 {
        let h = self.hash(key);
        (0..4)
            .map(|i| self.counters[self.index(h, i)].load(Ordering::Relaxed))
            .min()
            .unwrap_or(0)
    }

    fn hash<Q>(&self, key: &Q) -> u64
    where
        Q: Hash + ?Sized,
    {
        self.hash_builder.hash_one(key)
    }

    fn index(&self, hash: u64, i: u64) -> usize {
        let mixed = hash.wrapping_add(i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        (mixed & self.mask) as usize
    }

    /// Decay all counters so old traffic slowly loses influence.
    fn age(&self) {
        for counter in &self.counters {
            let old = counter.load(Ordering::Relaxed);
            counter.store(old / 2, Ordering::Relaxed);
        }
    }
}
