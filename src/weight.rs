//! Weight calculation for weighted capacity.
//!
//! The default cache treats every entry as weight one. Implement `Weigher` when
//! values have meaningfully different memory or resource costs.

/// Computes the cost of storing a cache entry.
pub trait Weigher<K, V>: Clone + Send + Sync + 'static {
    /// Return the weight for `key` and `value`.
    ///
    /// The cache clamps stored weights to at least one, so returning zero does
    /// not create free entries.
    fn weight(&self, key: &K, value: &V) -> u64;
}

#[derive(Debug, Clone, Copy, Default)]
/// Weigher that assigns every entry a cost of one.
pub struct DefaultWeigher;

impl<K, V> Weigher<K, V> for DefaultWeigher {
    fn weight(&self, _key: &K, _value: &V) -> u64 {
        1
    }
}
