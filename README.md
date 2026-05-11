# ScaffoldCache

ScaffoldCache is a production-oriented, high-performance in-process Rust cache.

It is built for the practical case where you want a fast local cache with bounded memory use, predictable cleanup, concurrent access, and enough observability to understand what it is doing under load.

It is **not** a distributed cache, not a Redis replacement, and not a full Caffeine-equivalent Window TinyLFU implementation. It is an embedded cache that keeps its guarantees local to the current process.

## What it gives you

- Sharded storage to reduce lock contention
- Global bounded entry capacity across all shards
- Optional global weighted capacity across all shards
- TTL expiry with lazy and janitor-driven cleanup
- Stale-while-revalidate support through the `async-loader` feature
- Per-key request coalescing for cache misses
- Single-flight stale refreshes
- Optional async loader backpressure with `with_max_concurrent_loads`
- Global sampled eviction using frequency and recency signals
- TinyLFU-style admission sketch with projected-capacity checks
- Atomic metrics snapshots
- Optional local metrics logging via stderr
- Background janitor with clean shutdown on `Drop`
- Criterion benchmarks
- Stress and concurrency tests
- Clippy-clean with `-D warnings`

## Design in one paragraph

The cache stores entries in multiple shards. Each shard owns a locked hash map, so unrelated keys can usually be read or written without fighting over the same lock. Capacity and weight are still enforced globally, because fixed per-shard quotas waste space and behave badly with uneven key distribution. Global length and weight are tracked with atomics, while capacity-changing writes use a single capacity lock to keep admission and eviction strict. Eviction samples candidate victims from the shards and chooses a weak entry using frequency and recency information.

## Installation

For local development:

```toml
[dependencies]
scaffold-cache = { path = "../scaffold-cache" }
```

With async loader support:

```toml
[dependencies]
scaffold-cache = { path = "../scaffold-cache", features = ["async-loader"] }
```

## Basic usage

```rust
use scaffold_cache::Cache;
use std::time::Duration;

let cache = Cache::builder()
    .shards(8)
    .max_entries(100_000)
    .metrics_log_interval(None)
    .build::<String, String>();

cache.insert(
    "key".to_string(),
    "value".to_string(),
    Some(Duration::from_secs(30)),
);

let value = cache.get(&"key".to_string());

assert_eq!(value.as_deref(), Some(&"value".to_string()));
```

## Capacity

Capacity is enforced globally across all shards.

```rust
let cache = scaffold_cache::Cache::builder()
    .shards(8)
    .max_entries(10_000)
    .build::<u64, String>();
```

If a new insert would exceed `max_entries`, the cache evicts existing entries before publishing the new entry. The inserted key is excluded from the victim search so a successful insert remains visible immediately after insertion.

## Weighted capacity

Weighted mode lets entries consume variable capacity. This is useful when values have very different memory cost.

```rust
use scaffold_cache::{Cache, Weigher};

#[derive(Clone)]
struct ByteWeigher;

impl Weigher<u64, Vec<u8>> for ByteWeigher {
    fn weight(&self, _key: &u64, value: &Vec<u8>) -> u64 {
        value.len() as u64
    }
}

let cache = Cache::builder()
    .shards(8)
    .max_entries(100_000)
    .max_weight(64 * 1024 * 1024)
    .build_with_weigher::<u64, Vec<u8>, ByteWeigher>(ByteWeigher);

cache.insert(1, vec![0u8; 1024], None);
```

Weights are enforced globally, not as per-shard quotas.

## Fallible admission

Use `try_insert` when the caller needs to retain ownership of a value that is rejected by weight or admission policy.

```rust
let cache = scaffold_cache::Cache::new(1);

match cache.try_insert("key".to_string(), "value".to_string(), None) {
    Ok(()) => {}
    Err(value) => {
        // The value was not admitted. Use it directly, retry later, or log it.
        let _ = value;
    }
}
```

## TTL expiry

```rust
use scaffold_cache::Cache;
use std::time::Duration;

let cache = Cache::builder()
    .max_entries(1_000)
    .build::<u64, String>();

cache.insert(1, "hello".to_string(), Some(Duration::from_secs(60)));
```

TTL is supplied per insert. Passing `None` stores the entry without an expiry deadline. Expired entries are removed lazily during access and also by the background janitor.

## Stale-while-revalidate

A stale value is an expired value that is still inside the configured stale window.

```rust
use scaffold_cache::{Cache, CacheValue};
use std::time::Duration;

let cache = Cache::builder()
    .max_entries(1_000)
    .stale_while_revalidate(Some(Duration::from_secs(30)))
    .build::<u64, String>();

cache.insert(1, "hello".to_string(), Some(Duration::from_secs(10)));

match cache.get_value(&1) {
    Some(CacheValue::Fresh(value)) => println!("fresh: {value}"),
    Some(CacheValue::Stale(value)) => println!("stale: {value}"),
    None => println!("missing"),
}
```

The raw `Cache` reports stale values. The async loader is what turns stale reads into background refreshes.

## Async loader

Enable the `async-loader` feature to use request coalescing and stale refresh.

Concurrent misses for the same key are deduplicated. Stale hits return immediately and trigger one coordinated refresh in the background.

```rust,no_run
use async_trait::async_trait;
use scaffold_cache::{AsyncCacheLoader, Cache, LoadingCache};
use std::time::Duration;

#[derive(Clone)]
struct Loader;

#[async_trait]
impl AsyncCacheLoader<String, usize> for Loader {
    type Error = ();

    async fn load(&self, key: String) -> Result<usize, Self::Error> {
        Ok(key.len())
    }
}

async fn example() -> Result<(), ()> {
    let cache = Cache::builder()
        .max_entries(10_000)
        .stale_while_revalidate(Some(Duration::from_secs(30)))
        .build::<String, usize>();

    let loading = LoadingCache::new(cache, Loader, Some(Duration::from_secs(60)))
        .with_max_concurrent_loads(128);

    let value = loading.get("hello".to_string()).await?;

    assert_eq!(*value, 5);
    Ok(())
}
```

## Metrics

```rust
let stats = cache.stats();

println!("hits={}", stats.hits);
println!("misses={}", stats.misses);
println!("evictions={}", stats.evictions);
```

Optional periodic stderr metrics logging:

```rust
use std::time::Duration;

let cache = scaffold_cache::Cache::builder()
    .metrics_log_interval(Some(Duration::from_secs(60)))
    .build::<u64, String>();
```

Disable logging in tests and benchmarks:

```rust
let cache = scaffold_cache::Cache::builder()
    .metrics_log_interval(None)
    .build::<u64, String>();
```

## Testing

Run standard tests:

```bash
cargo test
```

Run all feature tests:

```bash
cargo test --all-features
```

Run only stress tests:

```bash
cargo test --test stress_test
```

Run stress tests in release mode:

```bash
cargo test --release --test stress_test
```

Run Clippy:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Run formatting:

```bash
cargo fmt --all
cargo fmt --all --check
```

Recommended verification gate:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo test --release --test stress_test
```

## Benchmarks

Run all benchmarks:

```bash
cargo bench
```

Run the cache benchmark suite:

```bash
cargo bench --bench cache_bench
```

Run with a stable measurement setup:

```bash
cargo bench --bench cache_bench -- --measurement-time 15 --sample-size 20
```

Criterion reports are written under:

```text
target/criterion/
```

If `gnuplot` is unavailable, Criterion falls back to the plotters backend.

## Benchmark coverage

The benchmark suite covers:

- insert without eviction
- insert with eviction
- hot read hits
- cold read misses
- mixed read/write workloads
- weighted inserts
- expired TTL reads
- stale reads
- shard scaling
- concurrent read/write workloads

## Operational notes

- Shards are lock partitions, not worker threads.
- The cache is concurrent, not internally parallel.
- More shards are not automatically better. Benchmark your workload.
- The default capacity and weight limits are global.
- The frequency sketch is approximate and intentionally small.
- The eviction policy is sampled, not exhaustive.
- The async loader deduplicates same-key loads, not all loads.
- The cache uses `Arc<V>` internally so reads can return cheap shared handles.

## Production checklist

Before using this on a critical path, run:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --doc
cargo test --all-features
cargo test --release --test stress_test
cargo bench --bench cache_bench -- --measurement-time 15 --sample-size 20
```

Also review:

- expected value sizes
- eviction rate under realistic load
- stale refresh behavior during backend failure
- metrics volume
- desired shard count
- whether strict synchronous capacity enforcement is worth the write-path cost
