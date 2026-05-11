# Architecture

This document explains how the cache is put together and why the main pieces exist.

## Core idea

The cache is a bounded in-process map. Reads should be cheap. Writes should remain safe under concurrency. Capacity must be global, because fixed per-shard quotas waste space when keys are not evenly distributed.

The implementation is deliberately conservative: insertion and eviction stay synchronous so the cache does not publish an entry and then clean up later. That keeps capacity behavior predictable and makes stress testing easier.

## Main modules

- `cache.rs` owns the public cache API and global coordination.
- `config.rs` owns builder configuration.
- `entry.rs` stores value metadata such as TTL, stale window, weight, and last access tick.
- `shard.rs` owns each shard-local hash map.
- `frequency.rs` tracks approximate key popularity for admission and eviction.
- `weight.rs` defines how entry cost is measured.
- `stats.rs` tracks operational counters and optional local logging.
- `loader.rs` adds async loading, request coalescing, stale refresh, and backpressure.

## Sharding

Each shard is a hash map protected by an `RwLock`. A key is hashed to one shard. This allows unrelated keys to proceed independently most of the time.

Shards do not own worker threads. They are only lock partitions. The cache is concurrent because many caller threads can use it at the same time, but a single cache operation is not internally split into parallel work.

## Global capacity

Length and weight are tracked with atomics:

- `global_len`
- `global_weight`

These are the authoritative totals. They avoid the expensive old approach of summing every shard whenever the cache needed to check capacity.

Capacity-changing operations still use `capacity_lock`. That lock is intentional. It keeps admission, eviction, and accounting in one strict sequence so callers do not observe capacity overshoot during normal insertion.

## Eviction

Eviction is sampled globally:

1. Each shard can provide a candidate victim.
2. Candidates are scored using frequency and recency.
3. The weakest candidate is removed.
4. Global length and weight atomics are updated immediately.

The inserted key is excluded from victim selection during insertion. If insertion succeeds, the caller can immediately read the inserted value back.

## Admission

The frequency sketch gives an approximate popularity score for a key. When the projected insert would exceed capacity, the cache compares the candidate key against a sampled victim. If the candidate looks weaker, the insert can be rejected.

This is TinyLFU-style admission, not a full Window TinyLFU policy. There are no window/probation/protected segments.

## TTL and stale windows

Entries can be in one of three useful states:

- fresh: TTL has not expired
- stale but servable: TTL expired, but stale window has not ended
- dead: both TTL and stale window are over

The plain cache can report fresh or stale values. The async loader adds the behavior people usually expect from stale-while-revalidate: return stale now, refresh in the background.

## Async loader

The loader solves two problems:

- many callers missing the same key at the same time
- many callers seeing the same stale key at the same time

For misses, it uses a per-key async mutex so only one load for that key runs at a time.

For stale refreshes, it tracks a `refreshing` set so only one background refresh is spawned for a key.

`with_max_concurrent_loads` adds a semaphore around loader calls. This protects the backend from too many simultaneous loads.

## Metrics

Metrics are atomic counters. Snapshots are cheap and intentionally approximate in the usual concurrent-counter sense. They are good for operational visibility, not for transaction accounting.

The local metrics logger is simple stderr logging. It is useful during development, but benchmarks should disable it with `metrics_log_interval(None)`.

## Shutdown

The cache owns a janitor thread when `janitor_interval` is enabled. `Drop` signals shutdown, unparks the thread, and joins it. The metrics logger follows the same shutdown pattern.

## Performance lessons from benchmarking

The biggest improvement came from replacing global shard scans with authoritative global atomics. That made inserts and concurrent workloads dramatically faster.

Eviction is still more expensive than normal insertion because it has to sample across shards and remove victims synchronously. That is the price of strict bounded capacity.

## Tradeoffs

The implementation favors:

- strict capacity behavior
- understandable concurrency
- practical performance
- testability

It does not attempt:

- lock-free reads
- asynchronous eviction
- distributed caching
- background write pipelines
