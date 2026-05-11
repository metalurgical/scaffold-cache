//! Criterion benchmarks for the main cache paths.
//!
//! The suite separates cheap read paths from write-heavy and eviction-heavy
//! paths so regressions point at a useful subsystem instead of one vague number.
//! Benchmarks explicitly disable local metrics logging because stderr locking
//! pollutes timing results.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use scaffold_cache::{Cache, Weigher};
use std::{sync::Arc, thread, time::Duration};

const SMALL_CAPACITY: usize = 1_000;
const LARGE_CAPACITY: usize = 10_000;
const OPS: u64 = 10_000;

fn default_bench_shards() -> usize {
    thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).clamp(1, 16))
        .unwrap_or(1)
}

#[derive(Clone)]
struct ByteWeigher;

impl Weigher<u64, Vec<u8>> for ByteWeigher {
    fn weight(&self, _key: &u64, value: &Vec<u8>) -> u64 {
        value.len() as u64
    }
}

fn bench_insert_no_eviction(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_no_eviction");
    group.throughput(Throughput::Elements(OPS));

    group.bench_function("u64_u64", |b| {
        b.iter(|| {
            let cache = Cache::builder()
                .max_entries(OPS as usize + 1)
                .shards(default_bench_shards())
                .metrics_log_interval(None)
                .build::<u64, u64>();

            for i in 0..OPS {
                black_box(cache.insert(i, i, None));
            }

            black_box(cache.len());
        });
    });

    group.finish();
}

fn bench_insert_with_eviction(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_with_eviction");
    group.throughput(Throughput::Elements(OPS));

    group.bench_function("capacity_1k", |b| {
        b.iter(|| {
            let cache = Cache::builder()
                .max_entries(SMALL_CAPACITY)
                .shards(default_bench_shards())
                .metrics_log_interval(None)
                .build::<u64, u64>();

            for i in 0..OPS {
                black_box(cache.insert(i, i, None));
            }

            black_box(cache.len());
        });
    });

    group.finish();
}

fn bench_get_hot_hit(c: &mut Criterion) {
    let cache = Cache::builder()
        .max_entries(LARGE_CAPACITY)
        .shards(default_bench_shards())
        .metrics_log_interval(None)
        .build::<u64, u64>();

    for i in 0..OPS {
        cache.insert(i, i, None);
    }

    let mut group = c.benchmark_group("get_hot_hit");
    group.throughput(Throughput::Elements(OPS));

    group.bench_function("100k_hits", |b| {
        b.iter(|| {
            for i in 0..OPS {
                black_box(cache.get(black_box(&i)));
            }
        });
    });

    group.finish();
}

fn bench_get_cold_miss(c: &mut Criterion) {
    let cache = Cache::builder()
        .max_entries(LARGE_CAPACITY)
        .shards(default_bench_shards())
        .metrics_log_interval(None)
        .build::<u64, u64>();

    let mut group = c.benchmark_group("get_cold_miss");
    group.throughput(Throughput::Elements(OPS));

    group.bench_function("100k_misses", |b| {
        b.iter(|| {
            for i in 0..OPS {
                black_box(cache.get(black_box(&(i + OPS))));
            }
        });
    });

    group.finish();
}

fn bench_mixed_read_write(c: &mut Criterion) {
    for read_percent in [90u64, 50u64] {
        let mut group = c.benchmark_group(format!("mixed_{}_read", read_percent));
        group.throughput(Throughput::Elements(OPS));

        group.bench_function("ops", |b| {
            b.iter(|| {
                let cache = Cache::builder()
                    .max_entries(10_000)
                    .shards(default_bench_shards())
                    .metrics_log_interval(None)
                    .build::<u64, u64>();

                for i in 0..10_000 {
                    cache.insert(i, i, None);
                }

                for i in 0..OPS {
                    if i % 100 < read_percent {
                        let key = i % 10_000;
                        black_box(cache.get(black_box(&key)));
                    } else {
                        black_box(cache.insert(i + 10_000, i, None));
                    }
                }

                black_box(cache.len());
            });
        });

        group.finish();
    }
}

fn bench_weighted_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("weighted_insert");
    group.throughput(Throughput::Bytes(OPS * 256));

    group.bench_function("256_byte_values", |b| {
        b.iter(|| {
            let cache = Cache::builder()
                .max_entries(50_000)
                .max_weight(8 * 1024 * 1024)
                .shards(default_bench_shards())
                .metrics_log_interval(None)
                .build_with_weigher::<u64, Vec<u8>, ByteWeigher>(ByteWeigher);

            for i in 0..OPS {
                black_box(cache.insert(i, vec![0u8; 256], None));
            }

            black_box(cache.weight());
        });
    });

    group.finish();
}

fn bench_ttl_expired_get(c: &mut Criterion) {
    let cache = Cache::builder()
        .max_entries(LARGE_CAPACITY)
        .shards(default_bench_shards())
        .metrics_log_interval(None)
        .build::<u64, u64>();

    for i in 0..OPS {
        cache.insert(i, i, Some(Duration::from_millis(1)));
    }

    thread::sleep(Duration::from_millis(5));

    let mut group = c.benchmark_group("ttl_expired_get");
    group.throughput(Throughput::Elements(OPS));

    group.bench_function("expired_reads", |b| {
        b.iter(|| {
            for i in 0..OPS {
                black_box(cache.get(black_box(&i)));
            }
        });
    });

    group.finish();
}

fn bench_stale_get(c: &mut Criterion) {
    let cache = Cache::builder()
        .max_entries(LARGE_CAPACITY)
        .shards(default_bench_shards())
        .stale_while_revalidate(Some(Duration::from_secs(60)))
        .metrics_log_interval(None)
        .build::<u64, u64>();

    for i in 0..OPS {
        cache.insert(i, i, Some(Duration::from_millis(1)));
    }

    thread::sleep(Duration::from_millis(5));

    let mut group = c.benchmark_group("stale_get");
    group.throughput(Throughput::Elements(OPS));

    group.bench_function("stale_reads", |b| {
        b.iter(|| {
            for i in 0..OPS {
                black_box(cache.get_value(black_box(&i)));
            }
        });
    });

    group.finish();
}

fn bench_shard_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_scaling");
    group.throughput(Throughput::Elements(OPS));

    for shards in [1usize, 2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("insert_get", shards),
            &shards,
            |b, &shards| {
                b.iter(|| {
                    let cache = Cache::builder()
                        .max_entries(LARGE_CAPACITY)
                        .shards(shards)
                        .metrics_log_interval(None)
                        .build::<u64, u64>();

                    for i in 0..OPS {
                        cache.insert(i, i, None);
                    }

                    for i in 0..OPS {
                        black_box(cache.get(black_box(&i)));
                    }

                    black_box(cache.len());
                });
            },
        );
    }

    group.finish();
}

fn bench_concurrent_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_read_write");
    group.throughput(Throughput::Elements(OPS));

    for threads in [2usize, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let cache = Arc::new(
                        Cache::builder()
                            .max_entries(10_000)
                            .shards(default_bench_shards())
                            .metrics_log_interval(None)
                            .build::<u64, u64>(),
                    );

                    for i in 0..10_000 {
                        cache.insert(i, i, None);
                    }

                    let ops_per_thread = OPS / threads as u64;

                    let handles: Vec<_> = (0..threads)
                        .map(|thread_id| {
                            let cache = Arc::clone(&cache);

                            thread::spawn(move || {
                                let base = thread_id as u64 * ops_per_thread;

                                for i in 0..ops_per_thread {
                                    let op = base + i;
                                    let key = op % 20_000;

                                    if op.is_multiple_of(10) {
                                        black_box(cache.insert(key, op, None));
                                    } else {
                                        black_box(cache.get(black_box(&key)));
                                    }
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    black_box(cache.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_no_eviction,
    bench_insert_with_eviction,
    bench_get_hot_hit,
    bench_get_cold_miss,
    bench_mixed_read_write,
    bench_weighted_insert,
    bench_ttl_expired_get,
    bench_stale_get,
    bench_shard_scaling,
    bench_concurrent_read_write,
);

criterion_main!(benches);
