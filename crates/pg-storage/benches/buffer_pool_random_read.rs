//! Buffer pool random-read benchmark.
//!
//! Measures random `pin` throughput (ops/s) at 100% hit rate. The roadmap
//! target is ≥ 50K ops/s for 8 KB pages.

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pg_storage::buffer_pool::BufferPool;
use pg_storage::config::StorageConfig;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::types::PageId;
use pg_storage::wal::writer::WalWriter;

const WORKING_SET: usize = 64; // pages
const BATCH_SIZE: usize = 10000; // pin ops per iteration

fn setup() -> (
    tempfile::TempDir,
    Arc<parking_lot::Mutex<PageAllocator>>,
    Arc<WalWriter>,
    BufferPool,
) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    // Large enough to hold the working set in memory.
    config.buffer_pool_size = 8 * 1024 * 1024; // 8 MB = 1024 frames at 8 KB
                                               // Aggressive WAL settings for setup speed: the measured loop is read-only,
                                               // so these do not affect the result, but they reduce the latency of
                                               // allocating and flushing the working set during setup.
    config.wal_group_commit_timeout_ms = 1;
    config.wal_group_commit_batch_size = 1;

    let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
    let allocator = Arc::new(parking_lot::Mutex::new(
        PageAllocator::open(tmp.path(), &config, Arc::clone(&wal)).unwrap(),
    ));
    let pool = BufferPool::open(
        tmp.path(),
        &config,
        Arc::clone(&allocator),
        Arc::clone(&wal),
    )
    .unwrap();

    // Allocate and flush the working set so it is durable on disk.
    for _ in 0..WORKING_SET {
        let guard = pool.new_page().unwrap();
        let _ = guard.page_id();
    }
    for i in 0..WORKING_SET {
        pool.flush(PageId(i as u64 + 1)).unwrap();
    }

    // Warm up: load the entire working set into the buffer pool so
    // the measured loop runs at ~100% hit rate.
    for i in 0..WORKING_SET {
        let _ = pool.pin(PageId(i as u64 + 1)).unwrap();
    }

    (tmp, allocator, wal, pool)
}

/// Simple deterministic LCG for reproducible random page access.
fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(1103515245).wrapping_add(12345);
    *state
}

fn buffer_pool_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool_random_read");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    // Note: setup() is called per-iteration but its time is excluded from the
    // measurement by Criterion. Each iteration measures only the inner pin loop.
    group.bench_with_input(
        BenchmarkId::new("random_pin", BATCH_SIZE),
        &BATCH_SIZE,
        |b, _| {
            b.iter_with_setup(setup, |(_tmp, _allocator, _wal, pool)| {
                let mut rng: u64 = 42;
                let mut sum: u64 = 0;
                for _ in 0..BATCH_SIZE {
                    let id = (next_random(&mut rng) % WORKING_SET as u64) + 1;
                    let guard = pool.pin(PageId(id)).unwrap();
                    // Touch a few bytes so the read is not entirely optimized away.
                    sum += guard.page()[0] as u64;
                }
                black_box(sum);
            });
        },
    );

    group.finish();
}

criterion_group!(benches, buffer_pool_random_read);
criterion_main!(benches);
