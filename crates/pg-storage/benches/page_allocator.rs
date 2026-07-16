//! Page allocator benchmark.
//!
//! Measures `alloc_page` throughput (ops/s). M1 has no `free_page`, so the
//! benchmark only covers allocation.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pg_storage::config::StorageConfig;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::wal::writer::WalWriter;

const BATCH_SIZE: usize = 100;

fn setup() -> (
    tempfile::TempDir,
    Arc<WalWriter>,
    parking_lot::Mutex<PageAllocator>,
) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    config.wal_group_commit_timeout_ms = 1;
    config.wal_group_commit_batch_size = 64;

    let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
    let allocator = parking_lot::Mutex::new(
        PageAllocator::open(tmp.path(), &config, Arc::clone(&wal)).unwrap(),
    );

    (tmp, wal, allocator)
}

fn page_allocator_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_allocator");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // Note: setup() is called per-iteration but its time is excluded from the
    // measurement by Criterion. Each iteration measures only the inner alloc loop.
    group.bench_with_input(
        BenchmarkId::new("alloc_page", BATCH_SIZE),
        &BATCH_SIZE,
        |b, _| {
            b.iter_with_setup(setup, |(_tmp, _wal, allocator)| {
                for _ in 0..BATCH_SIZE {
                    let mut a = allocator.lock();
                    a.alloc_page().unwrap();
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, page_allocator_alloc);
criterion_main!(benches);
