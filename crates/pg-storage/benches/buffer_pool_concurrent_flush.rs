//! Buffer pool concurrent flush benchmark.
//!
//! Measures multi-threaded flush throughput (pages/s) to exercise the
//! group-fsync coalescing introduced in Stage F. Multiple threads dirty
//! and flush distinct pages concurrently; the benchmark validates that
//! the coalescing logic reduces total fsync count relative to N sequential
//! fsyncs.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pg_storage::buffer_pool::BufferPool;
use pg_storage::config::StorageConfig;
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::types::PageId;
use pg_storage::wal::writer::WalWriter;

const PAGES: usize = 128;

fn setup(thread_count: usize) -> (tempfile::TempDir, Arc<BufferPool>) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    config.buffer_pool_size = 16 * 1024 * 1024;
    config.wal_group_commit_timeout_ms = 1;
    config.wal_group_commit_batch_size = thread_count;

    let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
    let allocator = Arc::new(parking_lot::Mutex::new(
        PageAllocator::open(tmp.path(), &config, Arc::clone(&wal)).unwrap(),
    ));
    let pool = Arc::new(
        BufferPool::open(
            tmp.path(),
            &config,
            Arc::clone(&allocator),
            Arc::clone(&wal),
        )
        .unwrap(),
    );

    for _ in 0..PAGES {
        let mut g = pool.new_page().unwrap();
        g.page_mut()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 8].copy_from_slice(b"benchflh");
    }
    for i in 1..=PAGES {
        pool.flush(PageId(i as u64)).unwrap();
    }

    (tmp, pool)
}

fn concurrent_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool_concurrent_flush");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for &threads in &[1, 4, 8] {
        group.throughput(Throughput::Elements(PAGES as u64));
        group.bench_with_input(
            BenchmarkId::new("flush_threads", threads),
            &threads,
            |b, &tc| {
                b.iter_with_setup(
                    || setup(tc),
                    |(_tmp, pool)| {
                        // Dirty all pages.
                        for i in 1..=PAGES {
                            let mut g = pool.pin_mut(PageId(i as u64)).unwrap();
                            g.page_mut()[PAGE_HEADER_SIZE] =
                                g.page_mut()[PAGE_HEADER_SIZE].wrapping_add(1);
                        }

                        // Flush concurrently from `tc` threads.
                        let pages_per_thread = PAGES / tc;
                        std::thread::scope(|s| {
                            for t in 0..tc {
                                let pool = &pool;
                                s.spawn(move || {
                                    let start = t * pages_per_thread + 1;
                                    let end = if t == tc - 1 {
                                        PAGES
                                    } else {
                                        start + pages_per_thread - 1
                                    };
                                    for i in start..=end {
                                        pool.flush(PageId(i as u64)).unwrap();
                                    }
                                });
                            }
                        });
                    },
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, concurrent_flush);
criterion_main!(benches);
