//! WAL group-commit throughput benchmark.
//!
//! Measures how many MB/s the WAL writer can sustain when appending
//! `FullPageImage` records in batches (N appends → 1 fsync). The target from
//! the roadmap is ≥ 200 MB/s on a local SSD.
//!
//! NOTE (Stage B): `WalWriter::append()` no longer fsyncs implicitly
//! (writer.rs). The caller controls durability via `flush()` / `flush_to()`.
//! This benchmark exercises the intended group-commit pattern.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pg_storage::config::StorageConfig;
use pg_storage::types::{PageId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

const BATCH_SIZE: usize = 64;

fn setup() -> (tempfile::TempDir, Arc<WalWriter>) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    config.wal_segment_size = 256 * 1024 * 1024; // 256 MB segments
    config.wal_group_commit_batch_size = 8;
    config.wal_group_commit_timeout_ms = 5;

    let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
    (tmp, wal)
}

fn wal_group_commit_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_group_commit");

    let record_size = WalRecord::full_page_image(PageId(1), vec![0u8; PAGE_SIZE])
        .unwrap()
        .encode()
        .unwrap()
        .len();
    let bytes_per_batch = record_size * BATCH_SIZE;

    group.throughput(Throughput::Bytes(bytes_per_batch as u64));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("single_thread_batch_64", bytes_per_batch),
        &bytes_per_batch,
        |b, _| {
            b.iter_with_setup(setup, |(_tmp, wal)| {
                for i in 0..BATCH_SIZE {
                    let page_id = PageId(i as u64 + 1);
                    let image = vec![(i % 256) as u8; PAGE_SIZE];
                    let record = WalRecord::full_page_image(page_id, image).unwrap();
                    wal.append(record).unwrap();
                }
                wal.flush().unwrap();
            });
        },
    );

    group.finish();
}

fn wal_group_commit_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_group_commit_concurrent");

    let record_size = WalRecord::full_page_image(PageId(1), vec![0u8; PAGE_SIZE])
        .unwrap()
        .encode()
        .unwrap()
        .len();
    let threads = 100;
    let records_per_thread = 8;
    let total_bytes = record_size * threads * records_per_thread;

    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("100_threads_x_8_records", total_bytes),
        &total_bytes,
        |b, _| {
            b.iter_with_setup(setup, |(_tmp, wal)| {
                let handles: Vec<_> = (0..threads)
                    .map(|t| {
                        let wal = Arc::clone(&wal);
                        thread::spawn(move || {
                            for i in 0..records_per_thread {
                                let page_id = PageId((t * records_per_thread + i) as u64 + 1);
                                let image = vec![((t + i) % 256) as u8; PAGE_SIZE];
                                let record = WalRecord::full_page_image(page_id, image).unwrap();
                                wal.append(record).unwrap();
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
                wal.flush().unwrap();
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    wal_group_commit_single_thread,
    wal_group_commit_concurrent
);
criterion_main!(benches);
