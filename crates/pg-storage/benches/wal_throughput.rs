//! WAL sequential-write throughput benchmark.
//!
//! Measures how many MB/s the WAL writer can sustain when appending
//! `FullPageImage` records. The target from the roadmap is ≥ 200 MB/s on a
//! local SSD.
//!
//! NOTE: in the current M1 implementation `WalWriter::append()` synchronously
//! calls `flush_to(lsn)` before returning (writer.rs:184). Therefore every
//! append already waits for its own fsync. The explicit `flush()` at the end
//! of the batch is effectively a no-op for this single-threaded workload.

use std::sync::Arc;
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
    // Large segments avoid rotation overhead during the benchmark; with the
    // current synchronous append() design, group-commit settings do not reduce
    // fsync frequency for a single append thread.
    config.wal_segment_size = 256 * 1024 * 1024; // 256 MB segments

    let wal = Arc::new(WalWriter::open(tmp.path(), &config).unwrap());
    (tmp, wal)
}

fn wal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_throughput");

    // Each FullPageImage record carries an 8 KB page image plus headers.
    let record_size = WalRecord::full_page_image(PageId(1), vec![0u8; PAGE_SIZE])
        .unwrap()
        .encode()
        .unwrap()
        .len();
    let bytes_per_batch = record_size * BATCH_SIZE;

    group.throughput(Throughput::Bytes(bytes_per_batch as u64));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    // Measures sequential single-threaded append throughput. Because
    // WalWriter::append() internally calls flush_to(lsn), each record is
    // fsynced before append() returns. The trailing flush() is therefore a
    // no-op for this workload but is kept to mirror the call pattern used by
    // BufferPool::flush and the checkpoint coordinator.
    //
    // Note: setup() is called per-iteration but its time is excluded from the
    // measurement by Criterion. Each iteration measures only the inner closure.
    group.bench_with_input(
        BenchmarkId::new("append_and_flush", bytes_per_batch),
        &bytes_per_batch,
        |b, _| {
            b.iter_with_setup(setup, |(_tmp, wal)| {
                for i in 0..BATCH_SIZE {
                    // WAL payload does not require the page to have been
                    // allocated; use monotonically increasing ids for the image.
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

criterion_group!(benches, wal_throughput);
criterion_main!(benches);
