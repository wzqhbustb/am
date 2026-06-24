//! Verifies that page allocation is append-only (fsync-deferred): the fix that
//! removed the per-allocation `flush_to` so a transaction's allocations can be
//! amortized into a single fsync at commit time.
//!
//! This test does not measure wall-clock throughput (too flaky in CI); it
//! proves the *batching mechanism* exists by observing `synced_lsn`, which only
//! advances on fsync. With the group-commit worker configured never to fire on
//! its own, `synced_lsn` must stay at 0 across hundreds of allocations — if
//! `alloc_page`/`free_page` fsynced per call (the old behavior), it would
//! advance on every call and the assertion below would fail. Durability is
//! then confirmed by dropping the writer (which flushes all pending records on
//! shutdown) and reopening the WAL to read every record back.

use std::sync::Arc;

use pg_storage::config::StorageConfig;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::types::Lsn;
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::WalRecordType;
use pg_storage::wal::writer::WalWriter;

/// A config whose group-commit worker will not fire on its own during the
/// test: a huge batch threshold and a multi-hour timeout. The only sync that
/// can occur is the flush the worker performs on shutdown (Drop), which fires
/// regardless of these thresholds.
fn no_autoflush_config(dir: &std::path::Path) -> StorageConfig {
    let mut cfg = StorageConfig::new(dir);
    cfg.wal_group_commit_batch_size = 1_000_000;
    cfg.wal_group_commit_timeout_ms = 3_600_000;
    cfg
}

#[test]
fn alloc_and_free_are_append_only_and_durable_on_shutdown() {
    let tmp = tempfile::TempDir::new().unwrap();
    let cfg = no_autoflush_config(tmp.path());
    let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
    let mut allocator = PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap();

    // Nothing synced yet.
    assert_eq!(wal.synced_lsn(), Lsn(0));

    // Allocate many pages: each appends a PageAlloc record but must NOT fsync,
    // so synced_lsn stays at 0 across the whole batch.
    const N: u64 = 500;
    let mut ids = Vec::with_capacity(N as usize);
    for _ in 0..N {
        ids.push(allocator.alloc_page().unwrap());
    }
    assert_eq!(
        wal.synced_lsn(),
        Lsn(0),
        "append-only alloc_page must not advance synced_lsn; per-alloc fsync regressed"
    );

    // Free half of them: free_page is append-only too.
    for &id in ids.iter().take((N / 2) as usize) {
        allocator.free_page(id).unwrap();
    }
    assert_eq!(
        wal.synced_lsn(),
        Lsn(0),
        "append-only free_page must not advance synced_lsn; per-free fsync regressed"
    );

    // Shut down: dropping the writer flushes all pending records in one sync.
    drop(allocator);
    drop(wal);

    // Reopen the WAL and confirm every appended record is durable — proving the
    // single shutdown flush amortized all N allocations + N/2 frees.
    let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
    let mut allocs = 0u64;
    let mut frees = 0u64;
    while let Some(record) = reader.next_record().unwrap() {
        match record.record_type {
            WalRecordType::PageAlloc => allocs += 1,
            WalRecordType::PageFree => frees += 1,
            _ => {}
        }
    }
    assert_eq!(
        allocs, N,
        "all allocations must be durable after shutdown flush"
    );
    assert_eq!(
        frees,
        N / 2,
        "all frees must be durable after shutdown flush"
    );
}
