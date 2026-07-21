//! Integration tests for Stage E: Freelist CRC + WAL rebuild.
//!
//! Guards three invariants:
//! - CRC32 on `freelist.meta` detects corruption (hard failure, not silent).
//! - Recovery catches corruption and rebuilds the freelist from WAL replay.
//! - A valid snapshot accelerates recovery by seeding the freelist.

use std::sync::Arc;

use parking_lot::Mutex;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::error::StorageError;
use pg_storage::freelist_meta::FreelistMeta;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::types::PageId;
use pg_storage::wal::writer::WalWriter;

// ---------------------------------------------------------------------------
// CRC roundtrip + corruption detection
// ---------------------------------------------------------------------------

#[test]
fn test_freelist_crc_roundtrip() {
    let meta = FreelistMeta {
        checkpoint_lsn: pg_storage::types::Lsn(1024),
        page_ids: vec![PageId(3), PageId(7), PageId(42)],
    };
    let encoded = meta.encode();
    let decoded = FreelistMeta::decode(&encoded).unwrap();
    assert_eq!(meta, decoded);
}

#[test]
fn test_freelist_corrupted_returns_hard_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = FreelistMeta::path(tmp.path());

    let meta = FreelistMeta {
        checkpoint_lsn: pg_storage::types::Lsn(512),
        page_ids: vec![PageId(1), PageId(2), PageId(3)],
    };
    meta.write(&path).unwrap();

    // Corrupt a byte in the body (after the 4-byte CRC prefix).
    let raw = std::fs::read(&path).unwrap();
    assert!(raw.len() > 4);
    let mut corrupted = raw.clone();
    corrupted[10] ^= 0xFF;
    std::fs::write(&path, &corrupted).unwrap();

    let err = FreelistMeta::read(&path).unwrap_err();
    assert!(
        matches!(err, StorageError::MetadataCorrupted(ref msg) if msg.contains("CRC")),
        "expected CRC mismatch error, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// WAL rebuild after corruption
// ---------------------------------------------------------------------------

/// Helper: allocate `n` pages, checkpoint, then free the first `k`.
/// PageFree records are post-checkpoint so WAL replay from checkpoint_lsn
/// sees them.
fn setup_engine_with_freed_pages(
    data_dir: &std::path::Path,
    n: u32,
    k: u32,
) -> (Vec<PageId>, Vec<PageId>) {
    let config = StorageConfig::new(data_dir);
    let engine = StorageEngine::open(data_dir, &config).unwrap();

    let allocated: Vec<PageId> = (0..n)
        .map(|_| engine.page_allocator().lock().alloc_page().unwrap())
        .collect();

    // Checkpoint first so subsequent PageFree records are post-checkpoint.
    engine.trigger_checkpoint().unwrap();

    let freed: Vec<PageId> = allocated[..k as usize]
        .iter()
        .map(|&pid| {
            engine.page_allocator().lock().free_page(pid).unwrap();
            pid
        })
        .collect();

    // Do NOT checkpoint again: the snapshot stays at the pre-free state,
    // so recovery must rebuild the freelist from WAL replay.
    engine.shutdown();
    drop(engine);

    (allocated, freed)
}

#[test]
fn test_freelist_rebuild_from_wal() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (allocated, freed) = setup_engine_with_freed_pages(tmp.path(), 5, 3);

    // Corrupt freelist.meta so recovery must rebuild from WAL.
    let fl_path = FreelistMeta::path(tmp.path());
    if fl_path.exists() {
        let mut data = std::fs::read(&fl_path).unwrap();
        data[10] ^= 0xFF;
        std::fs::write(&fl_path, &data).unwrap();
    }

    // Recover.
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // The freelist must match the freed pages (order-insensitive).
    let recovered_fl: Vec<PageId> = engine.page_allocator().lock().freelist().to_vec();
    let mut expected = freed.clone();
    let mut actual = recovered_fl.clone();
    expected.sort_by_key(|p| p.0);
    actual.sort_by_key(|p| p.0);
    assert_eq!(
        actual, expected,
        "freelist after WAL rebuild must match freed pages"
    );

    // next_page_id must be correct (one past the last allocated).
    let last = allocated.last().unwrap();
    assert_eq!(
        engine.page_allocator().lock().next_page_id(),
        PageId(last.0 + 1)
    );
}

// ---------------------------------------------------------------------------
// Snapshot acceleration: valid snapshot seeds the freelist
// ---------------------------------------------------------------------------

#[test]
fn test_freelist_snapshot_accelerates_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();

    // Allocate + free + checkpoint, so the snapshot captures the freed pages.
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let p1 = engine.page_allocator().lock().alloc_page().unwrap();
    let p2 = engine.page_allocator().lock().alloc_page().unwrap();
    let _p3 = engine.page_allocator().lock().alloc_page().unwrap();
    engine.page_allocator().lock().free_page(p1).unwrap();
    engine.page_allocator().lock().free_page(p2).unwrap();
    engine.trigger_checkpoint().unwrap();
    engine.shutdown();
    drop(engine);

    // The freelist.meta written by checkpoint must be valid (no corruption).
    let fl_path = FreelistMeta::path(tmp.path());
    assert!(
        fl_path.exists(),
        "checkpoint must have written freelist.meta"
    );
    let snap = FreelistMeta::read(&fl_path).unwrap();
    assert_eq!(
        snap.page_ids.len(),
        2,
        "snapshot must contain 2 freed pages"
    );

    // Recover — the snapshot should seed the allocator, and WAL replay
    // confirms it (no additional PageFree records post-checkpoint).
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let recovered_fl: Vec<PageId> = engine.page_allocator().lock().freelist().to_vec();
    let mut expected = snap.page_ids.clone();
    let mut actual = recovered_fl.clone();
    expected.sort_by_key(|p| p.0);
    actual.sort_by_key(|p| p.0);
    assert_eq!(actual, expected, "recovered freelist must match snapshot");
}

// ---------------------------------------------------------------------------
// End-to-end: free_page → recover → reuse
// ---------------------------------------------------------------------------

#[test]
fn test_freed_page_is_reusable_after_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();

    // Allocate 3 pages, free page 2, checkpoint (snapshot captures the free).
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let p1 = engine.page_allocator().lock().alloc_page().unwrap();
    let p2 = engine.page_allocator().lock().alloc_page().unwrap();
    let p3 = engine.page_allocator().lock().alloc_page().unwrap();
    assert_eq!((p1, p2, p3), (PageId(1), PageId(2), PageId(3)));
    engine.page_allocator().lock().free_page(p2).unwrap();
    engine.trigger_checkpoint().unwrap();
    engine.shutdown();
    drop(engine);

    // Recover.
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // The next alloc must reuse the freed page (page 2), not page 4.
    let reused = engine.page_allocator().lock().alloc_page().unwrap();
    assert_eq!(
        reused,
        PageId(2),
        "freed page must be reused after recovery"
    );
    assert_eq!(
        engine.page_allocator().lock().next_page_id(),
        PageId(4),
        "next_page_id must stay at 4"
    );
}

// ---------------------------------------------------------------------------
// Direct allocator test: free_page writes WAL, replay rebuilds freelist
// ---------------------------------------------------------------------------

#[test]
fn test_page_free_wal_record_replays_correctly() {
    let tmp = tempfile::TempDir::new().unwrap();
    let cfg = StorageConfig::new(tmp.path());
    let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
    let allocator = Arc::new(Mutex::new(
        PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
    ));

    // Allocate 5 pages, free 2 of them.
    let pages: Vec<PageId> = (0..5)
        .map(|_| allocator.lock().alloc_page().unwrap())
        .collect();
    allocator.lock().free_page(pages[1]).unwrap();
    allocator.lock().free_page(pages[3]).unwrap();
    let expected_fl = vec![pages[1], pages[3]];

    // Drop everything (WAL is already fsynced by free_page/alloc_page).
    drop(allocator);
    drop(wal);

    // Reopen allocator and replay WAL from the beginning.
    let cfg2 = StorageConfig::new(tmp.path());
    let wal2 = Arc::new(WalWriter::open(tmp.path(), &cfg2).unwrap());
    let mut recovered = PageAllocator::open_at(tmp.path(), &cfg2, wal2, PageId(1)).unwrap();

    // Read WAL from the start and replay every record.
    let mut reader = pg_storage::wal::reader::WalReader::open_at(
        tmp.path().join("wal"),
        cfg2.wal_segment_size,
        pg_storage::types::Lsn::FIRST,
    )
    .unwrap();
    while let Some(record) = reader.next_record().unwrap() {
        recovered.replay_record(&record).unwrap();
    }
    recovered.mark_recovery_complete();

    let mut actual: Vec<PageId> = recovered.freelist().to_vec();
    let mut expected = expected_fl.clone();
    actual.sort_by_key(|p| p.0);
    expected.sort_by_key(|p| p.0);
    assert_eq!(actual, expected, "WAL replay must rebuild freelist");
    assert_eq!(recovered.next_page_id(), PageId(6));
}

// ---------------------------------------------------------------------------
// Concurrent free_page during checkpoint: no duplicate freelist entries
// ---------------------------------------------------------------------------

/// Regression test for the P1-1 race: `free_page` running concurrently with
/// `trigger_checkpoint` must not produce duplicate freelist entries on
/// recovery.
///
/// The invariant: a PageFree record is in the freelist snapshot IFF its LSN <
/// `begin_lsn`. If a `free_page` interleaves between `reserve_lsn` and
/// `snapshot`, the freed page would appear in both the snapshot (in-memory
/// push) and WAL replay (post-begin_lsn record), producing a duplicate.
///
/// The fix atomizes `reserve_lsn` + `set_checkpoint_lsn` + `snapshot` under
/// the `page_allocator` lock. This test exercises the race by dirtying many
/// pages (extending the flush phase) and freeing pages concurrently with the
/// checkpoint.
#[test]
fn test_concurrent_free_during_checkpoint_no_duplicates() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // Allocate pages.
    let pages: Vec<PageId> = (0..50)
        .map(|_| engine.page_allocator().lock().alloc_page().unwrap())
        .collect();

    // Dirty many pages to extend the checkpoint's flush phase, creating a
    // wider window for free_page to race with the checkpoint.
    for &pid in &pages {
        let mut guard = engine.buffer_pool().pin_mut(pid).unwrap();
        guard.page_mut()[0] = 1;
    }

    // Pages to free concurrently (subset of allocated pages).
    let to_free: Vec<PageId> = pages.iter().copied().take(20).collect();

    // Run checkpoint and free_page concurrently.
    let freed = to_free.clone();
    std::thread::scope(|s| {
        s.spawn(|| {
            engine.trigger_checkpoint().unwrap();
        });
        s.spawn(|| {
            for &pid in &freed {
                engine.page_allocator().lock().free_page(pid).unwrap();
            }
        });
    });

    engine.shutdown();
    drop(engine);

    // Recover.
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // The freelist must contain exactly the freed pages — no duplicates.
    let recovered_fl: Vec<PageId> = engine.page_allocator().lock().freelist().to_vec();

    // Check no duplicates.
    let mut seen = std::collections::HashSet::new();
    for &pid in &recovered_fl {
        assert!(
            seen.insert(pid),
            "duplicate page {pid} in recovered freelist: {:?}",
            recovered_fl
        );
    }

    // Check the freelist matches the freed pages (order-insensitive).
    let mut expected = to_free.clone();
    let mut actual = recovered_fl.clone();
    expected.sort_by_key(|p| p.0);
    actual.sort_by_key(|p| p.0);
    assert_eq!(
        actual, expected,
        "recovered freelist must match freed pages exactly"
    );
}

// ---------------------------------------------------------------------------
// Double-free and invalid free_page: runtime protection
// ---------------------------------------------------------------------------

#[test]
fn test_free_page_rejects_double_free() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let pid = engine.page_allocator().lock().alloc_page().unwrap();
    engine.page_allocator().lock().free_page(pid).unwrap();

    // Second free of the same page must fail with InvalidOperation.
    let err = engine.page_allocator().lock().free_page(pid).unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidOperation(ref msg) if msg.contains("double-free")),
        "expected InvalidOperation double-free error, got {err:?}"
    );

    // The freelist must still contain the page exactly once.
    let fl: Vec<PageId> = engine.page_allocator().lock().freelist().to_vec();
    assert_eq!(fl.iter().filter(|&&p| p == pid).count(), 1);
}

#[test]
fn test_free_page_rejects_invalid_and_unallocated() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // Freeing PageId::INVALID must fail.
    let err = engine
        .page_allocator()
        .lock()
        .free_page(PageId::INVALID)
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidOperation(_)),
        "expected InvalidOperation for PageId::INVALID, got {err:?}"
    );

    // Freeing a page that was never allocated must fail.
    let err = engine
        .page_allocator()
        .lock()
        .free_page(PageId(999))
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidOperation(ref msg) if msg.contains("never allocated")),
        "expected InvalidOperation for unallocated page, got {err:?}"
    );
}
