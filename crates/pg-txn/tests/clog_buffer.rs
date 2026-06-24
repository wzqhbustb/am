//! M2b Stage L: disk-backed `ClogBuffer` (SLRU) integration tests.
//!
//! Covers the Stage L task table (coding-plan §Stage L):
//! - CLOG segment format: 4-bit states, high-nibble=even / low-nibble=odd
//!   XID bit order (tech-selection §6.2), segment/page/offset math;
//! - `ClogBuffer` semantics: unknown XID → `InProgress`, round-trip, miss
//!   load, clock-sweep eviction with dirty writeback;
//! - flush discipline (§6.4, v2.3-21): `set_state` is memory-only until the
//!   checkpoint hook (`ClogFlush::flush_dirty`) runs — never before;
//! - crash recovery: checkpoint-persisted state survives reopen without WAL;
//!   the txn redo handlers rebuild the disk CLOG from WAL after a crash.

use std::path::Path;
use std::sync::Arc;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::positioned_file::PositionedFile;
use pg_storage::types::TxnId;
use pg_txn::clog_file::{segment_path, XIDS_PER_CLOG_PAGE, XIDS_PER_SEGMENT};
use pg_txn::{txn_redo_handlers, ClogAccessor, ClogBuffer, CommitWal, TxnManager, TxnState};

/// Read one raw byte straight from a CLOG segment file, bypassing the
/// buffer — the ground truth for "is it on disk yet".
fn pread_byte(data_dir: &Path, segment_id: u64, offset: u64) -> u8 {
    let path = segment_path(&data_dir.join("clog"), segment_id);
    let pf = PositionedFile::open(&path).unwrap();
    let mut byte = [0u8; 1];
    pf.read_exact_at(&mut byte, offset).unwrap();
    byte[0]
}

// ---------------------------------------------------------------------------
// Bit order (§6.2): high 4 bits = even XID, low 4 bits = odd XID.
// ---------------------------------------------------------------------------

/// The §6.2 bit-order contract, verified against the actual bytes on disk.
/// Writing xid=2N / 2N+1 as Committed/Aborted must produce byte N = 0x12;
/// if the nibble direction were flipped the whole CLOG would be mirrored.
#[test]
fn bit_order_high_nibble_even_xid_low_nibble_odd_xid() {
    let tmp = tempfile::TempDir::new().unwrap();

    // xid 2 (even) = Committed (0b0001) → high nibble;
    // xid 3 (odd)  = Aborted   (0b0010) → low nibble. Byte 1 = 0x12.
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    clog.set_state(TxnId(2), TxnState::Committed);
    clog.set_state(TxnId(3), TxnState::Aborted);
    clog.flush_dirty().unwrap();
    assert_eq!(pread_byte(tmp.path(), 0, 1), 0x12);

    // Same byte, reversed states: xid 2 Aborted, xid 3 Committed → 0x21.
    clog.set_state(TxnId(2), TxnState::Aborted);
    clog.set_state(TxnId(3), TxnState::Committed);
    clog.flush_dirty().unwrap();
    assert_eq!(pread_byte(tmp.path(), 0, 1), 0x21);
    assert_eq!(clog.get_state(TxnId(2)), TxnState::Aborted);
    assert_eq!(clog.get_state(TxnId(3)), TxnState::Committed);

    // A lone even XID touches only the high nibble; a lone odd XID only the
    // low nibble. xid 4 (even) Committed + xid 5 (odd) Aborted → 0x12 in
    // byte 2, leaving byte 1 = 0x21 undisturbed.
    clog.set_state(TxnId(4), TxnState::Committed);
    clog.set_state(TxnId(5), TxnState::Aborted);
    clog.flush_dirty().unwrap();
    assert_eq!(pread_byte(tmp.path(), 0, 2), 0x12);
    assert_eq!(pread_byte(tmp.path(), 0, 1), 0x21);
}

// ---------------------------------------------------------------------------
// Segment / page / offset math on boundary XIDs.
// ---------------------------------------------------------------------------

/// Boundary XIDs land in the expected segment, page, and nibble — checked
/// behaviorally against the bytes on disk (the pure math is unit-tested in
/// `clog_file::tests`).
#[test]
fn boundary_xids_land_in_expected_segment_and_page() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), 8).unwrap();

    // Last XID of page 0 and first XID of page 1 share nothing.
    clog.set_state(TxnId(XIDS_PER_CLOG_PAGE - 1), TxnState::Aborted); // page 0, byte 8191, low
    clog.set_state(TxnId(XIDS_PER_CLOG_PAGE), TxnState::Committed); // page 1, byte 0, high
                                                                    // Last XID of segment 0 and first XID of segment 1.
    clog.set_state(TxnId(XIDS_PER_SEGMENT - 1), TxnState::Aborted); // seg 0, last byte, low
    clog.set_state(TxnId(XIDS_PER_SEGMENT), TxnState::Committed); // seg 1, byte 0, high
    clog.flush_dirty().unwrap();

    assert_eq!(pread_byte(tmp.path(), 0, 8191), 0x02);
    assert_eq!(pread_byte(tmp.path(), 0, 8192), 0x10);
    assert_eq!(pread_byte(tmp.path(), 0, 128 * 1024 * 1024 - 1), 0x02);
    assert_eq!(pread_byte(tmp.path(), 1, 0), 0x10);

    // Round-trip through the cache (page 16383/16384 segment crossing).
    assert_eq!(
        clog.get_state(TxnId(XIDS_PER_CLOG_PAGE - 1)),
        TxnState::Aborted
    );
    assert_eq!(
        clog.get_state(TxnId(XIDS_PER_CLOG_PAGE)),
        TxnState::Committed
    );
    assert_eq!(
        clog.get_state(TxnId(XIDS_PER_SEGMENT - 1)),
        TxnState::Aborted
    );
    assert_eq!(clog.get_state(TxnId(XIDS_PER_SEGMENT)), TxnState::Committed);
}

// ---------------------------------------------------------------------------
// ClogAccessor semantics (same contract as the M2a in-memory CLOG).
// ---------------------------------------------------------------------------

#[test]
fn unknown_xid_reads_in_progress() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    for xid in [0u64, 1, 16384, XIDS_PER_SEGMENT, u64::MAX - 1] {
        assert_eq!(
            clog.get_state(TxnId(xid)),
            TxnState::InProgress,
            "xid {xid} must read InProgress before any set_state"
        );
    }
}

#[test]
fn set_then_get_round_trips() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    clog.set_state(TxnId(5), TxnState::Committed);
    clog.set_state(TxnId(6), TxnState::Aborted);
    clog.set_state(TxnId(700_000), TxnState::Committed);
    assert_eq!(clog.get_state(TxnId(5)), TxnState::Committed);
    assert_eq!(clog.get_state(TxnId(6)), TxnState::Aborted);
    assert_eq!(clog.get_state(TxnId(700_000)), TxnState::Committed);
    assert_eq!(clog.get_state(TxnId(7)), TxnState::InProgress);
}

/// A page not resident in any frame is loaded from the segment file on
/// first access (miss), then served from the cache (hit).
#[test]
fn miss_loads_page_from_segment_file() {
    let tmp = tempfile::TempDir::new().unwrap();

    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    clog.set_state(TxnId(42), TxnState::Committed);
    clog.flush_dirty().unwrap();
    drop(clog);

    // Fresh buffer over the same directory: all frames empty, so the first
    // read must miss and pread the page; the second must hit.
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    assert_eq!(clog.get_state(TxnId(42)), TxnState::Committed);
    assert_eq!(clog.misses(), 1);
    assert_eq!(clog.hits(), 0);
    assert_eq!(clog.get_state(TxnId(42)), TxnState::Committed);
    assert_eq!(clog.hits(), 1);
    assert!(clog.hit_rate() > 0.0);
}

// ---------------------------------------------------------------------------
// Clock-sweep eviction (§6.3).
// ---------------------------------------------------------------------------

/// With all frames full, a new page evicts the first frame whose reference
/// bit the sweep cleared — recently re-referenced pages survive.
#[test]
fn clock_sweep_evicts_unreferenced_frames_first() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    let xid_in = |page: u64| TxnId(page * XIDS_PER_CLOG_PAGE + 1);

    // Fill all 4 frames with pages 0..4 (reads → clean frames), then
    // re-reference page 0 and page 1.
    for page in 0..4 {
        assert_eq!(clog.get_state(xid_in(page)), TxnState::InProgress);
    }
    assert_eq!(clog.misses(), 4);
    clog.get_state(xid_in(0)); // re-reference page 0 (hit)
    clog.get_state(xid_in(1)); // re-reference page 1 (hit)

    // Loading page 4 starts the sweep at frame 0: it clears every reference
    // bit on the first revolution, then evicts frame 0 (page 0).
    clog.get_state(xid_in(4));
    assert_eq!(clog.misses(), 5);
    let (hits, misses) = (clog.hits(), clog.misses());

    // Only page 0 was evicted: pages 1 and 3 are still resident (hits),
    // page 0 must miss on its next access.
    clog.get_state(xid_in(1));
    clog.get_state(xid_in(3));
    assert_eq!(
        clog.hits(),
        hits + 2,
        "pages 1 and 3 must still be resident"
    );
    clog.get_state(xid_in(0));
    assert_eq!(clog.misses(), misses + 1, "page 0 was evicted by the sweep");
}

/// A dirty frame chosen as victim is written back to its segment file
/// (without fsync) before reuse — never silently dropped.
#[test]
fn dirty_frame_is_written_back_before_eviction() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();

    // Dirty all 4 frames: pages 0..4, one Committed XID each.
    for page in 0..4u64 {
        clog.set_state(TxnId(page * XIDS_PER_CLOG_PAGE + 1), TxnState::Committed);
    }
    // No flush: nothing on disk yet.
    assert_eq!(pread_byte(tmp.path(), 0, 0), 0x00);

    // Touching a 5th page forces an eviction. After a full revolution all
    // frames are dirty, so the sweep evicts frame 0 (page 0) and must
    // write it back first — even though flush_dirty never ran.
    clog.set_state(TxnId(4 * XIDS_PER_CLOG_PAGE + 1), TxnState::Committed);
    assert_eq!(
        pread_byte(tmp.path(), 0, 0),
        0x01,
        "evicted dirty page 0 must have been written back (xid 1 = low nibble Committed)"
    );
    // The surviving in-memory copy is gone; the state is served from disk.
    assert_eq!(clog.get_state(TxnId(1)), TxnState::Committed);
}

// ---------------------------------------------------------------------------
// Flush discipline (§6.4, v2.3-21): memory-only until flush_dirty.
// ---------------------------------------------------------------------------

/// `set_state` must NOT reach the segment file on its own; only
/// `flush_dirty` (the checkpoint hook) makes it durable.
#[test]
fn set_state_is_not_on_disk_until_flush_dirty() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();

    clog.set_state(TxnId(7), TxnState::Committed);
    assert_eq!(
        pread_byte(tmp.path(), 0, 3),
        0x00,
        "commit path must not write the CLOG page (v2.3-21)"
    );

    clog.flush_dirty().unwrap();
    assert_eq!(
        pread_byte(tmp.path(), 0, 3),
        0x01, // xid 7: byte 3, low nibble = Committed
        "flush_dirty must persist the dirty frame"
    );
}

// ---------------------------------------------------------------------------
// Checkpoint integration (ClogFlush hook in pg-storage).
// ---------------------------------------------------------------------------

/// End to end: commit through `TxnManager` with the disk CLOG installed as
/// the checkpoint flush hook. After commit the segment file holds nothing;
/// after `trigger_checkpoint` the state is durable (§6.4 single flush point).
#[test]
fn checkpoint_flushes_clog_between_begin_and_end() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let clog = Arc::new(ClogBuffer::open(tmp.path(), 8).unwrap());
    engine.checkpoint().set_clog_flush(clog.clone());
    let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
    let mgr = TxnManager::new(engine.txn_id_clock(), wal, clog.clone());

    let xid = mgr.begin_txn(); // xid 1 on a fresh database
    mgr.commit_txn(xid).unwrap();

    // The commit fsynced its WAL record, but the CLOG page is memory-only.
    assert_eq!(clog.get_state(xid), TxnState::Committed);
    assert_eq!(
        pread_byte(tmp.path(), 0, 0),
        0x00,
        "commit must not flush the CLOG (v2.3-21)"
    );

    engine.trigger_checkpoint().unwrap();
    assert_eq!(
        pread_byte(tmp.path(), 0, 0),
        0x01, // xid 1: byte 0, low nibble = Committed
        "checkpoint must flush dirty CLOG frames to disk"
    );
    engine.shutdown();
}

// ---------------------------------------------------------------------------
// Crash recovery.
// ---------------------------------------------------------------------------

/// State flushed by a checkpoint survives a reopen with no WAL involvement:
/// a fresh `ClogBuffer` over the same directory reads the states back from
/// the segment files alone.
#[test]
fn checkpoint_persisted_state_survives_reopen_without_wal() {
    let tmp = tempfile::TempDir::new().unwrap();

    {
        let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
        clog.set_state(TxnId(11), TxnState::Committed);
        clog.set_state(TxnId(12), TxnState::Aborted);
        clog.flush_dirty().unwrap();
    }

    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();
    assert_eq!(clog.get_state(TxnId(11)), TxnState::Committed);
    assert_eq!(clog.get_state(TxnId(12)), TxnState::Aborted);
    assert_eq!(clog.get_state(TxnId(13)), TxnState::InProgress);
}

/// The txn redo handlers rebuild the disk CLOG from the WAL after a crash —
/// the same `ctx.clog.set_state` path as the M2a in-memory CLOG
/// (`recovery_clog_consistency.rs`), now backed by `ClogBuffer`. A
/// subsequent checkpoint persists the rebuilt state; after WAL recycling,
/// the segment files alone reproduce it.
#[test]
fn txn_redo_rebuilds_disk_clog_after_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let mut committed = Vec::new();
    let mut aborted = Vec::new();
    let mut in_progress = Vec::new();

    // --- Session 1: commit/abort through the disk CLOG, then "crash". ---
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let clog = Arc::new(ClogBuffer::open(tmp.path(), 8).unwrap());
        engine.checkpoint().set_clog_flush(clog.clone());
        let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
        let mgr = TxnManager::new(engine.txn_id_clock(), wal, clog.clone());

        for i in 0..10 {
            let xid = mgr.begin_txn();
            if i % 2 == 0 {
                mgr.commit_txn(xid).unwrap();
                committed.push(xid);
            } else {
                mgr.abort_txn(xid).unwrap();
                aborted.push(xid);
            }
        }
        in_progress.push(mgr.begin_txn());

        // "Crash": no shutdown, no checkpoint — the CLOG bits exist only in
        // memory and are lost; durability rests on the WAL alone.
        std::mem::forget(engine);
    }

    // --- Session 2: replay rebuilds the CLOG through the redo handlers. ---
    {
        let clog = Arc::new(ClogBuffer::open(tmp.path(), 8).unwrap());
        let engine = StorageEngine::open_with_redo_and_clog(
            tmp.path(),
            &config,
            txn_redo_handlers(),
            Vec::new(),
            clog.clone(),
        )
        .unwrap();
        engine.checkpoint().set_clog_flush(clog.clone());

        for xid in &committed {
            assert_eq!(clog.get_state(*xid), TxnState::Committed, "xid {xid:?}");
        }
        for xid in &aborted {
            assert_eq!(clog.get_state(*xid), TxnState::Aborted, "xid {xid:?}");
        }
        for xid in &in_progress {
            assert_eq!(clog.get_state(*xid), TxnState::InProgress, "xid {xid:?}");
        }

        // Persist the rebuilt CLOG; the checkpoint recycles the pre-checkpoint
        // WAL, so session 3 can only succeed via the segment files.
        engine.trigger_checkpoint().unwrap();
        engine.shutdown();
    }

    // --- Session 3: states come back from the segment files. ---
    {
        let clog = Arc::new(ClogBuffer::open(tmp.path(), 8).unwrap());
        let engine = StorageEngine::open_with_redo_and_clog(
            tmp.path(),
            &config,
            txn_redo_handlers(),
            Vec::new(),
            clog.clone(),
        )
        .unwrap();

        for xid in &committed {
            assert_eq!(clog.get_state(*xid), TxnState::Committed, "xid {xid:?}");
        }
        for xid in &aborted {
            assert_eq!(clog.get_state(*xid), TxnState::Aborted, "xid {xid:?}");
        }
        for xid in &in_progress {
            assert_eq!(clog.get_state(*xid), TxnState::InProgress, "xid {xid:?}");
        }
        engine.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Concurrency smoke test.
// ---------------------------------------------------------------------------

/// `get_state`/`set_state` from many threads over disjoint XID ranges stay
/// correct (single RwLock granularity is allowed for M2b; sharding is
/// Phase 7b).
#[test]
fn concurrent_get_set_are_thread_safe() {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = Arc::new(ClogBuffer::open(tmp.path(), 16).unwrap());

    let handles: Vec<_> = (0..8u64)
        .map(|t| {
            let clog = Arc::clone(&clog);
            std::thread::spawn(move || {
                // Each thread owns a contiguous 2-page XID range.
                let base = (t * 2 + 1) * XIDS_PER_CLOG_PAGE;
                for i in 0..1000u64 {
                    let xid = TxnId(base + i);
                    let state = if i % 2 == 0 {
                        TxnState::Committed
                    } else {
                        TxnState::Aborted
                    };
                    clog.set_state(xid, state);
                    assert_eq!(clog.get_state(xid), state);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    clog.flush_dirty().unwrap();
    for i in 0..10u64 {
        let xid = TxnId(XIDS_PER_CLOG_PAGE + i);
        let expected = if i % 2 == 0 {
            TxnState::Committed
        } else {
            TxnState::Aborted
        };
        assert_eq!(clog.get_state(xid), expected);
    }
}

/// Regression for the Stage L review P1 (path B): a dirty frame evicted by
/// the clock sweep is written back WITHOUT fsync, so by flush time the cache
/// can be entirely clean — a flush that only fsynced "segments of currently
/// dirty frames" would then issue NO fsync at all, and a completed
/// checkpoint would recycle the WAL while those commits exist only in the
/// page cache. `flush_dirty` must go through the store's unsynced-segment
/// tracking and fsync regardless of cache cleanliness.
#[test]
fn flush_dirty_fsyncs_segments_from_evicted_writebacks() {
    use pg_storage::types::TxnId;
    use pg_txn::ClogBuffer;
    use pg_txn::{ClogAccessor, TxnState};

    let tmp = tempfile::TempDir::new().unwrap();
    // Minimum frames: eviction is easy to force.
    let clog = ClogBuffer::open(tmp.path(), 4).unwrap();

    // Dirty 4 pages' worth of XIDs (fills every frame, all dirty).
    for page in 0..4u64 {
        clog.set_state(TxnId(page * 16384 + 1), TxnState::Committed);
    }
    // Touch 4 more pages: the clock sweep evicts the dirty frames, writing
    // them back WITHOUT fsync. The cache now holds only clean frames.
    for page in 4..8u64 {
        let _ = clog.get_state(TxnId(page * 16384 + 1));
    }

    // The writebacks left written-but-unsynced pages behind.
    assert!(
        clog.unsynced_segment_count() > 0,
        "eviction writebacks must be tracked as unsynced"
    );

    // The flush must fsync them even though no frame is dirty anymore.
    clog.flush_dirty().unwrap();
    assert_eq!(
        clog.unsynced_segment_count(),
        0,
        "flush_dirty must fsync segments written by eviction writebacks, \
         not just segments of currently dirty frames"
    );
}
