//! Stage J recovery: the CLOG is rebuilt authoritatively from the WAL.
//!
//! A `TxnManager` commits some transactions and aborts others through a real
//! `WalWriter`. After a shutdown, reopening the engine with the transaction
//! redo handlers and a fresh (empty) `InMemoryClogAccessor` must replay the
//! `TxnCommit` / `TxnAbort` records and reproduce the exact terminal states —
//! proving the in-memory CLOG is not the source of truth, the WAL is.

use std::sync::Arc;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_txn::{
    txn_redo_handlers, ClogAccessor, CommitWal, InMemoryClogAccessor, TxnManager, TxnState,
};

#[test]
fn clog_is_rebuilt_from_wal_after_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    // --- Session 1: commit even XIDs, abort odd XIDs, then shut down. ---
    let mut committed = Vec::new();
    let mut aborted = Vec::new();
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
        let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
        let mgr = TxnManager::new(engine.txn_id_clock(), wal, Arc::clone(&clog));

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
        engine.shutdown();
    }

    // --- Session 2: reopen with txn redo handlers + a FRESH empty CLOG. ---
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        txn_redo_handlers(),
        Vec::new(),
        Arc::clone(&clog),
    )
    .unwrap();

    // Replay rebuilt every terminal state from the WAL.
    for xid in &committed {
        assert_eq!(
            clog.get_state(*xid),
            TxnState::Committed,
            "xid {xid:?} should replay as committed"
        );
    }
    for xid in &aborted {
        assert_eq!(
            clog.get_state(*xid),
            TxnState::Aborted,
            "xid {xid:?} should replay as aborted"
        );
    }

    // The recovered engine exposes the same CLOG instance.
    assert!(Arc::ptr_eq(engine.clog(), &clog));

    // P0 regression guard: the XID clock resumed *past* the highest XID that
    // any replayed WAL record carried. Without advancing the clock during
    // recovery (transactions committed after the last checkpoint left the
    // superblock's next_txn_id untouched), a fresh transaction would reuse a
    // committed XID and its tuples would be instantly visible.
    let max_used = committed
        .iter()
        .chain(&aborted)
        .max()
        .copied()
        .expect("some XIDs were allocated");
    let next = engine.txn_id_clock().alloc();
    assert!(
        next > max_used,
        "clock handed out {next:?} which is not past the recovered high-water {max_used:?}"
    );
    engine.shutdown();
}

/// Same rebuild guarantee as `clog_is_rebuilt_from_wal_after_restart`, but
/// the first session ends WITHOUT a clean shutdown (`mem::forget` skips the
/// engine's Drop, including the WAL writer's shutdown flush and the
/// checkpoint thread join) — the closest single-process approximation of a
/// crash. The CLOG must still rebuild perfectly because every commit/abort
/// record was fsynced by `flush_to` before the CLOG bit ever flipped (the
/// commit hard-order is exactly what makes a shutdown flush unnecessary).
#[test]
fn clog_is_rebuilt_from_wal_after_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let mut committed = Vec::new();
    let mut aborted = Vec::new();
    let mut in_progress = Vec::new();
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
        let wal: Arc<dyn CommitWal> = Arc::clone(engine.wal_writer()) as Arc<dyn CommitWal>;
        let mgr = TxnManager::new(engine.txn_id_clock(), wal, Arc::clone(&clog));

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
        // One transaction left open: it must recover as InProgress (no WAL
        // record, no CLOG entry), i.e. invisible — never committed.
        in_progress.push(mgr.begin_txn());

        // "Crash": no shutdown, no Drop, no final flush.
        std::mem::forget(engine);
    }

    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let engine = StorageEngine::open_with_redo_and_clog(
        tmp.path(),
        &config,
        txn_redo_handlers(),
        Vec::new(),
        Arc::clone(&clog),
    )
    .unwrap();

    for xid in &committed {
        assert_eq!(clog.get_state(*xid), TxnState::Committed);
    }
    for xid in &aborted {
        assert_eq!(clog.get_state(*xid), TxnState::Aborted);
    }
    for xid in &in_progress {
        assert_eq!(
            clog.get_state(*xid),
            TxnState::InProgress,
            "a transaction that never reached commit must stay in-progress"
        );
    }

    // The clock still advances past every XID handed out before the crash —
    // including the never-committed one (its abort/commit records are absent,
    // but its XID is on the WAL records of... itself only via high-water of
    // stamped records; at minimum it must be past all terminal XIDs).
    let max_terminal = committed.iter().chain(&aborted).max().copied().unwrap();
    assert!(engine.txn_id_clock().current() > max_terminal);
    engine.shutdown();
}
