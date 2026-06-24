//! Stage J v2.3-2: ABORTED CLOG entries are never garbage-collected. A
//! `gc_committed_below` bound may remove COMMITTED entries below it, but any
//! ABORTED entry must survive so recovery/visibility can always tell an
//! aborted XID apart from one that never ran.

use std::sync::Arc;
use std::thread;

use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::TxnId;
use pg_txn::{ClogAccessor, CommitWal, InMemoryClogAccessor, TxnManager, TxnState};

/// A no-op WAL: append/flush always succeed. Used to drive the manager without
/// touching disk.
#[derive(Debug, Default)]
struct OkWal;

impl CommitWal for OkWal {
    fn append(
        &self,
        _record: pg_storage::wal::record::WalRecord,
    ) -> pg_storage::error::Result<pg_storage::types::Lsn> {
        Ok(pg_storage::types::Lsn::FIRST)
    }
    fn flush_to(&self, _lsn: pg_storage::types::Lsn) -> pg_storage::error::Result<()> {
        Ok(())
    }
}

#[test]
fn aborted_survives_gc_below_checkpoint_bound() {
    // GC explicitly enabled: this test exercises the eventual M2b truncation
    // semantics. The default (`m2a_clog_never_gc`) keeps GC a no-op.
    let clog = InMemoryClogAccessor::with_gc_enabled();
    // xid 1: committed, xid 2: aborted, xid 3: committed.
    clog.set_state(TxnId(1), TxnState::Committed);
    clog.set_state(TxnId(2), TxnState::Aborted);
    clog.set_state(TxnId(3), TxnState::Committed);

    // Simulate a checkpoint whose next_txn_id is 4: everything < 4 is eligible.
    let removed = clog.gc_committed_below(TxnId(4));
    assert_eq!(removed, 2); // xid 1 and 3 committed
    assert_eq!(clog.get_state(TxnId(1)), TxnState::InProgress); // gc'd
    assert_eq!(clog.get_state(TxnId(3)), TxnState::InProgress); // gc'd
                                                                // The aborted XID is still explicitly recorded.
    assert_eq!(clog.get_state(TxnId(2)), TxnState::Aborted);
}

#[test]
fn concurrent_commit_and_abort_100_txns_no_race() {
    let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
    let wal: Arc<dyn CommitWal> = Arc::new(OkWal);
    let mgr = Arc::new(TxnManager::new(
        TxnIdClock::new(TxnId::FIRST),
        wal,
        Arc::clone(&clog),
    ));

    let handles: Vec<_> = (0..100)
        .map(|i| {
            let mgr = Arc::clone(&mgr);
            thread::spawn(move || {
                let xid = mgr.begin_txn();
                if i % 2 == 0 {
                    mgr.commit_txn(xid).expect("commit");
                } else {
                    mgr.abort_txn(xid).expect("abort");
                }
                xid
            })
        })
        .collect();

    let xids: Vec<TxnId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Every XID is unique (monotone allocation, no duplicates under contention).
    let mut sorted = xids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 100, "XIDs must be unique");

    // Every transaction reached a terminal state; none remain active.
    assert!(mgr.active_xids().is_empty());
    for xid in xids {
        let state = clog.get_state(xid);
        assert!(
            state == TxnState::Committed || state == TxnState::Aborted,
            "xid {xid:?} left in non-terminal state {state:?}"
        );
    }
}
