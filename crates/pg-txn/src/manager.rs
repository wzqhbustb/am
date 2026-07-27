//! Minimal transaction manager (M2a Stage J).
//!
//! [`TxnManager`] gives M2a a single real transaction per SQL statement: each
//! `begin_txn` allocates one XID from the shared [`TxnIdClock`], and
//! `commit_txn` / `abort_txn` make the outcome durable in the WAL and then
//! record it in the commit log. M2a runs in auto-commit, so callers pair one
//! `begin_txn` with exactly one `commit_txn`/`abort_txn`.
//!
//! # Commit hard-order (§3 P1-5)
//!
//! Commit performs four steps in a fixed order so recovery can rebuild the
//! CLOG authoritatively from the WAL:
//!
//! 1. `wal.append(TxnCommit)` — stage the record.
//! 2. `wal.flush_to(lsn)` — fsync it (the commit is durable here).
//! 3. `clog.set_state(xid, Committed)` — flip the in-memory bit.
//! 4. `remove_active(xid)` — drop the XID from the active set.
//!
//! If step 2 fails the CLOG bit is never flipped (step 3 is unreachable), so a
//! transaction whose commit record did not reach disk is treated as aborted on
//! recovery — never as committed. Abort follows the same shape with
//! `TxnAbort` and `TxnState::Aborted`.
//!
//! # Group-commit batching (coding-plan Stage J `page_alloc flush 攒批`)
//!
//! `PageAllocator::alloc_page` / `free_page` are append-only: they write their
//! WAL record to the segment file but do **not** fsync. The commit's single
//! `flush_to(lsn)` at step 2 therefore amortizes every allocation fsync
//! accumulated during the transaction into one syscall — a `CREATE TABLE` that
//! extends many pages pays for one fsync at commit instead of one per page.
//! This is safe because `flush_to(commit_lsn)` fsyncs the whole WAL prefix up
//! to the commit record, and the LSN clock is monotonic, so every earlier
//! `PageAlloc`/`PageFree` LSN is covered.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::error::Result;
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

/// The two WAL operations the commit path needs: stage a record and fsync it.
///
/// [`WalWriter`] is the production implementation. The trait exists so the
/// commit hard-order (§3 P1-5) can be tested by injecting a WAL whose
/// `flush_to` fails — proving the CLOG bit is never flipped when the commit
/// record did not reach disk. It is intentionally tiny (append + flush).
pub trait CommitWal: std::fmt::Debug + Send + Sync {
    /// Append `record`, returning the LSN it was assigned.
    fn append(&self, record: WalRecord) -> Result<Lsn>;
    /// Flush (fsync) the WAL up to and including `lsn`.
    fn flush_to(&self, lsn: Lsn) -> Result<()>;
}

impl CommitWal for WalWriter {
    fn append(&self, record: WalRecord) -> Result<Lsn> {
        WalWriter::append(self, record)
    }

    fn flush_to(&self, lsn: Lsn) -> Result<()> {
        WalWriter::flush_to(self, lsn)
    }
}

/// Coordinates XID allocation and durable commit/abort for M2a.
///
/// Cheap to clone conceptually via `Arc`; hold a single instance per engine
/// and share it. All fields are `Arc`/interior-mutable so `&self` methods
/// are safe to call concurrently.
#[derive(Debug)]
pub struct TxnManager {
    txn_id_clock: TxnIdClock,
    wal: Arc<dyn CommitWal>,
    clog: Arc<dyn ClogAccessor>,
    /// XIDs that have begun but not yet committed or aborted.
    active: Mutex<HashSet<TxnId>>,
}

impl TxnManager {
    /// Create a transaction manager over the engine's shared components.
    pub fn new(
        txn_id_clock: TxnIdClock,
        wal: Arc<dyn CommitWal>,
        clog: Arc<dyn ClogAccessor>,
    ) -> Self {
        Self {
            txn_id_clock,
            wal,
            clog,
            active: Mutex::new(HashSet::new()),
        }
    }

    /// Begin a transaction: allocate a fresh XID and mark it active.
    ///
    /// The XID's CLOG entry is left implicit (`InProgress`) until commit/abort
    /// records the terminal state.
    pub fn begin_txn(&self) -> TxnId {
        let xid = self.txn_id_clock.alloc();
        self.active.lock().insert(xid);
        xid
    }

    /// Commit `xid` following the four-step hard order (§3 P1-5).
    ///
    /// Returns an error (leaving the CLOG bit unflipped) if the WAL append or
    /// fsync fails, so a non-durable commit is never observable as committed.
    ///
    /// # Failure semantics of the active set
    ///
    /// If step 1 or step 2 fails, `xid` is left in the active set on purpose.
    /// The commit is not durable, so the transaction is still logically
    /// in-progress from every reader's point of view; keeping it active
    /// reflects that. M2a runs auto-commit (one caller owns the XID and will
    /// not retry after an `Err`), so the stale entry is harmless — the process
    /// tears down on a WAL error anyway (the writer marks itself shut down on
    /// fsync failure). A future multi-statement layer that retries commits must
    /// treat the active entry as authoritative and re-drive the same four steps.
    ///
    /// # Step 3/4 ordering
    ///
    /// The CLOG bit is the source of truth for visibility and is flipped
    /// (step 3) *before* the XID leaves the active set (step 4). A concurrent
    /// reader that observes the XID still active will consult the CLOG and may
    /// already see `Committed`; that is correct — the commit is durable by
    /// step 2, so treating it as committed the instant the bit flips is sound.
    /// The reverse order (remove-then-set) would open a window where the XID is
    /// neither active nor yet Committed, i.e. momentarily invisible as either.
    pub fn commit_txn(&self, xid: TxnId) -> Result<()> {
        // 1. Append the commit record.
        let lsn = self.wal.append(WalRecord::txn_commit(xid)?)?;
        // 2. fsync it — the commit becomes durable here.
        self.wal.flush_to(lsn)?;
        // 3. Flip the in-memory CLOG bit (only after the record is durable).
        self.clog.set_state(xid, TxnState::Committed);
        // 4. Drop the XID from the active set (after the CLOG bit; see doc).
        self.active.lock().remove(&xid);
        Ok(())
    }

    /// Abort `xid`, recording a durable `TxnAbort` before the CLOG bit.
    ///
    /// ABORTED entries are never garbage-collected (v2.3-2), so recovery can
    /// always distinguish an aborted XID from one that never ran.
    ///
    /// Failure and ordering semantics mirror [`Self::commit_txn`]: on a WAL
    /// error `xid` stays active (the abort is not durable), and the CLOG bit is
    /// set before the active-set removal.
    pub fn abort_txn(&self, xid: TxnId) -> Result<()> {
        let lsn = self.wal.append(WalRecord::txn_abort(xid)?)?;
        self.wal.flush_to(lsn)?;
        self.clog.set_state(xid, TxnState::Aborted);
        self.active.lock().remove(&xid);
        Ok(())
    }

    /// Snapshot of the currently active XIDs (test/observability helper).
    pub fn active_xids(&self) -> Vec<TxnId> {
        let mut v: Vec<TxnId> = self.active.lock().iter().copied().collect();
        v.sort_unstable();
        v
    }
}
