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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use smallvec::SmallVec;
use thiserror::Error;

use pg_storage::clog::{ClogAccessor, TxnState};
// The barrier crosses the crate boundary into pg-storage's checkpoint
// coordinator, so it must be the aliased type: identical to
// `parking_lot::RwLock` in production builds, loom-instrumented under
// `--cfg loom` (Stage Q; see pg_storage::sync).
use pg_storage::sync::RwLock;
use pg_storage::error::Result;
use pg_storage::txn_id::TxnIdClock;
use pg_storage::types::{Lsn, TxnId};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::writer::WalWriter;

use crate::snapshot::Snapshot;

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

/// Errors from transaction wait operations (M2c Stage P).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TxnError {
    /// A transaction asked to wait on itself. That is always a caller bug:
    /// the row-lock protocol (§9.1) only ever waits on a *different* XID
    /// read from a tuple's `t_xmax`.
    #[error("transaction {0} cannot wait on itself")]
    SelfWait(TxnId),
}

/// The row-lock wait capability the heap AM's §9.1 5-step protocol needs
/// (M2c Stage P).
///
/// Implemented by [`TxnManager`]; declared as a separate trait so
/// `pg-am-heap` depends on the narrow register/wait surface instead of the
/// concrete manager type, and so AM tests can inject a fake. The protocol's
/// ordering requirements are on the methods.
pub trait RowWaiter: std::fmt::Debug + Send + Sync {
    /// Register the wait edge `self_xid → blocking_xid` (§9.1 step 5a).
    ///
    /// The caller MUST invoke this while still holding the page latch under
    /// which it read `blocking_xid` from the tuple's `t_xmax`, and MUST NOT
    /// release that latch before the call returns — registration before
    /// latch release is what makes the subsequent [`RowWaiter::wait_for`]
    /// wakeup unmissable.
    fn register_row_wait(&self, self_xid: TxnId, blocking_xid: TxnId);

    /// Drop `self_xid`'s wait edge without waiting (caller-side cleanup on
    /// paths that leave the protocol without a completed
    /// [`RowWaiter::wait_for`], which otherwise clears the edge itself).
    fn unregister_row_wait(&self, self_xid: TxnId);

    /// Block until `blocking_xid` commits or aborts (§9.1 step 5c). The
    /// caller must hold NO page latch while blocked. Clears the wait edge
    /// on success.
    fn wait_for(&self, self_xid: TxnId, blocking_xid: TxnId)
        -> std::result::Result<(), TxnError>;

    /// Is `xid` a live, in-flight transaction?
    ///
    /// The gate needs this to distinguish a genuinely active stamper (wait
    /// on it) from one whose CLOG entry reads `InProgress` but whose XID is
    /// gone from the active set. That combination has TWO causes, and the
    /// caller must tell them apart by RE-READING the CLOG after a `false`
    /// return:
    ///
    /// - the stamper ended between the caller's CLOG read and this check
    ///   (normal race): `end_txn` flips the CLOG bit BEFORE removing the
    ///   XID, so observing not-active orders the caller after the terminal
    ///   write and the CLOG re-read yields the terminal state;
    /// - the stamper CRASHED (post-recovery; recovery-end ATT abort
    ///   marking is still open, §11.3): the re-read still says
    ///   `InProgress`, and — WAL replay having rebuilt every durable
    ///   commit's bit — the stamp must be treated as aborted, never waited
    ///   on (waiting would return instantly and the gate would spin).
    fn is_active(&self, xid: TxnId) -> bool;
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
    /// Commit/checkpoint barrier (M2c Stage P: sunk down from pg-engine,
    /// where it was introduced in Stage L). `commit_txn` / `abort_txn` hold
    /// a READ guard for their whole hard order; the checkpoint coordinator
    /// in pg-storage takes the WRITE guard (via `set_commit_barrier`).
    /// This closes the "neither snapshot nor replay" window by construction:
    /// a commit durable before the checkpoint's `begin_lsn` has finished its
    /// `clog.set_state` before the checkpoint's CLOG flush runs (its bit is
    /// fsynced), and a commit starting after lands past `begin_lsn` (replay
    /// rebuilds it).
    ///
    /// Note on scope: the coordinator currently holds the WRITE guard
    /// across the WHOLE checkpoint (all dirty-page flushes), not merely the
    /// ATT-sampling + CLOG-flush critical section — correct but
    /// conservative, since commits stall for the full flush duration on
    /// this hardware. Narrowing the guard to the critical section is a
    /// planned Phase 7b optimization; the implementation is intentionally
    /// left as-is for now.
    commit_barrier: Arc<RwLock<()>>,
    /// Row-lock wait registry (§9.1 step 5a): waiter XID → the XID it is
    /// blocked on. Paired with `row_wait_cv`; waiters clear their own entry
    /// when their wait completes ([`Self::wait_for`]).
    row_wait_registry: Mutex<HashMap<TxnId, TxnId>>,
    /// Broadcast on every commit/abort (see [`Self::end_txn`]) so row-lock
    /// waiters re-check whether their blocking XID left the active set.
    row_wait_cv: Condvar,
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
            commit_barrier: Arc::new(RwLock::new(())),
            row_wait_registry: Mutex::new(HashMap::new()),
            row_wait_cv: Condvar::new(),
        }
    }

    /// The commit/checkpoint barrier (M2c Stage P). The engine installs
    /// this into the pg-storage checkpoint coordinator
    /// (`CheckpointCoordinator::set_commit_barrier`) at open time so every
    /// checkpoint's ATT sampling + CLOG flush runs under the write guard
    /// while commits/aborts run under read guards.
    pub fn commit_barrier(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.commit_barrier)
    }

    /// Begin a transaction: allocate a fresh XID and mark it active.
    ///
    /// The XID's CLOG entry is left implicit (`InProgress`) until commit/abort
    /// records the terminal state.
    ///
    /// The clock alloc and the active-set insert happen under the SAME lock
    /// (PostgreSQL: "store the new XID into the shared ProcArray before
    /// releasing XidGenLock"). Splitting them — a wait-free `alloc` followed
    /// by a locked `insert` — opens a window where a concurrent
    /// [`Self::snapshot`] can read `xmax = X+1` while `X` is not yet in the
    /// active set: `X < xmax`, `X ∉ xip`, and once `X` commits the snapshot
    /// sees its writes — a snapshot-isolation violation, because `X`
    /// linearized *after* the snapshot was taken. Holding the lock across
    /// both steps makes the pair atomic: any `xid < snapshot.xmax` is either
    /// in `xip` or already terminal.
    pub fn begin_txn(&self) -> TxnId {
        let mut active = self.active.lock();
        let xid = self.txn_id_clock.alloc();
        active.insert(xid);
        xid
    }

    /// Commit `xid` following the four-step hard order (§3 P1-5).
    ///
    /// Returns an error (leaving the CLOG bit unflipped) if the WAL append or
    /// fsync fails, so a non-durable commit is never observable as committed.
    ///
    /// # Table locks are NOT released here
    ///
    /// The manager knows nothing about the table `LockManager` (it lives in
    /// pg-engine, keyed by XID). Callers that acquired table locks through
    /// the engine MUST pair this with `LockManager::release_all(xid)` — the
    /// engine's `TxnHandle::commit` / `auto_commit` do so; a raw
    /// `commit_txn` through `Engine::txn_manager()` leaves the transaction's
    /// table locks behind, which later DDL (`AccessExclusive`) wedges on.
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
        // Commit-barrier read guard for the WHOLE hard order (M2c Stage P):
        // a checkpoint holds the write guard across its ATT sampling + CLOG
        // flush, so this commit's `set_state` can never land in the window
        // where it would be present in neither the fsynced CLOG nor the
        // replay. The guard is taken here, inside the manager, so every
        // caller — engine auto-commit, explicit TxnHandle, or direct
        // `txn_manager()` access — is covered by construction.
        let _barrier = self.commit_barrier.read();
        // 1. Append the commit record.
        let lsn = self.wal.append(WalRecord::txn_commit(xid)?)?;
        // 2. fsync it — the commit becomes durable here.
        self.wal.flush_to(lsn)?;
        // 3. Flip the in-memory CLOG bit (only after the record is durable).
        // 4. Drop the XID from the active set and wake row-lock waiters.
        self.end_txn(xid, TxnState::Committed);
        Ok(())
    }

    /// Abort `xid`, recording a durable `TxnAbort` before the CLOG bit.
    ///
    /// ABORTED entries are never garbage-collected (v2.3-2), so recovery can
    /// always distinguish an aborted XID from one that never ran.
    ///
    /// Failure and ordering semantics mirror [`Self::commit_txn`]: on a WAL
    /// error `xid` stays active (the abort is not durable), and the CLOG bit is
    /// set before the active-set removal. The same table-lock caveat applies:
    /// raw callers must pair this with `LockManager::release_all(xid)` (see
    /// [`Self::commit_txn`]).
    pub fn abort_txn(&self, xid: TxnId) -> Result<()> {
        // Same commit-barrier read guard as commit_txn (see there).
        let _barrier = self.commit_barrier.read();
        let lsn = self.wal.append(WalRecord::txn_abort(xid)?)?;
        self.wal.flush_to(lsn)?;
        self.end_txn(xid, TxnState::Aborted);
        Ok(())
    }

    /// The shared tail of commit/abort: flip the CLOG bit, drop the XID
    /// from the active set, then broadcast to row-lock waiters (§9.1 step
    /// 5d, M2c Stage P).
    ///
    /// # Broadcast ordering requirement
    ///
    /// The `notify_all` runs strictly AFTER `clog.set_state` (and after the
    /// active-set removal): a woken waiter that re-reads the CLOG must see
    /// the terminal state, never a stale `InProgress` that would send it
    /// back to sleep on a condvar nobody will signal again. The broadcast
    /// itself is delivered under the registry mutex — `wait_for` checks its
    /// predicate and sleeps atomically with respect to that mutex, so the
    /// removal-then-notify sequence cannot be missed (a waiter either sees
    /// the XID gone before sleeping, or is woken after it).
    ///
    /// Registry *entries* are not touched here: each waiter clears its own
    /// edge on wake (`wait_for` removes it), which keeps the wait-for graph
    /// free of stale edges without end_txn having to scan for them.
    ///
    /// # Lock order
    ///
    /// The active-set and registry mutexes are taken SEQUENTIALLY here,
    /// never nested; the only nested direction anywhere in this manager is
    /// registry → active (inside [`Self::wait_for`]), so no inversion is
    /// possible.
    fn end_txn(&self, xid: TxnId, state: TxnState) {
        self.clog.set_state(xid, state);
        // Active-set removal AFTER the CLOG bit; see the commit_txn doc on
        // the step 3/4 ordering argument.
        self.active.lock().remove(&xid);
        // Wake row-lock waiters (§9.1 5d) AFTER the terminal state is
        // visible; see the ordering doc above.
        let _registry = self.row_wait_registry.lock();
        self.row_wait_cv.notify_all();
    }

    /// Register a row-lock wait edge (§9.1 step 5a): `self_xid` is about to
    /// block on `blocking_xid`. Idempotent — re-registering the same waiter
    /// overwrites its edge, matching the protocol's restart-from-step-1
    /// loop where a waiter may re-block on a *different* XID.
    pub fn register_row_wait(&self, self_xid: TxnId, blocking_xid: TxnId) {
        self.row_wait_registry.lock().insert(self_xid, blocking_xid);
    }

    /// Drop `self_xid`'s wait edge, if any. Called on paths that leave the
    /// wait protocol without going through [`Self::wait_for`] (e.g. the
    /// caller aborts); `wait_for` itself clears the edge on completion.
    pub fn unregister_row_wait(&self, self_xid: TxnId) {
        self.row_wait_registry.lock().remove(&self_xid);
    }

    /// Snapshot of all row-lock wait edges `(waiter, waiting_on)` — the
    /// row-lock half of Stage R's wait-for graph (the table-lock half comes
    /// from `LockManager::table_lock_state`).
    pub fn wait_edges(&self) -> Vec<(TxnId, TxnId)> {
        let mut edges: Vec<(TxnId, TxnId)> = self
            .row_wait_registry
            .lock()
            .iter()
            .map(|(&w, &b)| (w, b))
            .collect();
        edges.sort_unstable();
        edges
    }

    /// Block until `blocking_xid` leaves the active set (§9.1 step 5c).
    ///
    /// Returns immediately when `blocking_xid` is already terminated
    /// (committed/aborted XIDs are removed from the active set by
    /// [`Self::end_txn`]). Spurious wakeups are handled by looping on the
    /// predicate; the only wakeup source is `end_txn`'s broadcast.
    ///
    /// While blocked this holds NO `TxnManager` lock except the registry
    /// mutex the condvar releases — the active set, CLOG, and WAL stay
    /// available to the blocking transaction's own commit/abort, which is
    /// exactly the progress the waiter is sleeping on.
    ///
    /// # Lock order
    ///
    /// The only legal nesting is registry → active (this function holds the
    /// registry mutex and takes the active mutex inside it). `end_txn`
    /// takes the two sequentially, never nested, so it cannot invert the
    /// order.
    ///
    /// # Non-returning waits
    ///
    /// There is no timeout and no interruption: if the blocking
    /// transaction's commit/abort never completes — e.g. its WAL fsync
    /// failed and the process is tearing down (the WAL writer marks itself
    /// shut down on fsync failure) — this wait never returns. That is a
    /// process-level failure policy, not a liveness bug: the waiter's own
    /// transaction cannot make meaningful progress in a tearing-down
    /// process either.
    ///
    /// TODO(Stage R): the deadlock detector must be able to break a wait —
    /// a victim selected for abort is typically blocked inside this
    /// function. Add an interruption/timeout channel (e.g. a victim flag
    /// checked alongside the predicate, or `wait_timeout` with re-check)
    /// when the detector lands.
    ///
    /// On success the waiter's registry edge is cleared (§9.1: waiters
    /// clear their own entries on wake).
    ///
    /// # Errors
    ///
    /// [`TxnError::SelfWait`] if `self_xid == blocking_xid` — a transaction
    /// waiting on itself is a caller bug, not a schedulable state.
    pub fn wait_for(&self, self_xid: TxnId, blocking_xid: TxnId) -> std::result::Result<(), TxnError> {
        if self_xid == blocking_xid {
            return Err(TxnError::SelfWait(self_xid));
        }
        let mut registry = self.row_wait_registry.lock();
        loop {
            // Predicate: the blocking XID has left the active set. Checked
            // while holding the registry mutex; `end_txn` notifies under
            // the same mutex after removing the XID, so no wakeup is lost.
            if !self.active.lock().contains(&blocking_xid) {
                registry.remove(&self_xid);
                return Ok(());
            }
            self.row_wait_cv.wait(&mut registry);
        }
    }

    /// Snapshot of the currently active XIDs (test/observability helper).
    pub fn active_xids(&self) -> Vec<TxnId> {
        let mut v: Vec<TxnId> = self.active.lock().iter().copied().collect();
        v.sort_unstable();
        v
    }

    /// Is `xid` a live, in-flight transaction? See [`RowWaiter::is_active`]
    /// for how the row-lock gate uses this to tell a crashed stamper from
    /// an active one.
    pub fn is_active(&self, xid: TxnId) -> bool {
        self.active.lock().contains(&xid)
    }
    /// Take a real Snapshot-Isolation snapshot for `current_xid`
    /// (tech-selection §7.1).
    ///
    /// The snapshot reads `xmax` from the XID clock and `xip` from the active
    /// set; `xmin` is the smallest active XID (or `xmax` when the active set
    /// is empty), and `curcid` starts at 0 (the executor advances it per
    /// statement, §7.1 Q4). The caller's own XID may appear in `xip` when it
    /// is still active; the oracle's `xmin == self_xid` branch is checked
    /// before `xip`, so this is harmless and matches PG, which also records
    /// the snapshot taker among the running XIDs.
    ///
    /// # Atomicity argument
    ///
    /// The active-set mutex is the single serialization point for membership
    /// changes: `begin_txn` inserts, `commit_txn`/`abort_txn` remove, all
    /// under this lock. `snapshot` holds the lock while reading **both** the
    /// clock and the set, which defines the logical instant — `xip` and
    /// `xmax` are mutually consistent by construction:
    ///
    /// - Every XID in the set was allocated before its insert, and the insert
    ///   happened-before our lock acquisition, so every `xip` entry is
    ///   strictly below the `xmax` we read inside the same critical section
    ///   (the invariant `xmin <= xip[i] < xmax` holds).
    /// - A concurrent `begin_txn` may allocate from the clock during our read
    ///   (allocation is wait-free and takes no lock), but its XID then lands
    ///   at or above our `xmax` and is judged "future" — invisible — which is
    ///   correct: that begin is not yet observable to anyone.
    /// - A concurrent commit between its CLOG-bit flip (step 3) and its
    ///   active-set removal (step 4) leaves the XID in our `xip` with
    ///   CLOG = Committed; the oracle consults `xip` before the CLOG, so the
    ///   transaction stays invisible — correct, because at the snapshot's
    ///   logical instant the commit had not completed (removal is the
    ///   completion signal).
    pub fn snapshot(&self, current_xid: TxnId) -> Snapshot {
        let active = self.active.lock();
        let xmax = self.txn_id_clock.current();
        let mut xip: SmallVec<[TxnId; 32]> = active.iter().copied().collect();
        drop(active);
        xip.sort_unstable();
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot {
            xmin,
            xmax,
            xip,
            current_xid,
            curcid: 0,
        }
    }
}

/// M2b Stage N wiring (tech-selection §11.4): the checkpoint coordinator in
/// `pg-storage` captures the ATT snapshot through this trait, keeping the
/// dependency direction `pg-txn` → `pg-storage` (same pattern as
/// `ClogFlush`). The engine installs the manager at open time via
/// `CheckpointCoordinator::set_att_provider`.
impl pg_storage::recovery::AttProvider for TxnManager {
    fn active_xids(&self) -> Vec<TxnId> {
        // Delegates to the inherent method (sorted), so the ATT snapshot
        // file is deterministic.
        TxnManager::active_xids(self)
    }
}

/// The heap AM's row-lock protocol (§9.1 step 5) drives the manager through
/// this narrow surface; every method delegates to the inherent implementation
/// documented there.
impl RowWaiter for TxnManager {
    fn register_row_wait(&self, self_xid: TxnId, blocking_xid: TxnId) {
        TxnManager::register_row_wait(self, self_xid, blocking_xid);
    }

    fn unregister_row_wait(&self, self_xid: TxnId) {
        TxnManager::unregister_row_wait(self, self_xid);
    }

    fn wait_for(&self, self_xid: TxnId, blocking_xid: TxnId) -> std::result::Result<(), TxnError> {
        TxnManager::wait_for(self, self_xid, blocking_xid)
    }

    fn is_active(&self, xid: TxnId) -> bool {
        TxnManager::is_active(self, xid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryClogAccessor;

    /// A no-op WAL: append/flush always succeed, so the manager can be driven
    /// without touching disk.
    #[derive(Debug, Default)]
    struct OkWal;

    impl CommitWal for OkWal {
        fn append(&self, _record: WalRecord) -> Result<Lsn> {
            Ok(Lsn::FIRST)
        }

        fn flush_to(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }
    }

    fn manager() -> TxnManager {
        TxnManager::new(
            TxnIdClock::new(TxnId::FIRST),
            Arc::new(OkWal),
            Arc::new(InMemoryClogAccessor::new()),
        )
    }

    /// M2b Stage N (§11.4): the manager doubles as the checkpoint
    /// coordinator's ATT snapshot source — begun-but-not-committed XIDs show
    /// up, committed/aborted ones do not.
    #[test]
    fn att_provider_reports_in_flight_xids() {
        use pg_storage::recovery::AttProvider;

        let mgr = manager();
        let t1 = mgr.begin_txn();
        let t2 = mgr.begin_txn();
        let t3 = mgr.begin_txn();
        mgr.commit_txn(t2).unwrap();
        assert_eq!(AttProvider::active_xids(&mgr), vec![t1, t3]);
        mgr.abort_txn(t1).unwrap();
        assert_eq!(AttProvider::active_xids(&mgr), vec![t3]);
    }

    #[test]
    fn snapshot_with_empty_active_set() {
        let mgr = manager();
        let snap = mgr.snapshot(TxnId::INVALID);
        assert!(snap.xip.is_empty());
        // Empty active set: xmin collapses to xmax = next unallocated XID.
        assert_eq!(snap.xmax, TxnId::FIRST);
        assert_eq!(snap.xmin, snap.xmax);
        assert_eq!(snap.curcid, 0);
    }

    #[test]
    fn snapshot_captures_active_set_contents() {
        let mgr = manager();
        let t1 = mgr.begin_txn();
        let t2 = mgr.begin_txn();
        let t3 = mgr.begin_txn();

        let snap = mgr.snapshot(t2);
        assert_eq!(snap.xip.as_slice(), &[t1, t2, t3], "sorted full copy");
        assert_eq!(snap.xmin, t1, "xmin = smallest active XID");
        assert_eq!(snap.xmax, TxnId(4), "xmax = next unallocated XID");
        assert_eq!(snap.current_xid, t2);
        assert_eq!(snap.curcid, 0);
        for &xid in snap.xip.iter() {
            assert!(snap.xmin <= xid && xid < snap.xmax);
        }
    }

    #[test]
    fn snapshot_xmin_xmax_boundaries_track_commit() {
        let mgr = manager();
        let t1 = mgr.begin_txn();
        let t2 = mgr.begin_txn();
        mgr.commit_txn(t1).unwrap();

        let snap = mgr.snapshot(t2);
        assert_eq!(snap.xip.as_slice(), &[t2], "committed XID leaves xip");
        assert_eq!(snap.xmin, t2, "xmin advances past the committed XID");
        assert_eq!(snap.xmax, TxnId(3));

        mgr.commit_txn(t2).unwrap();
        let snap = mgr.snapshot(TxnId::INVALID);
        assert!(snap.xip.is_empty());
        assert_eq!(snap.xmin, snap.xmax);
    }

    #[test]
    fn snapshot_is_consistent_under_concurrent_begin_commit() {
        // Hammer the manager from multiple threads; every snapshot must
        // satisfy the structural invariants (sorted xip, xmin <= xip < xmax).
        let mgr = Arc::new(manager());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let mgr = Arc::clone(&mgr);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    let xid = mgr.begin_txn();
                    let snap = mgr.snapshot(xid);
                    for w in snap.xip.windows(2) {
                        assert!(w[0] < w[1], "xip sorted");
                    }
                    for &entry in snap.xip.iter() {
                        assert!(snap.xmin <= entry && entry < snap.xmax);
                    }
                    mgr.commit_txn(xid).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(mgr.active_xids().is_empty());
    }
}

#[cfg(test)]
mod begin_atomicity_tests {
    //! Regression for the Stage L review P1: `begin_txn` used to split the
    //! clock alloc (wait-free) from the active-set insert (locked). A
    //! concurrent `snapshot()` could then read `xmax = X+1` while `X` was
    //! not yet registered — `X < xmax`, `X ∉ xip`, and after X committed
    //! the snapshot saw its writes: an SI violation (PG avoids this by
    //! registering the XID in ProcArray before releasing XidGenLock).
    use super::*;
    use crate::InMemoryClogAccessor;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct NoWal;
    impl CommitWal for NoWal {
        fn append(&self, _record: WalRecord) -> Result<Lsn> {
            Ok(Lsn::FIRST)
        }
        fn flush_to(&self, _lsn: Lsn) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn snapshot_never_sees_alloc_without_insert() {
        let mgr = Arc::new(TxnManager::new(
            TxnIdClock::new(TxnId::FIRST),
            Arc::new(NoWal),
            Arc::new(InMemoryClogAccessor::new()),
        ));
        let mgr2 = Arc::clone(&mgr);

        // A "slow begin": hold the active lock, alloc, sleep, then insert —
        // exactly the old implementation's interleaving. With the fix,
        // `snapshot()` blocks on the same lock until the insert completes.
        let slow = thread::spawn(move || {
            let mut active = mgr2.active.lock();
            let xid = mgr2.txn_id_clock.alloc();
            thread::sleep(Duration::from_millis(50));
            active.insert(xid);
            drop(active);
            xid
        });

        // While the slow begin sleeps, take a snapshot.
        let snap = mgr.snapshot(TxnId::INVALID);
        let xid = slow.join().unwrap();

        // The snapshot must either predate the alloc (xid >= xmax) or have
        // the xid registered in xip. The middle state — xmax above xid while
        // xid is absent from xip — is the SI violation and must not occur.
        assert!(
            xid.0 >= snap.xmax.0 || snap.xip.contains(&xid),
            "snapshot saw alloc-without-insert: xid={xid:?}, xmax={:?}, xip={:?}",
            snap.xmax,
            snap.xip
        );
    }
}
