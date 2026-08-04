//! Table-level lock manager (M2c Stage P; tech-selection §9.2).
//!
//! Four standard lock modes with the §9.2 conflict matrix:
//!
//! | Mode              | Conflicts with                                  |
//! |-------------------|-------------------------------------------------|
//! | `AccessShare`     | `AccessExclusive`                               |
//! | `RowExclusive`    | `Exclusive`, `AccessExclusive`                  |
//! | `Exclusive`       | `RowExclusive`, `Exclusive`, `AccessExclusive`  |
//! | `AccessExclusive` | everything                                      |
//!
//! Intention locks (IS/IX) are deliberately absent (deferred to Phase 6 per
//! the ROADMAP), which keeps the matrix at 4×4 instead of PG's 8×8.
//!
//! # Fairness (FIFO, anti-starvation)
//!
//! A requester blocks when its mode conflicts with any *granted* mode OR any
//! waiter is already queued ahead of it — no barging. The second clause is
//! what keeps a queued `AccessExclusive` (a DDL) from starving under a stream
//! of mutually compatible `AccessShare` readers: once anyone queues, every
//! later arrival queues behind them. [`LockManager::release_all`] re-grants
//! the queue head(s) in order, walking the queue while consecutive head
//! waiters are compatible with the granted set.
//!
//! # 2PL: held to transaction end, no downgrade
//!
//! Locks are released only by [`LockManager::release_all`] at commit/abort
//! time. Re-acquiring a held lock with a stronger mode upgrades in place
//! (max strength); re-acquiring with the same or a weaker mode is a no-op.
//! An upgrade that must wait keeps the old grant while queued (same as PG):
//! the requester's old mode still counts as granted, so two transactions
//! upgrading the same table lock in opposite directions can deadlock. That
//! is accepted for Stage P — Stage R's deadlock detector will consume the
//! wait state exposed here ([`LockManager::table_lock_state`]) to break it.
//!
//! # Deadlock scope
//!
//! Deadlock *detection* is Stage R and not implemented here. What Stage P
//! guarantees is only that waiting never holds the global lock-manager mutex
//! while blocked: [`parking_lot::Condvar::wait`] releases the mutex for the
//! duration of the sleep, so a classic A-holds-t1-wants-t2 / B-holds-t2-
//! wants-t1 cycle wedges the two transactions, never the lock manager.

use std::collections::{HashMap, VecDeque};

use parking_lot::{Condvar, Mutex};
use thiserror::Error;

use pg_storage::types::{Oid, TxnId};

/// Errors from the table lock manager.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LockError {
    /// Reserved for Stage R's deadlock detector: a wait that would close a
    /// cycle in the wait-for graph is aborted with this error. Stage P never
    /// produces it (no detection yet), but the variant fixes the error type
    /// so the row-lock 5-step protocol (§9.1) and Stage R can land without
    /// signature churn.
    #[error("deadlock detected: transaction {0} chosen as victim")]
    DeadlockVictim(TxnId),
}

/// A convenient alias for lock-manager results.
pub type LockResult<T> = std::result::Result<T, LockError>;

/// The four table-level lock modes (tech-selection §9.2).
///
/// Ordered by strength: `AccessShare < RowExclusive < Exclusive <
/// AccessExclusive`. Strength drives upgrade semantics: re-acquiring with a
/// stronger mode upgrades in place, with a same-or-weaker mode is a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockMode {
    /// SELECT. Conflicts only with `AccessExclusive`.
    AccessShare,
    /// INSERT / UPDATE / DELETE. Conflicts with `Exclusive` and
    /// `AccessExclusive`.
    RowExclusive,
    /// Index creation (M2b/M3). Conflicts with `RowExclusive`, itself, and
    /// `AccessExclusive`.
    Exclusive,
    /// DDL (DROP, ALTER). Conflicts with every mode.
    AccessExclusive,
}

impl LockMode {
    /// The §9.2 conflict matrix, symmetric by construction:
    /// `AccessExclusive` conflicts with everything (including itself);
    /// `Exclusive` additionally conflicts with `RowExclusive` and itself.
    pub fn conflicts_with(self, other: LockMode) -> bool {
        use LockMode::{AccessExclusive, Exclusive, RowExclusive};
        matches!(
            (self, other),
            (AccessExclusive, _)
                | (_, AccessExclusive)
                | (RowExclusive, Exclusive)
                | (Exclusive, RowExclusive)
                | (Exclusive, Exclusive)
        )
    }
}

/// A queued lock request, FIFO order per table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Waiter {
    xid: TxnId,
    mode: LockMode,
}

/// Per-table lock state: the granted set plus the FIFO wait queue.
#[derive(Debug, Default)]
struct LockEntry {
    /// XID → held mode. Several XIDs may hold compatible modes concurrently;
    /// one XID holds exactly one (strongest) mode per table.
    granted: HashMap<TxnId, LockMode>,
    /// Blocked requesters in arrival order. A waiter for an *upgrade* keeps
    /// its old entry in `granted` while queued (see module docs).
    wait_queue: VecDeque<Waiter>,
}

impl LockEntry {
    /// Would `mode` for `xid` be grantable right now?
    ///
    /// Three gates, in order:
    ///
    /// 1. **Already held** at same-or-stronger mode → grant (no-op success).
    ///    This also covers waiters that [`LockManager::release_all`]
    ///    re-granted eagerly before they woke up.
    /// 2. **FIFO**: anyone queued ahead of `xid` blocks the grant, even when
    ///    the modes would be compatible — the anti-starvation rule.
    /// 3. **Conflict**: `mode` must not conflict with any granted mode held
    ///    by a *different* XID. `xid`'s own grant is ignored, which is what
    ///    makes upgrades possible at all.
    fn can_grant(&self, xid: TxnId, mode: LockMode) -> bool {
        if let Some(&held) = self.granted.get(&xid) {
            if held >= mode {
                return true;
            }
        }
        if let Some(head) = self.wait_queue.front() {
            if head.xid != xid {
                return false;
            }
        }
        self.granted
            .iter()
            .all(|(&holder, &held)| holder == xid || !held.conflicts_with(mode))
    }

    /// Record the grant, keeping the strongest mode (upgrade-in-place) and
    /// dropping `xid`'s queue entry if it has one (it can only be the head).
    fn grant(&mut self, xid: TxnId, mode: LockMode) {
        if self.wait_queue.front().is_some_and(|w| w.xid == xid) {
            self.wait_queue.pop_front();
        }
        let entry = self.granted.entry(xid).or_insert(mode);
        if mode > *entry {
            *entry = mode;
        }
    }

    /// Queue `xid` unless it already has a pending request on this table.
    fn enqueue_if_absent(&mut self, xid: TxnId, mode: LockMode) {
        if !self.wait_queue.iter().any(|w| w.xid == xid) {
            self.wait_queue.push_back(Waiter { xid, mode });
        }
    }

    /// Re-grant queue heads in FIFO order after a release: walk from the
    /// head, granting each waiter that is compatible with the granted set,
    /// stopping at the first that still conflicts. Because `can_grant`
    /// requires the head position, this naturally grants all compatible
    /// *consecutive* head waiters (e.g. two queued `AccessShare` readers
    /// behind a departing `AccessExclusive` both proceed; an `Exclusive`
    /// behind them does not).
    fn regrant_heads(&mut self) {
        while let Some(head) = self.wait_queue.front() {
            let Waiter { xid, mode } = *head;
            if !self.can_grant(xid, mode) {
                break;
            }
            self.grant(xid, mode);
        }
    }
}

/// Snapshot of one table's lock state, for tests and for Stage R's deadlock
/// detector (wait-for graph edges: each queued waiter → every conflicting
/// grant holder).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLockState {
    /// Granted (XID, mode) pairs, unordered.
    pub granted: Vec<(TxnId, LockMode)>,
    /// Queued (XID, mode) pairs in FIFO order.
    pub waiters: Vec<(TxnId, LockMode)>,
}

/// Table-level lock manager: `HashMap<Oid, LockEntry>` behind a single
/// mutex, with one condvar shared by all waiters.
///
/// Depends only on `pg-storage` types (`Oid`, `TxnId`) — never on
/// `pg-catalog` (tech-selection §一 dependency rule).
#[derive(Debug, Default)]
pub struct LockManager {
    entries: Mutex<HashMap<Oid, LockEntry>>,
    /// Shared by waiters of every table. Wakeups are `notify_all`: a woken
    /// waiter re-checks `can_grant` and either proceeds (it was re-granted or
    /// reached a compatible head) or sleeps again. A per-table condvar would
    /// avoid cross-table wakeups but is not worth the extra map at M2c
    /// concurrency levels.
    condvar: Condvar,
}

impl LockManager {
    /// Create an empty lock manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire `mode` on `table` for `xid`, blocking in FIFO order until the
    /// lock can be granted.
    ///
    /// Re-acquisition follows 2PL upgrade semantics: same-or-weaker mode is a
    /// no-op, a stronger mode upgrades in place (waiting, if necessary, with
    /// the old grant retained). The lock is held until
    /// [`Self::release_all`].
    ///
    /// Never blocks while holding the internal mutex — `Condvar::wait`
    /// releases it — so two transactions deadlocked against each other wedge
    /// only themselves (Stage R will detect the cycle).
    ///
    /// # Errors
    ///
    /// Stage P never fails: the `LockError` return type exists so Stage R's
    /// deadlock detector can abort a victim's wait without an API change.
    pub fn acquire(&self, xid: TxnId, table: Oid, mode: LockMode) -> LockResult<()> {
        let mut entries = self.entries.lock();
        loop {
            let entry = entries.entry(table).or_default();
            if entry.can_grant(xid, mode) {
                entry.grant(xid, mode);
                return Ok(());
            }
            entry.enqueue_if_absent(xid, mode);
            self.condvar.wait(&mut entries);
        }
    }

    /// Non-blocking acquire: returns `Ok(true)` if the lock was granted
    /// (immediately, following the same FIFO and upgrade rules as
    /// [`Self::acquire`]), `Ok(false)` if it would have had to wait. Never
    /// enqueues: a failed `try_acquire` leaves no trace.
    pub fn try_acquire(&self, xid: TxnId, table: Oid, mode: LockMode) -> LockResult<bool> {
        let mut entries = self.entries.lock();
        let entry = entries.entry(table).or_default();
        if entry.can_grant(xid, mode) {
            entry.grant(xid, mode);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Release every table lock held by `xid` (commit/abort, 2PL release
    /// point) and re-grant compatible queue heads in FIFO order, then wake
    /// all waiters so the re-granted heads proceed.
    ///
    /// Any *queued* request by `xid` is also dropped defensively: in Stage P
    /// a waiting `acquire` cannot be interrupted, so `xid` cannot be queued
    /// and releasing at once — but Stage R's victim abort will create
    /// exactly that state, and silently discarding the queue entry is the
    /// correct cleanup for it.
    pub fn release_all(&self, xid: TxnId) {
        let mut entries = self.entries.lock();
        entries.retain(|_, entry| {
            entry.granted.remove(&xid);
            entry.wait_queue.retain(|w| w.xid != xid);
            entry.regrant_heads();
            // Drop the entry once the table has neither holders nor waiters,
            // so long runs do not accumulate empty entries per table.
            !entry.granted.is_empty() || !entry.wait_queue.is_empty()
        });
        self.condvar.notify_all();
    }

    /// Is `xid` holding `table` at `mode` or stronger? (test/observability)
    pub fn is_granted(&self, xid: TxnId, table: Oid, mode: LockMode) -> bool {
        self.entries
            .lock()
            .get(&table)
            .is_some_and(|e| e.granted.get(&xid).is_some_and(|&held| held >= mode))
    }

    /// Every table lock `xid` currently holds: `(table, mode)` pairs
    /// (test/observability).
    pub fn held_by(&self, xid: TxnId) -> Vec<(Oid, LockMode)> {
        let entries = self.entries.lock();
        let mut held: Vec<(Oid, LockMode)> = entries
            .iter()
            .filter_map(|(&table, entry)| entry.granted.get(&xid).map(|&mode| (table, mode)))
            .collect();
        held.sort_unstable();
        held
    }

    /// Snapshot of one table's granted set and wait queue — the input Stage
    /// R's deadlock detector needs to build wait-for edges from table locks
    /// (row-lock edges come from `TxnManager::wait_edges`). Returns `None`
    /// when the table has no lock state at all.
    pub fn table_lock_state(&self, table: Oid) -> Option<TableLockState> {
        self.entries.lock().get(&table).map(|entry| {
            let mut granted: Vec<(TxnId, LockMode)> =
                entry.granted.iter().map(|(&x, &m)| (x, m)).collect();
            granted.sort_unstable();
            TableLockState {
                granted,
                waiters: entry.wait_queue.iter().map(|w| (w.xid, w.mode)).collect(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use LockMode::{AccessExclusive, AccessShare, Exclusive, RowExclusive};

    const ALL: [LockMode; 4] = [AccessShare, RowExclusive, Exclusive, AccessExclusive];

    /// The §9.2 matrix, restated as data so the implementation cannot drift
    /// from the spec silently.
    const CONFLICTS: [[bool; 4]; 4] = [
        // held:    AS      RE      EX      AE      requested:
        [false, false, false, true],  // AccessShare
        [false, false, true, true],   // RowExclusive
        [false, true, true, true],    // Exclusive
        [true, true, true, true],     // AccessExclusive
    ];

    #[test]
    fn conflict_matrix_matches_spec() {
        for (i, &a) in ALL.iter().enumerate() {
            for (j, &b) in ALL.iter().enumerate() {
                assert_eq!(
                    a.conflicts_with(b),
                    CONFLICTS[i][j],
                    "{a:?} vs {b:?} disagrees with §9.2"
                );
                assert_eq!(
                    a.conflicts_with(b),
                    b.conflicts_with(a),
                    "matrix must be symmetric"
                );
            }
        }
    }

    #[test]
    fn strength_ordering() {
        assert!(AccessShare < RowExclusive);
        assert!(RowExclusive < Exclusive);
        assert!(Exclusive < AccessExclusive);
    }
}
