//! MVCC snapshot (tech-selection §7.1).
//!
//! A [`Snapshot`] is the set of transactions whose effects are visible to one
//! scan. It is fully XID-based: visibility never compares LSNs. The v1
//! `snapshot_lsn` field was removed (v2 revision P2-8) — hint bits cache CLOG
//! outcomes, which are idempotent states, so readers need no LSN boundary to
//! shield themselves from "future" transactions.
//!
//! Interpretation:
//! - every XID `< xmin` is complete (committed or aborted) — consult the CLOG;
//! - every XID `>= xmax` started after this snapshot and is invisible;
//! - `xip` lists the XIDs in `[xmin, xmax)` that were still in progress when
//!   the snapshot was taken; they are invisible even if they later commit.

use smallvec::SmallVec;

use pg_storage::types::TxnId;

/// An MVCC snapshot: the set of transactions whose effects are visible
/// (tech-selection §7.1).
///
/// `xip` is a [`SmallVec`] with 32 inline slots (§16 approved dependency): the
/// overwhelming majority of snapshots see far fewer than 32 concurrent
/// transactions, so the common case never touches the heap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Lowest XID that is still considered running (all `< xmin` are complete).
    pub xmin: TxnId,
    /// First XID not yet assigned when the snapshot was taken.
    pub xmax: TxnId,
    /// In-progress XIDs in `[xmin, xmax)` at snapshot time, sorted ascending.
    pub xip: SmallVec<[TxnId; 32]>,
    /// The XID of the transaction that took this snapshot (sees its own writes).
    pub current_xid: TxnId,
    /// Command counter within the current transaction (v2.3, §7.1 Q4).
    ///
    /// The executor increments `curcid` by one **before** each SQL statement
    /// starts executing (not at commit, not at statement end). Every tuple
    /// written during the statement carries `t_cid = curcid`; a self-scan
    /// inside the same statement shares that `curcid`, so `t_cid < curcid` is
    /// false for the statement's own writes and they are skipped (Halloween
    /// protection). When the next statement advances the counter, the previous
    /// statement's writes become `t_cid < curcid` and turn into visible
    /// "writes by an earlier command".
    ///
    /// M2a ran one command per auto-commit transaction, so this stayed `0`
    /// (dead code path); M2b Stage L activates the increment protocol via
    /// [`Snapshot::advance_curcid`].
    pub curcid: u32,
}

impl Snapshot {
    /// A snapshot that sees every committed transaction and no in-progress one.
    ///
    /// M2a compatibility path: `xmin = 0`, `xmax = u64::MAX`, empty `xip`.
    /// Combined with a real CLOG, aborted inserters drop out automatically.
    pub fn everything() -> Self {
        Snapshot {
            xmin: TxnId(0),
            xmax: TxnId(u64::MAX),
            xip: SmallVec::new(),
            current_xid: TxnId::INVALID,
            curcid: 0,
        }
    }

    /// Advance the command counter by one and return the new value (§7.1 Q4).
    ///
    /// The executor calls this **before** each SQL statement of a
    /// multi-statement transaction begins; the returned value is stamped into
    /// every tuple the statement writes as `t_cid`. All `is_visible` calls
    /// within one statement observe the same `curcid` (the statement shares
    /// the counter), so the statement never re-scans its own writes.
    ///
    /// # Two couplings the caller must respect (Stage L review)
    ///
    /// - **RC isolation**: a per-statement `TxnManager::snapshot()` call
    ///   always returns `curcid = 0`, so an RC executor must carry the
    ///   command counter in the transaction/executor state and inject it
    ///   here (set `snapshot.curcid` after obtaining the snapshot) — the
    ///   counter is transaction-scoped, not snapshot-scoped. Stage O wires
    ///   this.
    /// - **`t_cid` stamping**: advancing the counter is meaningless unless
    ///   every tuple written by the statement is stamped with the new value
    ///   (tuple header offset 60..64). Migrating heap scans to the
    ///   `PgVisibilityOracle` without stamping `t_cid` silently disables the
    ///   Halloween protection (all writes read as `t_cid = 0 < curcid`).
    pub fn advance_curcid(&mut self) -> u32 {
        self.curcid += 1;
        self.curcid
    }
}
