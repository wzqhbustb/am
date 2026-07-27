//! pg_rust transaction layer — Phase 1 M2.
//!
//! This crate implements transaction management, MVCC visibility, and locking:
//! - XID allocation (`TxnIdClock`)
//! - CLOG (transaction status log) with `ClogBuffer` SLRU cache
//! - Snapshot and `VisibilityOracle`
//! - Lock Manager (row-level via tuple.xmax + table-level 4-mode locks)
//!
//! It depends only on `pg-storage` for physical types and primitives.
//!
//! # M2a scope (Stage I–J)
//!
//! Stage I added the minimal [`Snapshot`] + [`is_visible`] surface for heap
//! scan. Stage J adds the [`manager::TxnManager`] (XID allocation + durable
//! commit/abort), the [`clog_mem::InMemoryClogAccessor`] (a real CLOG that
//! records aborts), and the [`redo`] handlers that rebuild the CLOG from the
//! WAL on recovery. The disk-backed CLOG SLRU and lock manager arrive later.
//! Visibility runs against a [`ClogAccessor`]; wiring in
//! [`clog_mem::InMemoryClogAccessor`] makes abort invisibility observable.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod clog_mem;
pub mod manager;
pub mod redo;

pub use clog_mem::InMemoryClogAccessor;
pub use manager::{CommitWal, TxnManager};
pub use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::types::TxnId;
pub use redo::txn_redo_handlers;

/// An MVCC snapshot: the set of transactions whose effects are visible.
///
/// This is the minimal M2a form (tech-selection §6). PostgreSQL uses a
/// `SmallVec` for `xip`; M2a uses `Vec` and adds `curcid` in Stage J.
///
/// Interpretation:
/// - every XID `< xmin` is complete (committed or aborted) — consult the CLOG;
/// - every XID `>= xmax` started after this snapshot and is invisible;
/// - `xip` lists the XIDs in `[xmin, xmax)` that were still in progress when
///   the snapshot was taken; they are invisible even if they later commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Lowest XID that is still considered running (all `< xmin` are complete).
    pub xmin: TxnId,
    /// First XID not yet assigned when the snapshot was taken.
    pub xmax: TxnId,
    /// In-progress XIDs in `[xmin, xmax)` at snapshot time.
    pub xip: Vec<TxnId>,
    /// The XID of the transaction that took this snapshot (sees its own writes).
    pub current_xid: TxnId,
    /// Command counter within the current transaction.
    ///
    /// PostgreSQL uses `curcid` to make earlier commands within the same
    /// transaction visible to later ones while hiding the effects of the
    /// current command from itself (avoiding the Halloween problem). M2a runs
    /// one command per auto-commit transaction, so this is `0` throughout;
    /// it exists now so multi-statement transactions (later stages) need no
    /// signature change.
    pub curcid: u32,
}

impl Snapshot {
    /// A snapshot that sees every committed transaction and no in-progress one.
    ///
    /// M2a runs in auto-commit with no live concurrent writers, so "see all
    /// committed" is the correct scan snapshot: `xmin = 0`, `xmax = u64::MAX`,
    /// empty `xip`. Combined with `NoOpClogAccessor` every non-deleted tuple is
    /// visible; once Stage J supplies a real CLOG, aborted inserters drop out
    /// automatically.
    pub fn everything() -> Self {
        Snapshot {
            xmin: TxnId(0),
            xmax: TxnId(u64::MAX),
            xip: Vec::new(),
            current_xid: TxnId::INVALID,
            curcid: 0,
        }
    }
}

/// Whether `xid` counts as committed *and* visible relative to `snap`.
///
/// A transaction is visible when it either is the snapshot's own transaction,
/// or it completed before the snapshot (`< xmax`, not in `xip`) and the CLOG
/// records it as committed.
fn is_effectively_committed(xid: TxnId, snap: &Snapshot, clog: &dyn ClogAccessor) -> bool {
    if xid == TxnId::INVALID {
        return false;
    }
    if xid == snap.current_xid {
        return true;
    }
    if xid >= snap.xmax {
        return false;
    }
    if snap.xip.contains(&xid) {
        return false;
    }
    clog.get_state(xid) == TxnState::Committed
}

/// Decide whether a heap tuple is visible under `snap`.
///
/// Takes the raw `t_xmin` / `t_xmax` header fields rather than a
/// `pg_am_heap::TupleHeader` on purpose: `pg-am-heap` depends on `pg-txn`, so
/// referencing its types here would create a dependency cycle.
///
/// A tuple is visible when its inserter is committed-and-visible and it has not
/// been deleted by a committed-and-visible transaction.
pub fn is_visible(t_xmin: TxnId, t_xmax: TxnId, snap: &Snapshot, clog: &dyn ClogAccessor) -> bool {
    if !is_effectively_committed(t_xmin, snap, clog) {
        return false;
    }
    if t_xmax == TxnId::INVALID {
        return true;
    }
    // Deleted: invisible only if the deleting transaction is itself visible.
    !is_effectively_committed(t_xmax, snap, clog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_storage::clog::NoOpClogAccessor;

    #[test]
    fn everything_sees_live_tuple() {
        let snap = Snapshot::everything();
        let clog = NoOpClogAccessor;
        assert!(is_visible(TxnId(5), TxnId::INVALID, &snap, &clog));
    }

    #[test]
    fn everything_hides_deleted_tuple() {
        let snap = Snapshot::everything();
        let clog = NoOpClogAccessor;
        // NoOpClog treats the deleter as committed, so a set xmax hides the row.
        assert!(!is_visible(TxnId(5), TxnId(6), &snap, &clog));
    }

    #[test]
    fn own_insert_is_visible_even_if_uncommitted() {
        let mut snap = Snapshot::everything();
        snap.current_xid = TxnId(42);
        snap.xmax = TxnId(43);
        snap.xip = vec![TxnId(42)];
        let clog = NoOpClogAccessor;
        assert!(is_visible(TxnId(42), TxnId::INVALID, &snap, &clog));
    }

    #[test]
    fn future_inserter_is_invisible() {
        let snap = Snapshot {
            xmin: TxnId(1),
            xmax: TxnId(10),
            xip: Vec::new(),
            current_xid: TxnId::INVALID,
            curcid: 0,
        };
        let clog = NoOpClogAccessor;
        assert!(!is_visible(TxnId(20), TxnId::INVALID, &snap, &clog));
    }
}
