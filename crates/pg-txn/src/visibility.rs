//! Visibility Oracle (tech-selection §7.2, contract with AMs in §7.3).
//!
//! The oracle is the single entry point for MVCC visibility judgments: every
//! AM that stores `xmin`/`xmax` in its tuples (heap AM today, HNSW tuples in
//! Phase 2) must call [`VisibilityOracle::is_visible`] before returning a
//! tuple upward. Pure index AMs hold `(key, tid)` entries without `xmin`/`xmax`
//! and never judge visibility themselves — the heap lookup by `tid` does it.
//!
//! The oracle depends only on `pg-txn`-internal pieces ([`ClogAccessor`]); AMs
//! never read the CLOG directly.

use std::sync::Arc;

use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::types::{Tid, TxnId};

use crate::snapshot::Snapshot;

/// The outcome of a visibility judgment (tech-selection §7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    /// The tuple version is visible to the snapshot.
    Visible,
    /// The tuple version is invisible to the snapshot.
    Invisible,
    /// The tuple version is being modified by a concurrent in-progress
    /// transaction (`xmax IN_PROGRESS`) and the caller must consult the lock
    /// manager before deciding.
    ///
    /// Never returned under M2b semantics — this variant exists so the row
    /// lock-wait protocol of M2c keeps a stable interface (v2.3 P2-2). Where
    /// the §7.2 pseudocode says "见 (Uncertain 若 M2c 需要写锁)", M2b answers
    /// [`Visibility::Visible`].
    Uncertain,
}

/// A hint-bit write-back request for a tuple header's infomask.
///
/// The variants map one-to-one onto the `pg-am-heap` infomask constants
/// `HEAP_XMIN_COMMITTED` / `HEAP_XMIN_INVALID` / `HEAP_XMAX_COMMITTED` /
/// `HEAP_XMAX_INVALID` (`pg-am-heap` cannot be referenced here — it depends on
/// `pg-txn`, so the mapping is re-declared at the write-back site).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintBit {
    /// The inserting transaction is known committed.
    XminCommitted,
    /// The inserting transaction is known aborted.
    XminInvalid,
    /// The deleting transaction is known committed.
    XmaxCommitted,
    /// The deleting transaction is known aborted.
    XmaxInvalid,
}

/// Unified visibility judgment entry point (tech-selection §7.2).
pub trait VisibilityOracle {
    /// Decide whether a tuple version is visible under `snapshot`.
    ///
    /// - `xmin` / `xmax`: the transaction IDs from the tuple header;
    /// - `t_cid`: the command ID from the tuple header (v2.3);
    /// - `current_xid` / `curcid` are taken from `snapshot`.
    fn is_visible(&self, xmin: TxnId, xmax: TxnId, t_cid: u32, snapshot: &Snapshot) -> Visibility;

    /// Optionally flush a tuple's hint bit asynchronously (read path may call;
    /// returns no error).
    ///
    /// Stage L implements judgment only; the write-back channel arrives in
    /// Phase 7, so the default is a no-op. Callers must therefore treat hint
    /// bits purely as a cache and always be prepared to re-run the full
    /// judgment.
    fn set_hint_bit(&self, _tid: Tid, _hint: HintBit) {
        // No-op: Phase 7 wires this to the buffer-pool page holding `tid`.
    }
}

/// The production oracle: full PostgreSQL textbook judgment over a
/// [`ClogAccessor`] (tech-selection §7.2).
#[derive(Debug)]
pub struct PgVisibilityOracle {
    clog: Arc<dyn ClogAccessor>,
}

impl PgVisibilityOracle {
    /// Create an oracle that resolves transaction states through `clog`.
    pub fn new(clog: Arc<dyn ClogAccessor>) -> Self {
        Self { clog }
    }
}

impl VisibilityOracle for PgVisibilityOracle {
    /// Textbook judgment, following the §7.2 pseudocode line by line.
    fn is_visible(&self, xmin: TxnId, xmax: TxnId, t_cid: u32, snapshot: &Snapshot) -> Visibility {
        let self_xid = snapshot.current_xid;
        let curcid = snapshot.curcid;

        // §7.2 step 1 — xmin 判定.
        if xmin == self_xid {
            // 自己写的: visible only when written by an earlier command
            // (`t_cid < curcid`); the current command's own writes do not
            // participate in its own scan (UPDATE-loop avoidance). M2b
            // activation point (v2.3-3 / Q4).
            if t_cid >= curcid {
                return Visibility::Invisible;
            }
            // `t_cid < curcid`: fall through to the xmax 判定.
        } else {
            if xmin >= snapshot.xmax {
                return Visibility::Invisible; // 未来事务
            }
            if snapshot.xip.contains(&xmin) {
                return Visibility::Invisible; // 并发未提交
            }
            if self.clog.get_state(xmin) != TxnState::Committed {
                return Visibility::Invisible; // 未提交/已回滚
            }
        }
        // → 到这里 xmin 已提交且早于快照 (or self, written by an earlier command).

        // §7.2 step 2 — xmax 判定.
        if xmax == TxnId::INVALID {
            return Visibility::Visible; // 未被删除
        }
        if xmax == self_xid {
            // 自己删的 (symmetric M2b activation point, v2.3-3 / Q4): deleted
            // by an earlier command → invisible to the current command's
            // SELECT; deleted by the current command → still visible (a
            // same-command DELETE ... RETURNING reads its own victim through
            // the output channel, and a same-command re-scan tolerates it).
            return if t_cid < curcid {
                Visibility::Invisible
            } else {
                Visibility::Visible
            };
        }
        if xmax >= snapshot.xmax {
            return Visibility::Visible; // 未来删除
        }
        if snapshot.xip.contains(&xmax) {
            // 并发未提交删除 → 见 (§7.2: "Uncertain 若 M2c 需要写锁"; M2b
            // has no lock-wait protocol, so it answers Visible — see the
            // `Uncertain` variant doc, v2.3 P2-2).
            return Visibility::Visible;
        }
        if self.clog.get_state(xmax) != TxnState::Committed {
            return Visibility::Visible; // 删除未提交/已回滚
        }
        Visibility::Invisible
    }
}

/// Decide whether a heap tuple is visible under `snap` (§7.2 full logic).
///
/// Takes the raw `t_xmin` / `t_xmax` / `t_cid` header fields rather than a
/// `pg_am_heap::TupleHeader` on purpose: `pg-am-heap` depends on `pg-txn`, so
/// referencing its types here would create a dependency cycle.
///
/// This is the `bool`-returning twin of [`PgVisibilityOracle::is_visible`],
/// kept for the AM's `&dyn ClogAccessor` call shape (the oracle takes
/// `Arc<dyn ClogAccessor>`).
pub fn is_visible(
    t_xmin: TxnId,
    t_xmax: TxnId,
    t_cid: u32,
    snap: &Snapshot,
    clog: &dyn ClogAccessor,
) -> bool {
    let self_xid = snap.current_xid;
    let curcid = snap.curcid;

    // §7.2 step 1 — xmin 判定.
    if t_xmin == self_xid {
        // 自己写的: visible only when written by an earlier command
        // (`t_cid < curcid`); the current command's own writes do not
        // participate in its own scan (UPDATE-loop avoidance, v2.3-3 / Q4).
        if t_cid >= curcid {
            return false;
        }
        // `t_cid < curcid`: fall through to the xmax 判定.
    } else {
        if t_xmin >= snap.xmax {
            return false; // 未来事务
        }
        if snap.xip.contains(&t_xmin) {
            return false; // 并发未提交
        }
        if clog.get_state(t_xmin) != TxnState::Committed {
            return false; // 未提交/已回滚
        }
    }
    // → 到这里 xmin 已提交且早于快照 (or self, written by an earlier command).

    // §7.2 step 2 — xmax 判定.
    if t_xmax == TxnId::INVALID {
        return true; // 未被删除
    }
    if t_xmax == self_xid {
        // 自己删的: deleted by an earlier command → invisible; deleted by
        // the current command → still visible (DELETE ... RETURNING reads its
        // own victim through the output channel).
        return t_cid >= curcid;
    }
    if t_xmax >= snap.xmax {
        return true; // 未来删除
    }
    if snap.xip.contains(&t_xmax) {
        // 并发未提交删除 → M2b answers Visible (no lock-wait protocol, v2.3 P2-2).
        return true;
    }
    if clog.get_state(t_xmax) != TxnState::Committed {
        return true; // 删除未提交/已回滚
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_storage::clog::NoOpClogAccessor;
    use smallvec::{smallvec, SmallVec};

    #[test]
    fn everything_sees_live_tuple() {
        let snap = Snapshot::everything();
        let clog = NoOpClogAccessor;
        assert!(is_visible(TxnId(5), TxnId::INVALID, 0, &snap, &clog));
    }

    #[test]
    fn everything_hides_deleted_tuple() {
        let snap = Snapshot::everything();
        let clog = NoOpClogAccessor;
        // NoOpClog treats the deleter as committed, so a set xmax hides the row.
        assert!(!is_visible(TxnId(5), TxnId(6), 0, &snap, &clog));
    }

    #[test]
    fn own_insert_visible_when_written_by_earlier_command() {
        let mut snap = Snapshot::everything();
        snap.current_xid = TxnId(42);
        snap.curcid = 1; // advanced past the insert command
        snap.xmax = TxnId(43);
        snap.xip = smallvec![TxnId(42)];
        let clog = NoOpClogAccessor;
        // t_cid=0 < curcid=1 → visible (written by an earlier command).
        assert!(is_visible(TxnId(42), TxnId::INVALID, 0, &snap, &clog));
    }

    #[test]
    fn own_insert_invisible_to_current_command() {
        let mut snap = Snapshot::everything();
        snap.current_xid = TxnId(42);
        snap.curcid = 1; // the insert command itself
        snap.xmax = TxnId(43);
        snap.xip = smallvec![TxnId(42)];
        let clog = NoOpClogAccessor;
        // t_cid=1 == curcid=1 → invisible (current command's own write).
        assert!(!is_visible(TxnId(42), TxnId::INVALID, 1, &snap, &clog));
    }

    #[test]
    fn future_inserter_is_invisible() {
        let snap = Snapshot {
            xmin: TxnId(1),
            xmax: TxnId(10),
            xip: SmallVec::new(),
            current_xid: TxnId::INVALID,
            curcid: 0,
        };
        let clog = NoOpClogAccessor;
        assert!(!is_visible(TxnId(20), TxnId::INVALID, 0, &snap, &clog));
    }

    #[test]
    fn oracle_future_and_xip_branches() {
        use crate::InMemoryClogAccessor;
        let clog: Arc<dyn ClogAccessor> = Arc::new(InMemoryClogAccessor::new());
        clog.set_state(TxnId(3), TxnState::Committed);
        let oracle = PgVisibilityOracle::new(clog);
        let snap = Snapshot {
            xmin: TxnId(4),
            xmax: TxnId(10),
            xip: smallvec![TxnId(5)],
            current_xid: TxnId(8),
            curcid: 0,
        };
        // Committed before the snapshot and not deleted → visible.
        assert_eq!(
            oracle.is_visible(TxnId(3), TxnId::INVALID, 0, &snap),
            Visibility::Visible
        );
        // Inserter in xip (in progress at snapshot time) → invisible.
        assert_eq!(
            oracle.is_visible(TxnId(5), TxnId::INVALID, 0, &snap),
            Visibility::Invisible
        );
        // Inserter at/after xmax (future) → invisible.
        assert_eq!(
            oracle.is_visible(TxnId(10), TxnId::INVALID, 0, &snap),
            Visibility::Invisible
        );
    }
}
