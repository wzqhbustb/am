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

/// Decide whether a heap tuple is visible under `snap` (M2a compatibility
/// entry point).
///
/// Takes the raw `t_xmin` / `t_xmax` header fields rather than a
/// `pg_am_heap::TupleHeader` on purpose: `pg-am-heap` depends on `pg-txn`, so
/// referencing its types here would create a dependency cycle.
///
/// This is the pre-curcid judgment kept for the existing AM call shape
/// (`is_visible(t_xmin, t_xmax, snap, clog)`). It is the `t_cid = 0`
/// equivalence path of [`PgVisibilityOracle`] with one deliberate difference:
/// the snapshot's own writes are always visible (`xmin == current_xid` short
/// circuits to committed), matching M2a auto-commit where the writer's own
/// snapshot must see its rows within the single statement. The full oracle
/// would hide them under `t_cid == curcid == 0`, which is the M2b self-scan
/// semantics — correct only once the executor advances `curcid` per statement
/// and stamps real `t_cid`s. Wave 3 migrates the AM to the oracle; until then
/// this function preserves the M2a behavior exactly.
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
    use smallvec::{smallvec, SmallVec};

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
        snap.xip = smallvec![TxnId(42)];
        let clog = NoOpClogAccessor;
        assert!(is_visible(TxnId(42), TxnId::INVALID, &snap, &clog));
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
        assert!(!is_visible(TxnId(20), TxnId::INVALID, &snap, &clog));
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
