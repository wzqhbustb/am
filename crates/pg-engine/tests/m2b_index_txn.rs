//! M2b Stage O review acceptance: index maintenance is transactional.
//!
//! The B+Tree holds `(key, tid)` entries with no MVCC metadata, so the
//! engine keeps a per-transaction index undo log: abort reverse-applies it
//! (explicit `TxnHandle::abort`, `TxnHandle::drop` auto-abort, and the
//! auto-commit failure path), commit discards it, and `index_lookup`
//! re-checks every candidate TID against heap visibility (duplicates
//! included — M2b indexes are non-unique).
//!
//! Acceptance: `cargo test -p pg-engine --test m2b_index_txn`

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Engine with table `t (id INT)` and an index on `id`, no rows.
fn setup() -> (TempDir, Engine) {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (id INT)").unwrap();
    engine.exec(None, "CREATE INDEX ON t (id)").unwrap();
    (tmp, engine)
}

fn scan_count(engine: &Engine) -> usize {
    engine.scan("t", None).unwrap().len()
}

fn lookup(engine: &Engine, key: i32) -> bool {
    engine
        .index_lookup("t", "id", &Datum::Int4(key))
        .unwrap()
        .is_some()
}

/// INSERT inside an explicit txn, then ABORT: the index entry must be gone
/// AND the heap scan must be empty.
#[test]
fn insert_abort_removes_index_entry() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (1)").unwrap();
    txn.abort().unwrap();
    assert!(!lookup(&engine, 1), "aborted insert left a dangling index entry");
    assert_eq!(scan_count(&engine), 0, "aborted insert visible to scan");
}

/// DELETE inside an explicit txn, then ABORT: the index entry must be
/// restored AND the scan must find the row.
#[test]
fn delete_abort_restores_index_entry() {
    let (_tmp, engine) = setup();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "DELETE FROM t WHERE id = 1").unwrap();
    txn.abort().unwrap();
    assert!(lookup(&engine, 1), "aborted delete lost the live row's index entry");
    assert_eq!(scan_count(&engine), 1, "aborted delete hid the row from scan");
}

/// UPDATE (key change) inside an explicit txn, then ABORT: the old key
/// must resolve again, the new key must not, and the scan must show the
/// original row.
#[test]
fn update_abort_restores_old_key_entry() {
    let (_tmp, engine) = setup();
    engine.exec(None, "INSERT INTO t VALUES (1)").unwrap();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "UPDATE t SET id = 2 WHERE id = 1").unwrap();
    txn.abort().unwrap();
    assert!(lookup(&engine, 1), "aborted update lost the old key's entry");
    assert!(!lookup(&engine, 2), "aborted update left the new key's entry");
    assert_eq!(scan_count(&engine), 1);
}

/// INSERT + COMMIT: the index entry stays and resolves.
#[test]
fn insert_commit_keeps_index_entry() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (7)").unwrap();
    txn.commit().unwrap();
    assert!(lookup(&engine, 7));
    assert_eq!(scan_count(&engine), 1);
}

/// Dropping the handle without commit auto-aborts: the index entry must be
/// undone too.
#[test]
fn txn_handle_drop_auto_abort_undoes_index() {
    let (_tmp, engine) = setup();
    {
        let txn = engine.begin_txn().unwrap();
        engine.exec(Some(&txn), "INSERT INTO t VALUES (3)").unwrap();
        // No commit/abort: drop triggers the best-effort auto-abort.
    }
    assert!(!lookup(&engine, 3), "drop auto-abort left a dangling index entry");
    assert_eq!(scan_count(&engine), 0);
}

/// A failing auto-commit statement (row 2 violates the column type) must
/// undo row 1's index entry as well as its heap insert.
#[test]
fn auto_commit_failure_undoes_index() {
    let (_tmp, engine) = setup();
    let res = engine.exec(None, "INSERT INTO t VALUES (1), ('not-an-int')");
    assert!(res.is_err(), "type-mismatched row must fail the statement");
    assert!(!lookup(&engine, 1), "failed statement left a dangling index entry");
    assert_eq!(scan_count(&engine), 0, "failed statement left a visible row");
}

/// Duplicate keys (non-unique index): two rows share key 5; after one is
/// deleted and committed, the lookup must skip the dead version and return
/// the live one.
#[test]
fn duplicate_keys_lookup_returns_visible_row() {
    let (_tmp, engine) = setup();
    engine.exec(None, "INSERT INTO t VALUES (5), (5)").unwrap();
    // Delete one of the two duplicates (auto-commit).
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 2);
    engine.delete("t", rows[0].0).unwrap();
    let tid = engine
        .index_lookup("t", "id", &Datum::Int4(5))
        .unwrap()
        .expect("one live duplicate must still resolve");
    assert_eq!(tid, rows[1].0, "lookup must skip the deleted duplicate");
    assert_eq!(scan_count(&engine), 1);
}

/// The same insert-abort scenario driven purely through SQL exec with an
/// explicit transaction.
#[test]
fn sql_exec_insert_abort_index_consistent() {
    let (_tmp, engine) = setup();
    let txn = engine.begin_txn().unwrap();
    engine.exec(Some(&txn), "INSERT INTO t VALUES (9)").unwrap();
    // Sanity: inside the txn the row is visible to its own SELECT.
    match engine.exec(Some(&txn), "SELECT * FROM t").unwrap() {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Rows, got {other:?}"),
    }
    txn.abort().unwrap();
    assert!(!lookup(&engine, 9));
    match engine.exec(None, "SELECT * FROM t").unwrap() {
        QueryResult::Rows { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected Rows, got {other:?}"),
    }
}
