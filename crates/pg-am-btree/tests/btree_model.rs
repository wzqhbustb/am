//! Stage M model-based proptest: random insert/delete/lookup sequences on a
//! real index must agree with a `BTreeMap<(key, tid)>` oracle, including a
//! full content comparison via range scan and a final structural validation.

use std::collections::BTreeMap;
use std::sync::Arc;

use pg_am_btree::key::{decode_i64, encode_i64};
use pg_am_btree::{BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use proptest::prelude::*;
use tempfile::TempDir;

const REL_OID: Oid = Oid(16_387);

#[derive(Debug, Clone)]
enum Op {
    Insert(i64),
    DeleteModelEntry,
    Lookup(i64),
    InsertDuplicate,
}

fn op_strategy() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        5 => (0i64..400).prop_map(Op::Insert),
        2 => Just(Op::DeleteModelEntry),
        3 => (0i64..400).prop_map(Op::Lookup),
        1 => Just(Op::InsertDuplicate),
    ];
    proptest::collection::vec(op, 50..200)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16)
    ))]

    #[test]
    fn matches_btree_map_model(ops in op_strategy()) {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let mut index = am.create_index(REL_OID, ColumnType::Int8).unwrap();

        let mut model: BTreeMap<(i64, Tid), ()> = BTreeMap::new();
        let mut next_tid: u64 = 0;
        let mut fresh_tid = || {
            next_tid += 1;
            Tid {
                page_id: PageId(50_000 + next_tid / 60_000),
                slot_id: (next_tid % 60_000) as u16,
            }
        };

        for (step, op) in ops.iter().enumerate() {
            match op {
                Op::Insert(k) => {
                    let t = fresh_tid();
                    index
                        .insert(&encode_i64(*k), t)
                        .unwrap_or_else(|e| panic!("step {step}: insert({k}) failed: {e}"));
                    model.insert((*k, t), ());
                }
                Op::DeleteModelEntry => {
                    let victim = model.keys().next().copied();
                    if let Some((k, t)) = victim {
                        index
                            .delete(&encode_i64(k), t)
                            .unwrap_or_else(|e| panic!("step {step}: delete({k}) failed: {e}"));
                        model.remove(&(k, t));
                    }
                }
                Op::Lookup(k) => {
                    let got = index.lookup(&encode_i64(*k)).unwrap();
                    let want = model
                        .range((*k, Tid { page_id: PageId(0), slot_id: 0 })..)
                        .next()
                        .and_then(|((mk, mt), _)| if mk == k { Some(*mt) } else { None });
                    prop_assert_eq!(got, want, "step {}: lookup({}) diverged", step, k);
                }
                Op::InsertDuplicate => {
                    if let Some((k, t)) = model.keys().next().copied() {
                        prop_assert!(index.insert(&encode_i64(k), t).is_err());
                    }
                }
            }

            // Periodically compare the full content through a range scan.
            if step % 25 == 24 {
                assert_full_content(&index, &model);
            }
        }

        assert_full_content(&index, &model);
        index.validate().unwrap();
    }
}

fn assert_full_content(index: &BTreeIndex, model: &BTreeMap<(i64, Tid), ()>) {
    let rows = index.range_scan(None, None).unwrap();
    let got: Vec<(i64, Tid)> = rows
        .iter()
        .map(|(k, t)| (decode_i64(k.clone().try_into().unwrap()), *t))
        .collect();
    let want: Vec<(i64, Tid)> = model.keys().copied().collect();
    assert_eq!(got, want, "full range scan must equal the model");
}
