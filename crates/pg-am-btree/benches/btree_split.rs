//! Stage T benchmark inventory: B+Tree split throughput.
//!
//! The Stage T benchmark set (coding-plan Stage T, `docs/phase1-m2-benchmarks.md`)
//! calls for a "B+Tree split 吞吐" measurement. `create_index` measures the
//! blocking bulk BUILD path (1M preloaded rows), not online splits, and
//! `m2c_btree_tps` mixes splits into an engine-level fsync-bound number —
//! this bench isolates the split path: sequential single-threaded
//! `BTreeIndex::insert` of ~500-byte Text keys, so only ~15 entries fit in
//! a leaf page and a leaf split fires every ~15 inserts (root splits as the
//! tree levels up). No commits are interleaved (pure AM path; the WAL
//! group-commit worker fsyncs in the background), so the number reflects
//! insert+split throughput, not fsync latency.
//!
//! The measured domain is the insert loop only (`iter_custom`): the
//! closing `validate()`, the `tree_level >= 1` vacuous-run guard and the
//! engine shutdown run untimed, so the reported number is insert+split
//! throughput, not teardown latency.
//!
//! Run with: `cargo bench -p pg-am-btree --bench btree_split`
//! Smoke: `BTREE_SPLIT_KEYS=200 cargo bench -p pg-am-btree --bench btree_split -- \
//!     --measurement-time 2 --sample-size 10`

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use pg_am_btree::{BTreeAM, BTreeIndex};
use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

const REL_OID: Oid = Oid(16_387);
/// Key payload size: ~500B keys ⇒ ~15 entries per 8K leaf ⇒ a leaf split
/// every ~15 inserts.
const KEY_PAD: usize = 500;

fn split_keys() -> i32 {
    std::env::var("BTREE_SPLIT_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000)
}

/// A unique, order-preserving key of ~`KEY_PAD` bytes (same construct as
/// the crash-rounds harness's split driver).
fn key(i: i32) -> Vec<u8> {
    format!("k{i:08}{}", "x".repeat(KEY_PAD)).into_bytes()
}

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(42_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: StorageEngine,
    index: BTreeIndex,
}

fn setup() -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.create_index(REL_OID, ColumnType::Text).unwrap();
    Fixture {
        _tmp: tmp,
        engine,
        index,
    }
}

fn bench_btree_split(c: &mut Criterion) {
    let n = split_keys();
    let mut group = c.benchmark_group("btree_split");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("insert_wide_keys_sequential", |b| {
        // iter_custom: only the inserts are timed; the vacuous-run guard,
        // validate() and the clean shutdown are teardown and must not count
        // toward insert/split throughput.
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut fixture = setup();
                let start = Instant::now();
                for i in 0..n {
                    fixture.index.insert(&key(i), tid(i as u64)).unwrap();
                }
                total += start.elapsed();
                // Untimed: the run must actually have split — ~15
                // keys/leaf, so past a few dozen keys a single-leaf tree is
                // impossible.
                assert!(
                    fixture.index.tree_level() >= 1,
                    "{n} wide keys but the tree never split — vacuous measurement"
                );
                fixture.index.validate().unwrap();
                fixture.engine.shutdown();
            }
            total
        });
    });
    group.finish();
}

criterion_group!(benches, bench_btree_split);
criterion_main!(benches);
