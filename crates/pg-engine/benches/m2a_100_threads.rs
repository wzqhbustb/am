//! M2a Stage K concurrency acceptance bench (coding-plan Stage K, v2.3-18):
//! 100 threads x 1000 INSERT (100K rows total) through the programmatic
//! `Engine` API, plus a 1-thread reference point.
//!
//! Measured (Apple Silicon, macOS, release; full 1000 ops/thread):
//!
//! - 1T x 1000 INSERT:   **~239 TPS** — bounded by raw fsync latency
//!   (F_FULLFSYNC ~4 ms/commit), matching Stage J's ~220 ops/s reference.
//! - 100T x 1000 INSERT (100K rows): **~11.6K TPS** — group commit amortizes
//!   fsync across concurrent committers (Stage J's txn_commit_concurrent
//!   measured ~12K at 100T; the engine API adds registry lookup + tuple
//!   encode per op, costing ~3%). Comfortably above the Stage K >= 3K target.
//! - Full-table scan of 100K pre-loaded rows: **~3.5M rows/s** (~28 ms per
//!   scan) — 17x the Stage K >= 200K rows/s target. This is a warm-cache
//!   number: 100K small rows fit the default 128 MB buffer pool, so the
//!   measurement bounds decode + visibility + materialize, not disk I/O.
//!
//! The 20K single-thread / 30K group-commit aspirations in the plan are not
//! reachable on this hardware with per-commit fsync semantics (see the Stage J
//! plan note: the "single-thread 30K" wording was already flagged there as a
//! physical impossibility, not a regression of this stage).
//!
//! Correctness gates checked inside every timed iteration: TID uniqueness
//! (no slot conflicts) and an exact post-insert row count.
//!
//! `M2A_BENCH_OPS` overrides the per-thread op count (default 1000) for
//! quicker smoke runs.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig, Tid};

fn ops_per_thread() -> usize {
    std::env::var("M2A_BENCH_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
}

struct Fixture {
    _tmp: tempfile::TempDir,
    engine: Arc<Engine>,
}

fn setup() -> Fixture {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .create_table(
            "t",
            &[
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Int4,
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::Text,
                },
            ],
        )
        .unwrap();
    Fixture { _tmp: tmp, engine }
}

/// Run `threads` x `ops` inserts, asserting no slot conflicts and an exact
/// final row count. Returns nothing; failures panic (criterion reports them).
fn run_concurrent_inserts(engine: &Arc<Engine>, threads: usize, ops: usize) {
    let tids = Arc::new(Mutex::new(Vec::with_capacity(threads * ops)));
    std::thread::scope(|s| {
        for t in 0..threads {
            let engine = Arc::clone(engine);
            let tids = Arc::clone(&tids);
            s.spawn(move || {
                for i in 0..ops {
                    let tid = engine
                        .insert(
                            "t",
                            &[
                                Some(Datum::Int4((t * ops + i) as i32)),
                                Some(Datum::Text("bench".to_string())),
                            ],
                        )
                        .unwrap();
                    tids.lock().unwrap().push(tid);
                }
            });
        }
    });

    let tids = tids.lock().unwrap();
    assert_eq!(tids.len(), threads * ops);
    let unique: HashSet<&Tid> = tids.iter().collect();
    assert_eq!(unique.len(), tids.len(), "slot conflict: duplicate TIDs");
    drop(tids);

    assert_eq!(
        engine.scan("t", None).unwrap().len(),
        threads * ops,
        "row count mismatch after concurrent inserts"
    );
}

fn bench_m2a_concurrent_insert(c: &mut Criterion) {
    let ops = ops_per_thread();
    let mut group = c.benchmark_group("m2a_100_threads");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    for &threads in &[1usize, 100] {
        group.throughput(Throughput::Elements((threads * ops) as u64));
        group.bench_with_input(
            BenchmarkId::new("engine_insert_autocommit", format!("{threads}T_x_{ops}ops")),
            &threads,
            |b, &t| {
                b.iter_with_setup(setup, |fixture| {
                    run_concurrent_inserts(&fixture.engine, t, ops);
                    fixture.engine.shutdown();
                });
            },
        );
    }
    group.finish();
}

/// Full-table scan throughput (coding-plan Stage K: SELECT >= 200K rows/s).
///
/// Setup pre-loads 100K rows (100 threads x 1000, untimed); the timed
/// section is a single `Engine::scan` materializing the whole table.
fn bench_m2a_scan_full_table(c: &mut Criterion) {
    const SCAN_ROWS: usize = 100_000;
    let mut group = c.benchmark_group("m2a_scan_full_table");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.throughput(Throughput::Elements(SCAN_ROWS as u64));
    group.bench_function("engine_scan_100k_rows", |b| {
        b.iter_with_setup(
            || {
                let fixture = setup();
                // Untimed load (assertions included: the load must be exact
                // or the scan measurement below is meaningless).
                run_concurrent_inserts(&fixture.engine, 100, SCAN_ROWS / 100);
                fixture
            },
            |fixture| {
                let rows = fixture.engine.scan("t", None).unwrap();
                assert_eq!(rows.len(), SCAN_ROWS);
                std::hint::black_box(rows);
                fixture.engine.shutdown();
            },
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_m2a_concurrent_insert,
    bench_m2a_scan_full_table
);
criterion_main!(benches);
