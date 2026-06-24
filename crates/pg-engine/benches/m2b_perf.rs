//! M2b Stage O performance benches (criterion; numbers are reported, nothing
//! is gated):
//!
//! - single-txn INSERT + COMMIT latency through the auto-commit path
//!   (target <= 5 ms — fsync-bound on macOS F_FULLFSYNC ~4 ms);
//! - index point lookup QPS over a >= 100K-row indexed table, random keys
//!   (target >= 100K QPS — warm buffer pool, no fsync on the read path).
//!
//! Run in release mode for meaningful numbers:
//!
//! ```sh
//! cargo bench -p pg-engine --bench m2b_perf
//! ```
//!
//! `M2B_BENCH_ROWS` overrides the lookup table size (default 100_000) and
//! `M2B_BENCH_LOAD_THREADS` the concurrent loader (default 100) for quicker
//! smoke runs.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use pg_engine::{ColumnDef, ColumnType, Datum, Engine, EngineConfig};

fn bench_rows() -> i32 {
    std::env::var("M2B_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000)
}

fn load_threads() -> i32 {
    std::env::var("M2B_BENCH_LOAD_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
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
            &[ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::Int4,
            }],
        )
        .unwrap();
    Fixture { _tmp: tmp, engine }
}

/// Load `rows` rows through `threads` concurrent auto-commit inserters
/// (group commit amortizes the fsync, same as the M2a loader).
fn load_rows(engine: &Arc<Engine>, threads: i32, rows: i32) {
    let per_thread = rows / threads;
    std::thread::scope(|s| {
        for t in 0..threads {
            let engine = Arc::clone(engine);
            s.spawn(move || {
                for i in 0..per_thread {
                    engine
                        .insert("t", &[Some(Datum::Int4(t * per_thread + i))])
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(engine.scan("t", None).unwrap().len(), rows as usize);
}

/// Single-txn INSERT + COMMIT latency (target <= 5 ms).
fn bench_txn_insert_commit(c: &mut Criterion) {
    let fixture = setup();
    let mut group = c.benchmark_group("m2b_txn_insert_commit");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);
    let mut seq = 0i32;
    group.bench_function("insert_commit_latency", |b| {
        b.iter(|| {
            seq += 1;
            fixture
                .engine
                .insert("t", &[Some(Datum::Int4(seq))])
                .unwrap();
        });
    });
    group.finish();
    fixture.engine.shutdown();
}

/// Index point lookup QPS over an indexed table of >= 100K rows, random
/// keys (target >= 100K QPS). The load and the blocking index build are
/// untimed setup; each timed iteration performs `BATCH` lookups and reports
/// lookups/sec as the throughput.
fn bench_index_point_lookup(c: &mut Criterion) {
    const BATCH: u64 = 1_000;
    let rows = bench_rows();
    let fixture = setup();
    load_rows(&fixture.engine, load_threads(), rows);
    fixture.engine.create_index("t", "id").unwrap();

    let mut group = c.benchmark_group("m2b_index_point_lookup");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);
    group.throughput(Throughput::Elements(BATCH));
    // xorshift64* — deterministic key stream, all keys in [0, rows).
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next_key = move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        (state.wrapping_mul(0x2545_F491_4F6C_DD1D) % rows as u64) as i32
    };
    group.bench_function("random_key_lookup_qps", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                let key = next_key();
                let tid = fixture
                    .engine
                    .index_lookup("t", "id", &Datum::Int4(key))
                    .unwrap();
                assert!(tid.is_some(), "key {key} must resolve");
                std::hint::black_box(tid);
            }
        });
    });
    group.finish();
    fixture.engine.shutdown();
}

criterion_group!(benches, bench_txn_insert_commit, bench_index_point_lookup);
criterion_main!(benches);
