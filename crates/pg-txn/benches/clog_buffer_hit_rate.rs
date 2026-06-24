//! ClogBuffer benchmarks (M2b Stage L; coding-plan Stage L acceptance).
//!
//! Three measurements, all against a real on-disk CLOG in a temp dir:
//!
//! 1. **Hit rate under a simulated TP load** — continuously allocate XIDs
//!    and `set_state(Committed)`, then per commit issue one `get_state`
//!    drawn 95% from the recent ~10K XIDs (hot) and 5% uniformly from the
//!    whole history so far (cold lookback, up to 1M XIDs). Acceptance
//!    targets (coding-plan Stage L / tech-selection §6.3): 8 frames ≥ 95%,
//!    256 frames ≥ 99%. The measured rates are printed to stderr (criterion
//!    measures time, not ratios), prefixed with `[hit-rate-report]`.
//! 2. **Hot-path latency** — `get_state` on a resident page (target
//!    < 500 ns).
//! 3. **Miss latency** — `get_state` cycling over far more pages than there
//!    are frames, so every access misses and preads (target < 20 µs).
//!
//! The workload uses a local xorshift PRNG so no extra dependencies are
//! needed and runs are deterministic.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pg_storage::types::TxnId;
use pg_txn::{ClogAccessor, ClogBuffer, TxnState};

/// xorshift64* — deterministic, dependency-free PRNG for the workload mix.
struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform value in `0..n` (n > 0).
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// True with probability `pct`%.
    fn coin(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

/// Simulated TP load: commit `total_xids` transactions in XID order; after
/// each commit, read one XID — 95% from the recent ~10K window, 5% from the
/// entire history so far (cold lookback). Returns (hit_rate, hits, misses).
fn run_tp_workload(frames: usize, total_xids: u64) -> (f64, u64, u64) {
    let tmp = tempfile::TempDir::new().unwrap();
    let clog = ClogBuffer::open(tmp.path(), frames).unwrap();
    let mut rng = XorShift64(0x9E37_79B9_7F4A_7C15);
    for xid in 1..=total_xids {
        clog.set_state(TxnId(xid), TxnState::Committed);
        let read_xid = if rng.coin(95) {
            // Hot: uniform in the recent ~10K XIDs.
            let lo = xid.saturating_sub(10_000).max(1);
            lo + rng.below(xid - lo + 1)
        } else {
            // Cold: uniform in the whole history so far (grows to 1M XIDs).
            1 + rng.below(xid)
        };
        black_box(clog.get_state(TxnId(read_xid)));
    }
    (clog.hit_rate(), clog.hits(), clog.misses())
}

/// Hit-rate measurement over the full 1M-XID history (untimed setup; the
/// ratios are reported on stderr), plus a criterion-timed 100K-XID sample of
/// the same workload per configuration.
fn bench_hit_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("clog_hit_rate");
    for frames in [8usize, 256] {
        let (rate, hits, misses) = run_tp_workload(frames, 1_000_000);
        let target = if frames == 8 { 95.0 } else { 99.0 };
        eprintln!(
            "[hit-rate-report] frames={frames:>3}  hit_rate={:.2}%  (hits={hits}, misses={misses})  target>={target:.0}%",
            rate * 100.0
        );
        group.bench_function(format!("tp_workload_{frames}_frames"), |b| {
            b.iter(|| run_tp_workload(frames, 100_000))
        });
    }
    group.finish();
}

/// Single-access latency: hot hit vs. guaranteed miss (page cycle far
/// larger than the frame count, so the sweep evicts every page before it is
/// re-referenced).
fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("clog_latency");

    // Hit: page 0 stays resident and referenced.
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let clog = ClogBuffer::open(tmp.path(), 8).unwrap();
        clog.set_state(TxnId(1), TxnState::Committed);
        group.bench_function("get_state_hit", |b| {
            b.iter(|| black_box(clog.get_state(TxnId(1))))
        });
    }

    // Miss: cycle through 64 distinct pages with only 8 frames.
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let clog = ClogBuffer::open(tmp.path(), 8).unwrap();
        let xids: Vec<u64> = (0..64u64).map(|p| p * 16_384 + 1).collect();
        let mut i = 0usize;
        group.bench_function("get_state_miss", |b| {
            b.iter(|| {
                i = (i + 1) % xids.len();
                black_box(clog.get_state(TxnId(xids[i])))
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_hit_rate, bench_latency);
criterion_main!(benches);
