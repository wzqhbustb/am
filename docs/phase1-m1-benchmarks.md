# Phase 1 M1 Benchmarks

This document records the benchmark results for the Phase 1 M1 storage engine
(`pg-storage`). The goal is to establish a reproducible performance baseline
that future stages can compare against.

## Environment

| Item | Value |
|------|-------|
| Date | 2026-07-16 |
| OS | macOS |
| CPU | Apple Silicon (local machine) |
| Filesystem | APFS |
| Rust toolchain | rustc 1.86.0 (05f9846f8 2025-03-31) |
| Build profile | `release` (Criterion default) |
| Features | `--all-features` |

> The numbers below are measured with Criterion on a warm release build. Because
> macOS APFS `fsync` behaves differently than Linux ext4/XFS with
> `O_DIRECT`, absolute throughput is expected to be lower than the Roadmap
> Linux/SSD target. The primary value of this stage is the baseline, not the
> absolute numbers.
>
> ⚠️ **The numbers below are snapshots from a single Criterion run.** Run-to-run
> variance on macOS APFS is roughly 2× for fsync-bound workloads (WAL, page
> allocation) and up to 2.5× for in-memory workloads (buffer pool). Re-run with
> `cargo bench -- --warm-up-time 5 --measurement-time 15` to compare against a
> known machine.

## Running the Benchmarks

The first `cargo bench` run will download and compile `criterion` and its
transitive dependencies; `Cargo.lock` is updated accordingly.

```bash
cargo bench --all-features -p pg-storage
```

Individual benchmark executables can also be run directly:

```bash
cargo bench --all-features -p pg-storage --bench wal_throughput
cargo bench --all-features -p pg-storage --bench buffer_pool_random_read
cargo bench --all-features -p pg-storage --bench page_allocator
```

## WAL Sequential Write Throughput

File: `crates/pg-storage/benches/wal_throughput.rs`

Workload: append 64 × 8 KB `FullPageImage` records. In the current M1
implementation `WalWriter::append()` calls `flush_to(lsn)` before returning
(`writer.rs:184`), so every append already waits for its own fsync. The
trailing `flush()` is therefore a no-op for this single-threaded workload.

| Metric | Throughput | Roadmap Target | Status |
|--------|-----------:|---------------:|--------|
| WAL sequential write | 2.11–2.15 MiB/s | ≥ 200 MiB/s | Not met |

> Measured 2026-07-16. Earlier runs observed ~4.1 MiB/s under lower system IO
> pressure; the 2.13 MiB/s figure is the more conservative baseline.

### Why WAL Throughput Is Far Below Target

1. **Every append synchronously waits for fsync.** `WalWriter::append()` calls
   `flush_to(lsn)` before returning, so the caller blocks until the record is
   durably on disk. The explicit `flush()` at the end of the batch adds no
   extra work.
2. **Group commit does not help single-threaded sequential writes.** The
   group-commit batch size and timeout only affect when the background worker
   is woken for *concurrent* appends. With one append thread, every append
   still pays the full fsync latency.
3. **No `O_DIRECT`.** The Roadmap target assumes optional `O_DIRECT` on Linux,
   which bypasses the page cache and reduces fsync latency. M1 uses buffered
   I/O for portability.
4. **macOS APFS fsync latency.** Local measurements show ~1–5 ms per fsync,
   which caps single-threaded throughput far below the Linux/SSD target.
5. **M1 correctness requirement.** `append()` is intentionally synchronous
   because `PageAllocator` and `BufferPool` flush paths rely on the returned
   `Lsn` being durable before they proceed. The async / append-split API is the
   M2 fix.

### Planned Optimizations

- **Separate append from flush:** Provide an API where `append()` only writes
  to the in-memory buffer and returns an `Lsn`, and a separate `flush()`
  durably syncs all records up to a given LSN. This is the most direct fix for
  the single-threaded throughput gap (Stage 7b / M2).
- **Concurrent append benchmark:** Once the append/flush split exists, add a
  multi-threaded benchmark to exercise the group-commit batching path.
- **`O_DIRECT` option on Linux:** Bypass the OS page cache for WAL and data
  files (Stage 7b).
- **Dedicated WAL device / log-structured segment writer:** Reduce fsync
  frequency by pipelining segment switches (M2+).
- **Batch fsync in checkpoint:** Stage I currently flushes dirty pages one by
  one; a batch fsync can amortize syscall cost (Stage I follow-up or M2).

## Buffer Pool Random Read Throughput

File: `crates/pg-storage/benches/buffer_pool_random_read.rs`

Workload: randomly `pin` pages from a 64-page working set that fits in the
8 MB buffer pool. The working set is loaded during `setup()` so the measured
loop runs at 100% hit rate.

| Metric | Value | Roadmap Target | Status |
|--------|------:|---------------:|--------|
| Random `pin` ops/s | 1.74–2.40 Melem/s | ≥ 50K ops/s | Met |

The buffer pool easily exceeds the 50K ops/s target because the working set is
resident and the sharded page table plus `parking_lot` locks keep contention
low. The number is reported as Criterion "elements/s" over 10,000 pin
operations per iteration. The confidence interval is relatively wide on macOS
because each iteration is only a few milliseconds and is sensitive to OS
scheduling jitter; even the lower bound is ~35× above the target.

### Notes

- This benchmark measures the happy path (page already in memory). Cold-read
  throughput is dominated by the single `Mutex<File>` around the data file and
  is not the focus of the M1 target.
- Future work: switch from `Mutex<File>` + `seek`/`read` to `pread`/`pwrite`
  via `std::os::unix::fs::FileExt`, allowing concurrent disk I/O.

## Page Allocator Throughput

File: `crates/pg-storage/benches/page_allocator.rs`

Workload: allocate 100 pages per iteration. Each Criterion sample starts
from a fresh temporary directory and allocator, so the first allocation of
each sample extends the data file from an empty state.

| Metric | Value | Roadmap Target | Status |
|--------|------:|---------------:|--------|
| `alloc_page` ops/s | 308–319 elem/s | Not specified | Baseline |

Each `alloc_page` call:

1. Locks the `PageAllocator` mutex.
2. Increments `next_page_id`.
3. Extends the data file by 1 MB chunks when crossing the chunk boundary and
   fsyncs it.
4. Appends a `PageAlloc` WAL record and fsyncs the WAL.

Because each sample starts from an empty file and M1 extends the data file in
1 MB chunks (128 pages at 8 KB), roughly one allocation per sample pays the
1 MB extension + fsync cost; the remaining 99 allocations only append WAL
records. The reported throughput is therefore dominated by that cold-start
fsync, not by steady-state allocation. This is the expected M1 baseline:
correctness first, with allocation batching deferred to M2/M3.

## Summary vs. Roadmap Targets

| Component | Roadmap Target | M1 Baseline | Verdict |
|-----------|---------------:|------------:|---------|
| WAL sequential write | ≥ 200 MiB/s | 2.11–2.15 MiB/s | Below target (expected for M1 baseline) |
| Buffer Pool ops/s | ≥ 50K ops/s | 1.74–2.40 Melem/s | Exceeds target |
| Page Allocator | Not specified | 308–319 ops/s | Baseline recorded |

## Interpretation for Phase 1 M1

M1 intentionally prioritizes correctness and crash recovery over performance.
The WAL writer is fully durable, the buffer pool respects WAL-before-data, and
recovery replays `PageAlloc` and `FullPageImage` records correctly. The WAL
throughput gap is a known consequence of:

- `WalWriter::append()` synchronously flushing each record before returning,
- buffered I/O on macOS,
- a single append thread,
- lack of `O_DIRECT`, and
- the intentional M1 design choice that `append()` must return a durable `Lsn`
  for `PageAllocator` and `BufferPool` flush paths.

These are not M1 regressions; they are explicit design choices that leave head
room for Stage 7b and M2 optimizations.

## Reproducibility Checklist

- [x] `cargo fmt --all` passes.
- [x] `cargo clippy --all-features --tests --benches -p pg-storage` passes.
- [x] `cargo test --all-features -p pg-storage` passes.
- [x] `cargo bench --all-features -p pg-storage` completes and prints the
      results above.

## Updating the Baseline

To refresh these numbers and store them as a new baseline:

```bash
cargo bench --bench wal_throughput -- --save-baseline m1-2026-07
cargo bench --bench buffer_pool_random_read -- --save-baseline m1-2026-07
cargo bench --bench page_allocator -- --save-baseline m1-2026-07
```

To compare a future run against the stored baseline:

```bash
cargo bench --bench wal_throughput -- --baseline m1-2026-07
```

> CI/CD gap: ROADMAP.md mentions running benchmarks in GitHub Actions, but the
> current workflow only covers `cargo check` / `cargo test`. A nightly benchmark
> job is out of scope for Stage K and is tracked for Phase 4b.

## Next Steps

1. **Stage 7b / M2 optimizations:** Address the WAL throughput bottleneck by
   separating `append()` from `flush()`, adding `O_DIRECT`, and possibly using a
   separate WAL device.
2. **Linux baseline:** Re-run these benchmarks on a Linux workstation or CI
   runner with local SSD and `O_DIRECT` to compare against the Roadmap targets.
3. **Allocation batching:** Add `alloc_pages(n)` to amortize file extension and
   WAL fsync costs for bulk imports.
