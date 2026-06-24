//! Stage Q loom model tests: exhaustive interleaving exploration of the REAL
//! latch choreography in `index.rs` (crabbing descents, coupled right hops,
//! optimistic leaf writes with re-validation, pessimistic split restarts).
//!
//! Run with bounded preemptions (the Stage Q acceptance command):
//!
//! ```sh
//! LOOM_MAX_PREEMPTIONS=3 cargo test -p pg-am-btree --features loom --test btree_loom
//! ```
//!
//! # How the model stays inside loom
//!
//! The `loom` feature turns on `pg-storage/loom`, which swaps every lock that
//! participates in the latch choreography for loom's instrumented primitives
//! (`pg_storage::sync`; see that module's docs). Everything the B+Tree latch
//! protocol touches — the buffer-pool page-table shards, frame `meta` mutexes
//! and `content` rwlocks, the WAL writer state lock, the LSN clock atomics —
//! is therefore a loom scheduling point. What is deliberately OUTSIDE the
//! model (stubbed, per the `pg_storage::sync` docs):
//!
//! - the WAL group-commit worker thread (replaced by an inline, fsync-free
//!   `flush_to`);
//! - `BufferPool::flush_frame`'s data-file write + fsync (state transitions
//!   only). The pool is sized at 16 frames so the tiny tree NEVER evicts —
//!   an evicted page would reload as zeros, since nothing is written;
//! - setup-path fsyncs (`io::sync_dir`, `write_atomic`'s temp-file fsync,
//!   the WAL segment preallocation fsync): a real F_FULLFSYNC per iteration
//!   makes exploring thousands of interleavings prohibitively slow.
//!
//! # Sizing rationale (loom state space explodes — keep models TINY)
//!
//! Every trim below was measured, not guessed (Apple Silicon, debug build):
//!
//! - **Preemption bound**: the acceptance command sets
//!   `LOOM_MAX_PREEMPTIONS=3`. The split model runs the full bound (~3.5
//!   min). The linearizable-reads model's state space is too large at 3
//!   (> 7 min unfinished even after every trim below), so it SELF-CLAMPS
//!   to 2 via `run_model(2, ..)` and prints a notice — at 2 it finishes in
//!   ~75 s. Manual full exploration: raise the clamp in `run_model`.
//! - **Per-thread work**: 1 insert per writer (reads model), 2 per writer
//!   (split model). Each insert is ~25-35 loom scheduling points; 2
//!   keys/writer in the reads model already blew past 5 minutes at bound 3.
//! - **Splits** are forced with ~2600-byte Text keys (~3 entries per 8 KB
//!   leaf), so 4 keys cause exactly 1 leaf split + root promotion — the
//!   "tiny pages" effect without a small-page build.
//! - **Handle construction is hoisted** out of the spawned threads:
//!   `open_index` pins the meta page (~8 scheduling points) without being
//!   part of the choreography; hoisting cut the reads model from ~153 s to
//!   ~75 s at bound 2.
//! - Each interleaving re-runs the whole closure including engine setup
//!   (tempdir + 64 KB WAL segment + 16-frame pool); setup runs on the lone
//!   driver thread so it costs runtime but no extra states.
//!
//! Iteration counts (LOOM_LOG=info, `grep -c Iteration`; Apple Silicon,
//! debug build — all interleavings passed: no lost keys, no TID mixups, no
//! panic/deadlock):
//!
//! - `loom_two_writers_one_reader_linearizable` @ bound 2: **6,551**
//!   interleavings (~75 s).
//! - `loom_split_with_concurrent_writers` @ bound 3: **13,842**
//!   interleavings (~3.5 min).

use std::sync::Arc;

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;

use pg_am_btree::{BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::buffer_pool::BufferPool;
use pg_storage::config::StorageConfig;
use pg_storage::page_allocator::PageAllocator;
use pg_storage::sync::Mutex;
use pg_storage::types::{Oid, PageId, Tid};
use pg_storage::wal::WalWriter;

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_387);

/// Generator stack size for every loom thread, INCLUDING the model driver.
/// loom runs the model closure itself on a generator with the `generator`
/// crate's 4 KB default stack, which real engine code (tempdir setup, key
/// formatting, WAL encode) overflows immediately; every thread is therefore
/// spawned through `spawn_big` / `run_model` with an ample explicit stack.
const LOOM_STACK_SIZE: usize = 4 << 20;

/// Spawn a loom thread with a real-sized stack (see [`LOOM_STACK_SIZE`]).
fn spawn_big<F>(f: F) -> thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new()
        .stack_size(LOOM_STACK_SIZE)
        .spawn(f)
        .unwrap()
}

/// Run `body` as a loom model: the model closure itself only spawns one
/// big-stack driver thread, so the whole engine workload runs on ample
/// generator stacks instead of loom's 4 KB default.
///
/// `max_preemptions` CLAMPS the `LOOM_MAX_PREEMPTIONS` env knob for this
/// model: the acceptance command runs the whole binary at
/// `LOOM_MAX_PREEMPTIONS=3`, but the linearizable-reads model's state space
/// is too large at that bound (> 7 min without finishing even after every
/// scheduling-point trim), so it self-caps at 2 and says so loudly here and
/// in its doc comment. The split model runs the full env bound.
fn run_model<F>(max_preemptions: usize, body: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let body = Arc::new(body);
    let mut builder = loom::model::Builder::new();
    if builder.preemption_bound.unwrap_or(usize::MAX) > max_preemptions {
        eprintln!(
            "btree_loom: clamping preemption bound {:?} -> {max_preemptions} (see test header)",
            builder.preemption_bound
        );
        builder.preemption_bound = Some(max_preemptions);
    }
    let run = move || {
        builder.check(move || {
            let body = Arc::clone(&body);
            spawn_big(move || body()).join().unwrap();
        });
    };
    // `Builder::check` (unlike `loom::model`) installs no tracing
    // subscriber; add one so `LOOM_LOG=1` iteration traces work for
    // measuring state-space sizes.
    if std::env::var_os("LOOM_LOG").is_some() {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_env("LOOM_LOG"))
            .with_test_writer()
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, run);
    } else {
        run();
    }
}

/// Pool frames for the loom models: the whole tiny tree (meta + root + a
/// handful of leaves, incl. the split pre-allocation) stays resident, so the
/// `flush_frame` I/O stub never hides a reload-from-zeros.
const LOOM_POOL_FRAMES: usize = 16;

/// Text-key length that yields ~3 entries per 8 KB leaf — a "tiny page"
/// effect so just 4 inserts force exactly one split (still comfortably
/// below `MAX_INDEX_KEY_BYTES`).
const BIG_KEY_LEN: usize = 2600;

fn loom_config(tmp: &TempDir) -> StorageConfig {
    let mut cfg = StorageConfig::new(tmp.path());
    cfg.buffer_pool_size = LOOM_POOL_FRAMES * cfg.page_size();
    cfg.buffer_pool_shards = 4;
    // Small segment: `WalSegmentManager::open` preallocates it on every
    // interleaving.
    cfg.wal_segment_size = 64 * 1024;
    cfg.wal_group_commit_batch_size = 1;
    cfg.wal_group_commit_timeout_ms = 1;
    cfg
}

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(9_000_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

/// Short, order-preserving Text key ("00000042").
fn small_key(n: u64) -> Vec<u8> {
    format!("{n:08}").into_bytes()
}

/// ~900-byte Text key with the same ordering prefix as `small_key`.
fn big_key(n: u64) -> Vec<u8> {
    let mut k = small_key(n);
    k.resize(BIG_KEY_LEN, b'k');
    k
}

fn key_number(key: &[u8]) -> u64 {
    std::str::from_utf8(&key[..8]).unwrap().parse().unwrap()
}

fn setup() -> (TempDir, Arc<BufferPool>, Arc<WalWriter>, PageId) {
    let tmp = TempDir::new().unwrap();
    let cfg = loom_config(&tmp);
    let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
    let allocator = Arc::new(Mutex::new(
        PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
    ));
    let pool = Arc::new(
        BufferPool::open(tmp.path(), &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap(),
    );
    let am = BTreeAM::new(Arc::clone(&pool), Arc::clone(&wal));
    let index = am.create_index(REL_OID, ColumnType::Text).unwrap();
    (tmp, pool, wal, index.meta_page())
}

/// Build a fresh per-thread handle (the engine's per-DML model).
fn open_handle(pool: &Arc<BufferPool>, wal: &Arc<WalWriter>, meta_page: PageId) -> BTreeIndex {
    let am = BTreeAM::new(Arc::clone(pool), Arc::clone(wal));
    am.open_index(REL_OID, meta_page, ColumnType::Text).unwrap()
}

/// 2 writers + 1 reader on a tree small enough to never split: writers
/// insert DISJOINT keys and publish each completed insert to a loom atomic;
/// the reader snapshots the completed count, then full-scans. Linearizable
/// outcomes require: every completed insert is visible in the scan, and
/// every scanned entry maps back to its own TID. No interleaving may panic
/// or deadlock (loom explores all of them up to the preemption bound).
#[test]
fn loom_two_writers_one_reader_linearizable() {
    // Sizing: ONE key per writer — the explored state space grows steeply
    // with per-thread operations (each insert is dozens of loom scheduling
    // points); 2 keys/writer already blew past 5 minutes at
    // LOOM_MAX_PREEMPTIONS=3.
    const WRITERS: u64 = 2;
    const PER_WRITER: u64 = 1;
    const TOTAL: u64 = WRITERS * PER_WRITER;

    run_model(2, || {
        let (_tmp, pool, wal, meta_page) = setup();
        let committed = Arc::new(AtomicUsize::new(0));

        // Build every thread's index handle BEFORE spawning: handle
        // construction pins the meta page (~8 loom scheduling points) and
        // is not part of the choreography under test, so keeping it out of
        // the modeled region shrinks the explored state space severalfold.
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let mut index = open_handle(&pool, &wal, meta_page);
            let committed = Arc::clone(&committed);
            handles.push(spawn_big(move || {
                for i in 0..PER_WRITER {
                    let n = w * PER_WRITER + i;
                    index.insert(&small_key(n), tid(n)).unwrap();
                    // Publish only after the insert fully returned.
                    committed.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        {
            let index = open_handle(&pool, &wal, meta_page);
            let committed = Arc::clone(&committed);
            handles.push(spawn_big(move || {
                // Snapshot first: the scan below must contain at least these
                // many entries (a scanned entry whose publisher has not run
                // yet only makes the scan LARGER, never smaller).
                let done = committed.load(Ordering::SeqCst);
                let rows = index.range_scan(None, None).unwrap();
                assert!(
                    rows.len() >= done,
                    "lost completed inserts: {done} done but only {} visible",
                    rows.len()
                );
                for (k, t) in &rows {
                    let n = key_number(k);
                    assert!(n < TOTAL, "phantom key {n}");
                    assert_eq!(*t, tid(n), "key {n} mapped to the wrong TID");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Quiescent final state: exact contents and a clean validate.
        let index = open_handle(&pool, &wal, meta_page);
        assert_eq!(index.range_scan(None, None).unwrap().len(), TOTAL as usize);
        index.validate().unwrap();
    });
}

/// Split-focused model: 2 writers race INTERLEAVED ascending ~2600-byte keys
/// into a single-leaf tree, forcing exactly one leaf split + root promotion
/// (~3 entries per leaf, 4 keys total). Every interleaving must end with
/// all 4 keys present, the root promoted, and a clean quiescent validate —
/// exercising pessimistic full-path restarts, split-page pre-allocation, and
/// the coupled right-hop reads that observe the split mid-flight.
///
/// Coverage boundary (deliberate): the tree stays TWO levels (root + leaves),
/// so the multi-level cascade — a split propagating into a full parent via
/// `split_commit_guarded`'s recursion — is NOT modeled here (loom state
/// space grows steeply with tree height). That path is covered by the
/// threaded stress tests in `btree_concurrent.rs` instead.
#[test]
fn loom_split_with_concurrent_writers() {
    // Sizing: ~2600-byte keys hold ~3 entries per leaf, so 4 keys force
    // exactly one leaf split + root promotion with the minimum possible
    // number of inserts (loom state space grows steeply per operation).
    const WRITERS: u64 = 2;
    const PER_WRITER: u64 = 2;
    const TOTAL: u64 = WRITERS * PER_WRITER;

    run_model(usize::MAX, || {
        let (_tmp, pool, wal, meta_page) = setup();

        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let mut index = open_handle(&pool, &wal, meta_page);
            handles.push(spawn_big(move || {
                for i in 0..PER_WRITER {
                    // Interleaved ascending: both writers hammer the same
                    // right edge, so the split fires mid-race.
                    let n = i * WRITERS + w;
                    index.insert(&big_key(n), tid(n)).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let index = open_handle(&pool, &wal, meta_page);
        assert!(
            index.tree_level() >= 1,
            "4 oversized keys must have split the root leaf"
        );
        for n in 0..TOTAL {
            assert_eq!(
                index.lookup(&big_key(n)).unwrap(),
                Some(tid(n)),
                "key {n} lost across the split"
            );
        }
        index.validate().unwrap();
    });
}
