//! M2b Stage O crash automation: repeated random kill -9 + reopen cycles
//! driven by the SQL `exec` API.
//!
//! Harness shape follows `m2a_crash_rounds.rs`: the parent spawns the test
//! binary itself as a child (`M2B_CRASH_CHILD=1`), the child runs a
//! deterministic pseudo-random DDL/DML workload (seeded by the round number)
//! through `Engine::exec` and is then SIGKILLed; the parent reopens the data
//! directory and validates consistency against an atomic expectation file.
//!
//! # What is verified each round
//!
//! Every child op is auto-committed (each commit fsyncs), so the committed
//! state is always a prefix of the op stream. After every op the child
//! rewrites `{data_dir}/expectation.txt` atomically (tmp + rename) with the
//! full committed state read back through `SELECT`. The parent compares the
//! reopened engine against the last surviving expectation:
//!
//! - every expected table exists with exactly the expected visible rows
//!   (row count AND content);
//! - for mid-workload kills (odd rounds): prefix-durability — every expected
//!   row present with exact content, at most one extra row from the single
//!   in-flight op.
//! - the B+Tree on `ixt(name)` validates and every committed `ixt` row is
//!   reachable through an index lookup, so the index agrees with the heap.
//!
//! # Stage S coverage
//!
//! The workload deliberately keeps a B+Tree split in flight when the kill
//! lands: `ixt(name)` carries ~500-byte keys, so only ~15 entries fit in a
//! leaf and the ~60-80 indexed inserts a round performs drive several leaf
//! splits plus a root split. Recovery must therefore run the undo pass and
//! finish those splits through a CLR. The same stream also carries HOT updates
//! (only the unindexed `id` changes), non-HOT updates (the indexed `name`
//! changes, forcing index maintenance) and `FOR SHARE` shared-lock stamping.
//!
//! # Kill timing
//!
//! Even rounds wait for the child's `ready-to-die` marker (full workload
//! committed, then killed). Odd rounds wait only for the child's
//! `engine-ready` marker and are then killed as soon as the expectation file
//! shows a seed-derived number of committed ops — a true mid-workload crash at
//! a PROGRESS point. Kills are never delivered before `engine-ready`.
//!
//! # Rounds
//!
//! `M2B_CRASH_ROUNDS` controls the round count. The default of 25 is the CI
//! configuration; the Stage O acceptance configuration is 1000 rounds, run
//! manually (~30-60 min):
//!
//! ```sh
//! M2B_CRASH_ROUNDS=1000 cargo test -p pg-engine --test m2b_crash_rounds -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};

const CHILD_ENV_VAR: &str = "M2B_CRASH_CHILD";
const DIR_ENV_VAR: &str = "M2B_CRASH_DIR";
const SEED_ENV_VAR: &str = "M2B_CRASH_SEED";
const ROUNDS_ENV_VAR: &str = "M2B_CRASH_ROUNDS";
const CHILD_TEST_NAME: &str = "m2b_crash_child_entry";

const READY_MARKER: &str = "ready-to-die";
const ENGINE_READY_MARKER: &str = "engine-ready";
const EXPECTATION_FILE: &str = "expectation.txt";
const EXPECTATION_TMP: &str = "expectation.tmp";

const BASE_TABLES: [&str; 3] = ["rt0", "rt1", "rt2"];

/// Table carrying a B+Tree index on its `name` column. Its keys are padded
/// wide on purpose: at ~500 bytes only ~15 entries fit in a leaf page, so the
/// few dozen inserts a round performs drive several leaf splits and a root
/// split. That is what puts a split in flight when the kill lands.
const IX_TABLE: &str = "ixt";
const IX_KEY_PAD: usize = 500;

/// A unique, space-free, order-preserving index key of `IX_KEY_PAD` bytes.
fn ix_key(seq: i32) -> String {
    format!("k{seq:08}{}", "x".repeat(IX_KEY_PAD))
}

/// xorshift64* — deterministic PRNG so each round's op stream is reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// Child process
// ---------------------------------------------------------------------------

/// Child entry point: runs the seeded workload, then sleeps until killed.
#[test]
fn m2b_crash_child_entry() {
    if std::env::var(CHILD_ENV_VAR).is_err() {
        return;
    }
    let data_dir = std::env::var(DIR_ENV_VAR).expect("data dir required");
    let seed: u64 = std::env::var(SEED_ENV_VAR)
        .expect("seed required")
        .parse()
        .expect("seed is u64");
    run_child(Path::new(&data_dir), seed);

    fs::write(Path::new(&data_dir).join(READY_MARKER), b"").unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

/// In-memory model of live row ids per table — the authoritative state is
/// re-derived from SELECT scans when the expectation file is written.
struct ChildModel {
    tables: BTreeMap<String, Vec<i32>>,
    extra_seq: u64,
    row_seq: i32,
}

fn run_child(data_dir: &Path, seed: u64) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir)).unwrap();
    fs::write(data_dir.join(ENGINE_READY_MARKER), b"").unwrap();

    for table in BASE_TABLES {
        engine
            .exec(None, &format!("CREATE TABLE {table} (id INT, name TEXT)"))
            .unwrap();
    }
    engine
        .exec(None, &format!("CREATE TABLE {IX_TABLE} (id INT, name TEXT)"))
        .unwrap();
    engine
        .exec(None, &format!("CREATE INDEX ON {IX_TABLE} (name)"))
        .unwrap();

    let destructive = seed % 2 == 0;
    let mut rng = Rng(seed | 0x9E37_79B9_7F4A_7C15);
    let mut model = ChildModel {
        tables: BASE_TABLES
            .iter()
            .map(|t| (t.to_string(), Vec::new()))
            .chain(std::iter::once((IX_TABLE.to_string(), Vec::new())))
            .collect(),
        extra_seq: 0,
        row_seq: 0,
    };

    // Enough ops that the indexed table reaches several leaf splits and a
    // root split before the workload ends.
    let op_count = 160 + rng.below(80);
    for completed in 1..=op_count {
        do_random_op(&engine, &mut rng, &mut model, destructive);
        write_expectation(&engine, data_dir, &model, destructive, completed as usize);
    }
}

/// Pick a heap-only table. The indexed table is excluded so the split pressure
/// on its B+Tree comes only from the dedicated indexed ops below.
fn pick_table<'a>(rng: &mut Rng, model: &'a ChildModel) -> &'a str {
    let names: Vec<&str> = model
        .tables
        .keys()
        .map(String::as_str)
        .filter(|t| *t != IX_TABLE)
        .collect();
    let idx = rng.below(names.len() as u64) as usize;
    names[idx]
}

fn insert_base_row(engine: &Engine, rng: &mut Rng, model: &mut ChildModel) {
    let table = pick_table(rng, model).to_string();
    let id = model.row_seq;
    model.row_seq += 1;
    engine
        .exec(None, &format!("INSERT INTO {table} VALUES ({id}, 'row-{id}')"))
        .unwrap();
    model.tables.get_mut(&table).unwrap().push(id);
}

/// Every arm adds at most one row, which is what lets the parent's mid-workload
/// verifier bound the in-flight op at a single extra row.
fn do_random_op(engine: &Engine, rng: &mut Rng, model: &mut ChildModel, destructive: bool) {
    match rng.below(20) {
        // Heap INSERT (25%).
        0..=4 => insert_base_row(engine, rng, model),
        // Indexed INSERT (35%): the wide key means ~15 entries per leaf, so
        // this is the op that drives leaf and root splits.
        5..=11 => {
            let id = model.row_seq;
            model.row_seq += 1;
            let key = ix_key(id);
            engine
                .exec(
                    None,
                    &format!("INSERT INTO {IX_TABLE} VALUES ({id}, '{key}')"),
                )
                .unwrap();
            model.tables.get_mut(IX_TABLE).unwrap().push(id);
        }
        // Heap UPDATE (10%): replace a random live row's id and name.
        12..=13 if destructive => {
            let table = pick_table(rng, model).to_string();
            let ids = model.tables.get_mut(&table).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let old_id = ids[idx];
            let new_id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(
                    None,
                    &format!(
                        "UPDATE {table} SET id = {new_id}, name = 'upd-{new_id}' WHERE id = {old_id}"
                    ),
                )
                .unwrap();
            ids[idx] = new_id;
        }
        // Heap DELETE (5%): remove a random live row.
        14 if destructive => {
            let table = pick_table(rng, model).to_string();
            let ids = model.tables.get_mut(&table).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids.swap_remove(idx);
            engine
                .exec(None, &format!("DELETE FROM {table} WHERE id = {id}"))
                .unwrap();
        }
        // HOT UPDATE (5%): only the unindexed `id` changes, so the new version
        // stays on the page and no index entry is added.
        15 if destructive => {
            let ids = model.tables.get_mut(IX_TABLE).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let old_id = ids[idx];
            let new_id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(
                    None,
                    &format!("UPDATE {IX_TABLE} SET id = {new_id} WHERE id = {old_id}"),
                )
                .unwrap();
            ids[idx] = new_id;
        }
        // Non-HOT UPDATE (5%): the indexed `name` changes, forcing an index
        // insert (and possibly a split) alongside the heap update.
        16 if destructive => {
            let ids = model.tables.get(IX_TABLE).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids[idx];
            let key_seq = model.row_seq;
            model.row_seq += 1;
            let key = ix_key(key_seq);
            engine
                .exec(
                    None,
                    &format!("UPDATE {IX_TABLE} SET name = '{key}' WHERE id = {id}"),
                )
                .unwrap();
        }
        // FOR SHARE (5%): stamps a shared lock in xmax, safe in both modes
        // because it changes no visible column.
        17 => {
            let ids = model.tables.get(IX_TABLE).unwrap();
            if ids.is_empty() {
                return;
            }
            let idx = rng.below(ids.len() as u64) as usize;
            let id = ids[idx];
            engine
                .exec(
                    None,
                    &format!("SELECT id FROM {IX_TABLE} WHERE id = {id} FOR SHARE"),
                )
                .unwrap();
        }
        // CHECKPOINT (5%).
        18 => engine.checkpoint().unwrap(),
        // CREATE extra table (5%).
        19 => {
            let name = format!("extra{}", model.extra_seq);
            model.extra_seq += 1;
            engine
                .exec(None, &format!("CREATE TABLE {name} (id INT, name TEXT)"))
                .unwrap();
            model.tables.insert(name, Vec::new());
        }
        // Non-destructive substitute for the UPDATE/DELETE slots: INSERT.
        _ => insert_base_row(engine, rng, model),
    }
}

/// Rewrite the expectation file atomically: live tables with their visible
/// rows (from authoritative SELECT scans), and the op-stream mode the
/// parent's verifier must apply.
fn write_expectation(
    engine: &Engine,
    data_dir: &Path,
    model: &ChildModel,
    destructive: bool,
    completed_ops: usize,
) {
    let mut out = String::new();
    out.push_str(if destructive {
        "MODE full\n"
    } else {
        "MODE mid\n"
    });
    out.push_str(&format!("OPS {completed_ops}\n"));
    for table in model.tables.keys() {
        out.push_str(&format!("TABLE {table}\n"));
        let res = engine
            .exec(None, &format!("SELECT * FROM {table}"))
            .unwrap();
        let mut rows: Vec<(i32, String)> = match res {
            QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|row| match (&row[0], &row[1]) {
                    (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                    other => panic!("unexpected row shape: {other:?}"),
                })
                .collect(),
            other => panic!("expected Rows, got {other:?}"),
        };
        rows.sort_unstable();
        for (id, name) in rows {
            out.push_str(&format!("ROW {table} {id} {name}\n"));
        }
    }

    let tmp = data_dir.join(EXPECTATION_TMP);
    fs::write(&tmp, &out).unwrap();
    fs::File::open(&tmp).unwrap().sync_all().unwrap();
    fs::rename(&tmp, data_dir.join(EXPECTATION_FILE)).unwrap();
}

// ---------------------------------------------------------------------------
// Parent harness
// ---------------------------------------------------------------------------

struct Expectation {
    tables: BTreeMap<String, Vec<(i32, String)>>,
    mid: bool,
    ops: usize,
}

fn parse_expectation(text: &str) -> Expectation {
    let mut tables: BTreeMap<String, Vec<(i32, String)>> = BTreeMap::new();
    let mut mid = None;
    let mut ops = 0;
    for line in text.lines() {
        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        match parts.as_slice() {
            ["MODE", mode] => {
                mid = Some(match *mode {
                    "full" => false,
                    "mid" => true,
                    other => panic!("unknown expectation mode {other}"),
                });
            }
            ["OPS", n] => {
                ops = n.parse().expect("OPS line must carry a number");
            }
            ["TABLE", name] => {
                tables.entry(name.to_string()).or_default();
            }
            ["ROW", table, id, name] => {
                tables
                    .entry(table.to_string())
                    .or_default()
                    .push((id.parse().unwrap(), name.to_string()));
            }
            other => panic!("malformed expectation line: {other:?}"),
        }
    }
    Expectation {
        tables,
        mid: mid.expect("expectation must start with a MODE line"),
        ops,
    }
}

fn spawn_child(data_dir: &Path, seed: u64) -> std::process::Child {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary path"));
    cmd.arg("--test-threads=1")
        .arg(CHILD_TEST_NAME)
        .env(CHILD_ENV_VAR, "1")
        .env(DIR_ENV_VAR, data_dir.as_os_str())
        .env(SEED_ENV_VAR, seed.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("failed to spawn crash child")
}

fn wait_for_marker(data_dir: &Path, timeout: Duration) -> bool {
    let marker = data_dir.join(READY_MARKER);
    let start = Instant::now();
    while start.elapsed() < timeout {
        if marker.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(2));
    }
    false
}

/// Read all visible rows of `table` via `SELECT * FROM {table}` as sorted
/// (id, name) pairs.
fn scan_table(engine: &Engine, table: &str) -> Vec<(i32, String)> {
    let res = engine
        .exec(None, &format!("SELECT * FROM {table}"))
        .unwrap_or_else(|e| panic!("scan of {table} failed: {e}"));
    let mut rows: Vec<(i32, String)> = match res {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| match (&row[0], &row[1]) {
                (Some(Datum::Int4(id)), Some(Datum::Text(name))) => (*id, name.clone()),
                other => panic!("unexpected row shape: {other:?}"),
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
    };
    rows.sort_unstable();
    rows
}

/// The M2b crash-automation acceptance: `M2B_CRASH_ROUNDS` random kill -9 +
/// reopen cycles (default 25; 1000 for the plan's literal acceptance).
#[test]
fn m2b_crash_rounds() {
    if std::env::var(CHILD_ENV_VAR).is_ok() {
        return; // we are the child; the entry test does the work
    }
    let rounds: u64 = std::env::var(ROUNDS_ENV_VAR)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25);

    for round in 0..rounds {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let mut child = spawn_child(&data_dir, round);

        if round % 2 == 0 {
            // Even rounds: let the full workload commit, then kill.
            assert!(
                wait_for_marker(&data_dir, Duration::from_secs(120)),
                "round {round}: child did not finish its workload in time"
            );
            thread::sleep(Duration::from_millis(round % 20));
        } else {
            // Odd rounds: kill at a seed-derived PROGRESS point — as soon as
            // the expectation file shows `target_ops` committed ops.
            let engine_ready = data_dir.join(ENGINE_READY_MARKER);
            let start = Instant::now();
            while !engine_ready.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "round {round}: child engine did not open in time"
                );
                thread::sleep(Duration::from_millis(2));
            }
            // Deep enough that the indexed table has already split at least
            // once, so the kill can land inside a split protocol.
            let target_ops = 40 + (round % 60) as usize;
            let start = Instant::now();
            loop {
                let reached = data_dir.join(EXPECTATION_FILE).exists()
                    && parse_expectation(
                        &fs::read_to_string(data_dir.join(EXPECTATION_FILE)).unwrap(),
                    )
                    .ops >= target_ops;
                if reached {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(120),
                    "round {round}: child did not reach op {target_ops} in time"
                );
                thread::sleep(Duration::from_millis(2));
            }
        }
        child.kill().expect("failed to kill crash child");
        child.wait().expect("failed to reap crash child");

        verify_round(round, &data_dir);
    }
}

fn verify_round(round: u64, data_dir: &Path) {
    let engine = Engine::open(data_dir, EngineConfig::new(data_dir))
        .unwrap_or_else(|e| panic!("round {round}: engine failed to reopen: {e}"));

    let expectation_path = data_dir.join(EXPECTATION_FILE);
    assert!(
        expectation_path.exists(),
        "round {round}: expectation file missing — the child never committed its first op (silent pass is worse than a loud failure)"
    );
    let expectation = parse_expectation(&fs::read_to_string(&expectation_path).unwrap());

    if expectation.mid {
        // Prefix-durability: every expected row present with exact content;
        // at most one extra row overall (the single in-flight op).
        let mut extras = 0usize;
        for (table, expected_rows) in &expectation.tables {
            let mut rows = scan_table(&engine, table);
            for expected in expected_rows {
                let pos = rows.iter().position(|r| r == expected).unwrap_or_else(|| {
                    panic!(
                        "round {round}: committed row {expected:?} of {table} lost across crash recovery"
                    )
                });
                rows.swap_remove(pos);
            }
            extras += rows.len();
        }
        assert!(
            extras <= 1,
            "round {round}: recovered state is more than one op ahead of the expectation ({extras} extra rows)"
        );
    } else {
        // Full-workload: exact match.
        for (table, expected_rows) in &expectation.tables {
            let rows = scan_table(&engine, table);
            assert_eq!(
                &rows, expected_rows,
                "round {round}: table {table} content diverged across crash recovery"
            );
        }
    }
    verify_index(round, &engine, &expectation);
    engine.shutdown();
}

/// The index must agree with the heap after recovery. `validate` walks the
/// whole tree and cross-checks the leaf chain against the root-reachable
/// leaves, which is exactly how a split left unfinished by undo shows up: its
/// right sibling is in the chain but has no downlink.
fn verify_index(round: u64, engine: &Engine, expectation: &Expectation) {
    let index = engine
        .btree_index(IX_TABLE, "name")
        .unwrap_or_else(|e| panic!("round {round}: {IX_TABLE} index failed to open: {e}"));
    index.validate().unwrap_or_else(|e| {
        panic!("round {round}: {IX_TABLE} index is corrupt after crash recovery: {e}")
    });

    let ix_rows = expectation.tables.get(IX_TABLE).map_or(0, Vec::len);
    // ~15 wide keys per leaf: past 30 rows the tree cannot still be a single
    // root leaf. Asserting it keeps a round from passing vacuously with no
    // split records in the stream that was just recovered.
    if ix_rows > 30 {
        assert!(
            index.tree_level() >= 1,
            "round {round}: {ix_rows} rows in {IX_TABLE} but its index never split — \
             this round exercised no split recovery"
        );
    }

    for (id, name) in expectation.tables.get(IX_TABLE).into_iter().flatten() {
        let found = engine
            .index_lookup(IX_TABLE, "name", &Datum::Text(name.clone()))
            .unwrap_or_else(|e| panic!("round {round}: index lookup for row {id} failed: {e}"));
        assert!(
            found.is_some(),
            "round {round}: committed row {id} of {IX_TABLE} is not reachable through its index"
        );
    }
}
