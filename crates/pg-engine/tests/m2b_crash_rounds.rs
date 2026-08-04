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

    let destructive = seed % 2 == 0;
    let mut rng = Rng(seed | 0x9E37_79B9_7F4A_7C15);
    let mut model = ChildModel {
        tables: BASE_TABLES
            .iter()
            .map(|t| (t.to_string(), Vec::new()))
            .collect(),
        extra_seq: 0,
        row_seq: 0,
    };

    let op_count = 40 + rng.below(40);
    for completed in 1..=op_count {
        do_random_op(&engine, &mut rng, &mut model, destructive);
        write_expectation(&engine, data_dir, &model, destructive, completed as usize);
    }
}

fn pick_table<'a>(rng: &mut Rng, model: &'a ChildModel) -> &'a str {
    let idx = rng.below(model.tables.len() as u64) as usize;
    model.tables.keys().nth(idx).expect("non-empty map")
}

fn do_random_op(engine: &Engine, rng: &mut Rng, model: &mut ChildModel, destructive: bool) {
    match rng.below(12) {
        // INSERT (50%).
        0..=5 => {
            let table = pick_table(rng, model).to_string();
            let id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(None, &format!("INSERT INTO {table} VALUES ({id}, 'row-{id}')"))
                .unwrap();
            model.tables.get_mut(&table).unwrap().push(id);
        }
        // UPDATE (17%): replace a random live row's id and name.
        6..=7 if destructive => {
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
        // DELETE (8%): remove a random live row.
        8 if destructive => {
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
        // CHECKPOINT (8%).
        9 => engine.checkpoint().unwrap(),
        // CREATE extra table (8%).
        10 => {
            let name = format!("extra{}", model.extra_seq);
            model.extra_seq += 1;
            engine
                .exec(None, &format!("CREATE TABLE {name} (id INT, name TEXT)"))
                .unwrap();
            model.tables.insert(name, Vec::new());
        }
        // Non-destructive substitute for UPDATE/DELETE/DROP slots: INSERT.
        _ => {
            let table = pick_table(rng, model).to_string();
            let id = model.row_seq;
            model.row_seq += 1;
            engine
                .exec(None, &format!("INSERT INTO {table} VALUES ({id}, 'row-{id}')"))
                .unwrap();
            model.tables.get_mut(&table).unwrap().push(id);
        }
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
            let target_ops = 1 + (round % 20) as usize;
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
    engine.shutdown();
}
