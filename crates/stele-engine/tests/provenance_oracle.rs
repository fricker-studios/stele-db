//! The provenance pseudo-column correctness oracle ([STL-247]).
//!
//! Reading a row's provenance inline — `_stele_txn_id`, `_stele_committed_at`,
//! `_stele_principal` — is an *audit-correctness* property: a row that reports the
//! wrong writing transaction is a silently false answer to "who wrote this?". So,
//! in the spirit of the temporal oracle
//! ([docs/06 §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart))
//! and as the ticket's correctness gate ("every row's `_stele_txn_id` matches the
//! transaction the reference model says wrote it"), this extends the STL-168
//! snapshot-isolation + provenance oracle from the storage/txn layer up to the
//! **SQL surface**: every provenance cell the real [`SessionEngine`] returns over
//! the bind → execute path is checked against a deliberately-dumb in-process
//! reference of who wrote each version.
//!
//! The reference mirrors the engine's deterministic allocation under the
//! [`ZeroClock`] + [`MonotonicClock`] pair: the shared high-water mark advances by
//! one on every committed write (and once on the `CREATE TABLE`), so a version's
//! `committed_at` (== its `sys_from`) and writing `txn_id` are exact functions of
//! write order — far too simple to be wrong, which is the point. A seeded workload
//! of inserts, updates, deletes and re-inserts builds a multi-version history in
//! both; then live, `AS OF`, and `WHERE`-on-pseudo-column reads are swept and the
//! engine's cells asserted byte-for-byte equal to the reference. The
//! [teeth test](#tests) perturbs a single `txn_id` in the reference and proves the
//! same differential catches it (the STL-168 `WrongWriter` mutation, at the SQL
//! surface).
//!
//! `_stele_principal` is the writing principal stored inline on each version. The
//! engine stamps the placeholder `stele` on every wire-issued write today;
//! threading the connection's authenticated user into it is a tracked follow-up
//! that changes the *value*, not this *surface* — so the oracle pins the surface
//! (the column resolves, hides from `SELECT *`, and filters in `WHERE`) and the
//! current value.
//!
//! [STL-168]: https://allegromusic.atlassian.net/browse/STL-168
//! [STL-247]: https://allegromusic.atlassian.net/browse/STL-247

use std::collections::{BTreeMap, BTreeSet};

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::{LogicalType, ScalarValue};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A constant inner clock; the engine's [`MonotonicClock`] turns its readings into
/// the strictly increasing `1, 2, 3, …` the writes need, deterministically — so a
/// version's `sys_from` (and `committed_at`) is exactly its position in the
/// write order. Matches the projection/predicate oracle's harness.
#[derive(Debug, Clone, Copy)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

/// A deterministic `splitmix64` so a seed replays an identical workload — no
/// dependency on the sim crate (this oracle drives the SQL path, not storage).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }
    const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// A uniform index into `0..len`.
    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("len fits u64");
        usize::try_from(self.next() % len).expect("index fits usize")
    }
    /// True with probability `1/n`.
    const fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
    }
}

/// The writing principal the engine stamps on every wire write today — the
/// placeholder identity surfaced by `_stele_principal` until the connection's user
/// is threaded through (a tracked follow-up).
const WIRE_PRINCIPAL: &str = "stele";

/// One committed version of a key, in write order: its commit instant (which the
/// engine sets equal to the version's `sys_from`), its writing transaction id, and
/// the balance — or `None` for a `DELETE` tombstone (the key is absent from that
/// point until re-inserted).
#[derive(Clone, Copy)]
struct Event {
    sys_from: i64,
    txn_id: i64,
    balance: Option<i32>,
}

/// The deliberately-dumb reference: per-key version history, plus a mirror of the
/// engine's monotonic high-water mark and transaction counter.
struct Reference {
    /// The shared commit high-water mark. `CREATE TABLE` advances it once; every
    /// committed write advances it once more, and the new value is that write's
    /// `sys_from` / `committed_at`.
    high_water: i64,
    /// The next transaction id the engine will stamp — `1`-based, bumped per
    /// committed write (a `CREATE` consumes none), mirroring `SessionEngine`.
    next_txn: i64,
    /// Each key's versions, oldest first.
    history: BTreeMap<i32, Vec<Event>>,
}

impl Reference {
    const fn new() -> Self {
        Self {
            high_water: 0,
            next_txn: 1,
            history: BTreeMap::new(),
        }
    }

    /// Account for the `CREATE TABLE`'s single commit tick (it stamps no txn id).
    const fn create(&mut self) {
        self.high_water += 1;
    }

    /// Record a committed write to `key` (a `Some` balance for insert/update, `None`
    /// for a delete), advancing the mirrors exactly as the engine does.
    fn write(&mut self, key: i32, balance: Option<i32>) {
        self.high_water += 1;
        let txn_id = self.next_txn;
        self.next_txn += 1;
        self.history.entry(key).or_default().push(Event {
            sys_from: self.high_water,
            txn_id,
            balance,
        });
    }

    /// The version of `key` live at system snapshot `s`: the latest version whose
    /// `sys_from <= s`, unless that version is a delete tombstone (then the key is
    /// absent). `None` for a key with no version at or before `s`.
    fn live_at(&self, key: i32, s: i64) -> Option<Event> {
        self.history
            .get(&key)?
            .iter()
            .rev()
            .find(|e| e.sys_from <= s)
            .copied()
            .filter(|e| e.balance.is_some())
    }

    /// Every key live at `s`, ascending — the row set a `SELECT … ORDER BY id`
    /// returns at that snapshot.
    fn live_keys_at(&self, s: i64) -> Vec<i32> {
        self.history
            .keys()
            .copied()
            .filter(|&k| self.live_at(k, s).is_some())
            .collect()
    }
}

/// Encode a scalar to its canonical bytes — the exact cell a `SelectResult` carries
/// for a present value (a SQL `NULL` is a bare `None` in the row literals below).
#[allow(clippy::unnecessary_wraps)]
fn cell(value: &ScalarValue) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    Some(bytes)
}

/// The reference's `[id, balance, _stele_txn_id, _stele_committed_at,
/// _stele_principal]` row for a key live at `s`, in the five-column projection's
/// order. `bug` adds one to the `txn_id` cell — the teeth seam; the correct
/// reference passes `false`.
fn full_row(model: &Reference, key: i32, s: i64, bug: bool) -> Vec<Option<Vec<u8>>> {
    let event = model.live_at(key, s).expect("key is live at s");
    let txn_id = event.txn_id + i64::from(bug);
    vec![
        cell(&ScalarValue::Int4(key)),
        cell(&ScalarValue::Int4(
            event.balance.expect("a live version has a balance"),
        )),
        cell(&ScalarValue::Int8(txn_id)),
        cell(&ScalarValue::TimestampTz(event.sys_from)),
        cell(&ScalarValue::Text(WIRE_PRINCIPAL.to_owned())),
    ]
}

/// The full five-column result the reference expects at snapshot `s`, keys ascending.
fn full_rows(model: &Reference, s: i64, bug: bool) -> Vec<Vec<Option<Vec<u8>>>> {
    model
        .live_keys_at(s)
        .into_iter()
        .map(|key| full_row(model, key, s, bug))
        .collect()
}

/// Run a statement against the engine, discarding the outcome (writes / DDL).
fn run(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) {
    let stmt = parse(sql).expect("parse").remove(0);
    engine.execute(&stmt).expect("execute");
}

/// Run a `SELECT` and return its full result (columns + rows).
fn select(engine: &mut SessionEngine<ZeroClock, MemDisk>, sql: &str) -> SelectResult {
    let stmt = parse(sql).expect("parse").remove(0);
    let StatementOutcome::Rows(result) = engine.execute(&stmt).expect("select") else {
        panic!("SELECT must return rows for `{sql}`");
    };
    result
}

/// The five-column provenance projection, plus a deterministic order.
const FULL_SELECT: &str =
    "SELECT id, balance, _stele_txn_id, _stele_committed_at, _stele_principal FROM t";

/// Build a seeded random multi-version history, applying it to a fresh engine and
/// the reference in lockstep. The workload only issues *valid* point writes — an
/// `INSERT` of an absent key, an `UPDATE`/`DELETE` of a live one — so every write
/// takes the single-key fast path (one transaction, one commit tick).
fn build(seed: u64) -> (SessionEngine<ZeroClock, MemDisk>, Reference) {
    let mut rng = Rng::new(seed);
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    let mut model = Reference::new();

    run(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    );
    model.create();

    let n_keys = 4 + rng.index(4); // ids 1..=n_keys
    let steps = 16 + rng.index(16);
    let mut live: BTreeSet<i32> = BTreeSet::new();
    for _ in 0..steps {
        let key = 1 + i32::try_from(rng.index(n_keys)).expect("key fits i32");
        let balance = i32::try_from(rng.index(1000)).expect("balance fits i32");
        if live.contains(&key) {
            if rng.one_in(3) {
                run(&mut engine, &format!("DELETE FROM t WHERE id = {key}"));
                model.write(key, None);
                live.remove(&key);
            } else {
                run(
                    &mut engine,
                    &format!("UPDATE t SET balance = {balance} WHERE id = {key}"),
                );
                model.write(key, Some(balance));
            }
        } else {
            run(
                &mut engine,
                &format!("INSERT INTO t VALUES ({key}, {balance})"),
            );
            model.write(key, Some(balance));
            live.insert(key);
        }
    }

    (engine, model)
}

/// Live, `AS OF`, and `WHERE`-on-pseudo-column reads all return provenance that
/// matches the reference — the SQL-surface provenance oracle ([STL-247]).
#[test]
fn provenance_matches_reference_across_seeds() {
    for seed in 0..64 {
        let (mut engine, model) = build(seed);

        // 1. The live read: every present row carries its live version's writing
        //    transaction, commit instant, and principal.
        let live = select(&mut engine, &format!("{FULL_SELECT} ORDER BY id"));
        assert_eq!(
            live.rows,
            full_rows(&model, model.high_water, false),
            "live provenance (seed {seed})",
        );

        // 2. `AS OF` every commit instant: each historical version reports its own
        //    provenance (a superseded or deleted version is gone, with it its
        //    provenance) — provenance is immutable on the version, not the latest.
        for s in 2..=model.high_water {
            let as_of = select(
                &mut engine,
                &format!("{FULL_SELECT} FOR SYSTEM_TIME AS OF {s} ORDER BY id"),
            );
            assert_eq!(
                as_of.rows,
                full_rows(&model, s, false),
                "AS OF {s} provenance (seed {seed})",
            );
        }

        // 3. `SELECT *` is the Postgres system-column posture: the pseudo-columns
        //    are hidden, only the user columns appear.
        let star = select(&mut engine, "SELECT * FROM t");
        let star_columns: Vec<(&str, LogicalType)> =
            star.columns.iter().map(|(n, t)| (n.as_str(), *t)).collect();
        assert_eq!(
            star_columns,
            vec![("id", LogicalType::Int4), ("balance", LogicalType::Int4)],
            "SELECT * hides pseudo-columns (seed {seed})",
        );

        // 4. A provenance pseudo-column is usable in `WHERE`. `_stele_txn_id = tx`
        //    returns exactly the live key whose live version that transaction wrote
        //    (each fast-path write touches one key); a superseded transaction's id
        //    matches no live row.
        for &key in &model.live_keys_at(model.high_water) {
            let tx = model.live_at(key, model.high_water).expect("live").txn_id;
            let by_txn = select(
                &mut engine,
                &format!("SELECT id FROM t WHERE _stele_txn_id = {tx} ORDER BY id"),
            );
            assert_eq!(
                by_txn.rows,
                vec![vec![cell(&ScalarValue::Int4(key))]],
                "WHERE _stele_txn_id = {tx} (seed {seed})",
            );
        }
        // A transaction id past every committed write wrote nothing live.
        let none = select(
            &mut engine,
            &format!(
                "SELECT id FROM t WHERE _stele_txn_id = {}",
                model.next_txn + 100
            ),
        );
        assert!(
            none.rows.is_empty(),
            "no row for an unused txn id (seed {seed})"
        );

        // 5. `_stele_principal` filters too: every live row was written by the
        //    placeholder principal, none by any other identity.
        let by_principal = select(
            &mut engine,
            "SELECT id FROM t WHERE _stele_principal = 'stele' ORDER BY id",
        );
        let live_ids: Vec<Vec<Option<Vec<u8>>>> = model
            .live_keys_at(model.high_water)
            .into_iter()
            .map(|k| vec![cell(&ScalarValue::Int4(k))])
            .collect();
        assert_eq!(
            by_principal.rows, live_ids,
            "WHERE _stele_principal = 'stele' (seed {seed})",
        );
        let other = select(
            &mut engine,
            "SELECT id FROM t WHERE _stele_principal = 'nobody'",
        );
        assert!(
            other.rows.is_empty(),
            "no row for a foreign principal (seed {seed})"
        );
    }
}

/// Provenance survives a `FLUSH`: after the delta tier is sealed into a segment,
/// the live and `AS OF` reads still return each version's writing provenance —
/// the columns are read back through the late-materialization path off the sealed
/// segment, not the delta ([STL-247]). Provenance is immutable on the version, so
/// the reference is unchanged by the flush.
#[test]
fn provenance_survives_flush_to_sealed_segments() {
    for seed in 0..16 {
        let (mut engine, model) = build(seed);

        // Seal every delta into a segment; the version provenance now lives in the
        // segment's `TxnId` / `CommittedAt` / `Principal` columns.
        run(&mut engine, "FLUSH");

        let live = select(&mut engine, &format!("{FULL_SELECT} ORDER BY id"));
        assert_eq!(
            live.rows,
            full_rows(&model, model.high_water, false),
            "live provenance after flush (seed {seed})",
        );
        for s in 2..=model.high_water {
            let as_of = select(
                &mut engine,
                &format!("{FULL_SELECT} FOR SYSTEM_TIME AS OF {s} ORDER BY id"),
            );
            assert_eq!(
                as_of.rows,
                full_rows(&model, s, false),
                "AS OF {s} provenance after flush (seed {seed})",
            );
        }
    }
}

/// Inside a transaction, a committed row reports its real provenance while the
/// transaction's own buffered (uncommitted) write reports `NULL` provenance — it
/// has no commit transaction, instant, or principal yet ([STL-247] over the
/// read-your-own-writes overlay, [STL-203]).
#[test]
fn a_buffered_write_has_null_provenance_in_read_your_own_writes() {
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    );
    run(&mut engine, "INSERT INTO t VALUES (1, 100)"); // committed: txn 1, sys_from 2

    // Open a block and buffer a second insert — uncommitted.
    let mut txn = engine.begin();
    let insert = parse("INSERT INTO t VALUES (2, 200)")
        .expect("parse")
        .remove(0);
    engine
        .execute_in_txn(&insert, &mut txn)
        .expect("buffered insert");

    let select = parse(&format!("{FULL_SELECT} ORDER BY id"))
        .expect("parse")
        .remove(0);
    let StatementOutcome::Rows(result) = engine
        .execute_in_txn(&select, &mut txn)
        .expect("read-your-own-writes select")
    else {
        panic!("SELECT must return rows");
    };

    assert_eq!(
        result.rows,
        vec![
            // The committed key 1 carries its real provenance.
            vec![
                cell(&ScalarValue::Int4(1)),
                cell(&ScalarValue::Int4(100)),
                cell(&ScalarValue::Int8(1)),
                cell(&ScalarValue::TimestampTz(2)),
                cell(&ScalarValue::Text(WIRE_PRINCIPAL.to_owned())),
            ],
            // The buffered key 2 is visible (read-your-own-writes) but uncommitted,
            // so all three provenance cells are NULL.
            vec![
                cell(&ScalarValue::Int4(2)),
                cell(&ScalarValue::Int4(200)),
                None,
                None,
                None,
            ],
        ],
    );
}

/// Provenance reads correctly off a **valid-time** table ([STL-247] "on any table
/// read"). The interesting interaction is the delta frame: a valid-time table
/// packs its period into a framed prefix the plain read strips ([STL-218]), and
/// the provenance scalars must survive that stripping intact — both on a plain
/// read and under a `FOR VALID_TIME AS OF` pin.
#[test]
fn provenance_reads_off_a_valid_time_table() {
    let mut engine = SessionEngine::open(MemDisk::new(), ZeroClock);
    run(
        &mut engine,
        "CREATE TABLE acct (id INT PRIMARY KEY, balance INT, vf TIMESTAMP, vt TIMESTAMP) \
         WITH SYSTEM VERSIONING VALID TIME (vf, vt)",
    );
    // One row, valid over [10, 20); the first committed write is transaction 1.
    run(&mut engine, "INSERT INTO acct VALUES (1, 50, 10, 20)");

    // A plain read: the user value is frame-stripped (balance 50), and the row
    // carries its writing provenance.
    let plain = select(
        &mut engine,
        "SELECT id, balance, _stele_txn_id, _stele_principal FROM acct",
    );
    assert_eq!(
        plain.rows,
        vec![vec![
            cell(&ScalarValue::Int4(1)),
            cell(&ScalarValue::Int4(50)),
            cell(&ScalarValue::Int8(1)),
            cell(&ScalarValue::Text(WIRE_PRINCIPAL.to_owned())),
        ]],
    );

    // A valid-time `AS OF` inside the period sees the row with its provenance;
    // outside the period sees nothing.
    let inside = select(
        &mut engine,
        "SELECT id, _stele_txn_id FROM acct FOR VALID_TIME AS OF 15",
    );
    assert_eq!(
        inside.rows,
        vec![vec![
            cell(&ScalarValue::Int4(1)),
            cell(&ScalarValue::Int8(1))
        ]],
    );
    let outside = select(
        &mut engine,
        "SELECT id, _stele_txn_id FROM acct FOR VALID_TIME AS OF 25",
    );
    assert!(outside.rows.is_empty(), "no row valid at 25");
}

/// The teeth test: a reference that mis-attributes a single row's writing
/// transaction (the STL-168 `WrongWriter` mutation, at the SQL surface) is caught
/// by the very same differential — proving the live-read assertion above is not
/// vacuous. Mirrors the storage-layer provenance oracle's negative test.
#[test]
fn perturbed_txn_id_is_caught() {
    // A seed whose workload leaves at least one live row, so there is a `txn_id`
    // cell to perturb.
    let (mut engine, model) = build(7);
    let live = select(&mut engine, &format!("{FULL_SELECT} ORDER BY id"));
    assert!(
        !live.rows.is_empty(),
        "the teeth test needs a live row to perturb",
    );

    // The correct reference matches; the perturbed one (every `txn_id + 1`) does not.
    assert_eq!(live.rows, full_rows(&model, model.high_water, false));
    assert_ne!(
        live.rows,
        full_rows(&model, model.high_water, true),
        "a wrong-writer reference must diverge from the engine",
    );
}
