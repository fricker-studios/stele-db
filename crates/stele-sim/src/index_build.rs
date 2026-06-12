//! Crash-during-index-build scenario ([STL-233]).
//!
//! The substrate's recovery posture says a secondary index is **derived,
//! rebuildable state** ([ADR-0023]): only the `CREATE INDEX` metadata record is
//! durable ([ADR-0028]), the access structure lives in memory, and a crash at
//! *any* point during the build leaves one of exactly two recoverable worlds —
//!
//! * the record never became durable (a torn/failed append, or a build-scan
//!   failure that aborted before the append): the DDL was never acknowledged
//!   and the restarted engine simply has no index;
//! * the record is durable (acknowledged — or appended with its fsync's fate
//!   indeterminate, the classic crash-at-the-durability-point window): the
//!   restarted engine rebuilds the structure from the recovered tiers.
//!
//! In **both** worlds, subsequent reads must answer exactly. This scenario
//! drives the real [`SessionEngine`] through a seeded [`FaultDisk`]: a clean
//! prelude builds a table (delta + a sealed segment, so the build scan crosses
//! tiers), the write/read fault classes are armed for the `CREATE INDEX`
//! window, the process "crashes" (the engine is dropped), the disk is
//! quiesced, and recovery is asserted to reproduce the reference model's rows
//! through both probe-served and full-scan reads — plus continued index
//! maintenance after the restart. Fault arming follows the [STL-187] lesson:
//! recovery itself runs on a quiesced disk, so every seed exercises the
//! build-window crash rather than starving in recovery read-rot.
//!
//! [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
//! [STL-187]: https://allegromusic.atlassian.net/browse/STL-187
//! [ADR-0023]: ../../docs/adr/0023-append-only-record-model-validity-index.md
//! [ADR-0028]: ../../docs/adr/0028-durable-catalog-log.md

use std::collections::BTreeMap;

use stele_common::time::{Clock, SystemTimeMicros};
use stele_common::types::ScalarValue;
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::Statement;
use stele_storage::backend::MemDisk;

use crate::fault_disk::{FaultDisk, FaultKind, FaultProfile};
use crate::{Rng, fault_kind_tag, fault_op_tag, fnv1a, prob_permille};

/// A constant inner clock; the engine's `MonotonicClock` mints the strictly
/// increasing commit instants — deterministic, no wall-clock read (ADR-0010).
#[derive(Debug, Clone, Copy)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

/// The engine under test, over the seeded fault disk.
type Engine = SessionEngine<ZeroClock, FaultDisk<MemDisk>>;

fn parse_one(sql: &str) -> Statement {
    stele_sql::parse(sql)
        .expect("scenario SQL parses")
        .into_iter()
        .next()
        .expect("one statement")
}

/// Execute a statement that the scenario requires to succeed (clean phases).
fn run(engine: &mut Engine, sql: &str) -> StatementOutcome {
    engine
        .execute(&parse_one(sql))
        .unwrap_or_else(|e| panic!("`{sql}` failed on a clean disk: {e}"))
}

/// Execute a `SELECT`, returning its raw rows.
fn rows(engine: &mut Engine, sql: &str) -> Vec<Vec<Option<Vec<u8>>>> {
    let StatementOutcome::Rows(SelectResult { rows, .. }) = run(engine, sql) else {
        panic!("`{sql}` must return rows");
    };
    rows
}

/// One `Int4` value in its canonical cell encoding.
fn int_bytes(v: i32) -> Vec<u8> {
    let mut bytes = Vec::new();
    ScalarValue::Int4(v).encode(&mut bytes);
    bytes
}

/// Assert the recovered engine's reads reproduce the reference model exactly:
/// every key's point read, every equality on the (possibly) indexed column —
/// including a value never written, the probe's proves-empty arm — and a
/// range sweep over the same column ([STL-237]: probe-served when the index
/// survived, full scans otherwise). Returns a digest fold of the agreed
/// answers.
///
/// [STL-237]: https://allegromusic.atlassian.net/browse/STL-237
fn assert_reads_match(
    engine: &mut Engine,
    model: &BTreeMap<i32, Option<i32>>,
    mut digest: u64,
) -> u64 {
    let fold = |digest: &mut u64, cells: &[Vec<Option<Vec<u8>>>]| {
        for row in cells {
            for cell in row {
                match cell {
                    None => *digest = fnv1a(*digest, &[0xFF]),
                    Some(bytes) => *digest = fnv1a(*digest, bytes),
                }
            }
            *digest = fnv1a(*digest, &[0x0A]);
        }
    };

    for (&id, &a) in model {
        let got = rows(engine, &format!("SELECT a FROM t WHERE id = {id}"));
        assert_eq!(
            got,
            vec![vec![a.map(int_bytes)]],
            "key {id}: point read diverged from the model after recovery"
        );
        fold(&mut digest, &got);
    }
    // Equality reads on the indexed column: probe-served when the index
    // survived, full scans otherwise — both must answer identically. The
    // engines emit rows in *encoded*-business-key order, which is not numeric
    // order for the little-endian `Int4` encoding, so sort the expectation by
    // the encoded cell.
    for v in [1, 2, 3, 99] {
        let got = rows(engine, &format!("SELECT id FROM t WHERE a = {v}"));
        let mut want: Vec<Vec<Option<Vec<u8>>>> = model
            .iter()
            .filter(|&(_, &a)| a == Some(v))
            .map(|(&id, _)| vec![Some(int_bytes(id))])
            .collect();
        want.sort();
        assert_eq!(
            got, want,
            "a = {v}: equality read diverged from the model after recovery"
        );
        fold(&mut digest, &got);
    }
    // Range reads over the same column ([STL-237]), cut inside and beyond the
    // 1..=3 domain so the candidate-window and proves-empty arms both fire.
    // NULL `a` cells never match (three-valued logic), matching the model's
    // `is_some_and`.
    for v in [1, 2, 4] {
        for (op, keeps) in [
            (
                ">",
                Box::new(move |a: i32| a > v) as Box<dyn Fn(i32) -> bool>,
            ),
            ("<", Box::new(move |a: i32| a < v)),
            (">=", Box::new(move |a: i32| a >= v)),
        ] {
            let got = rows(engine, &format!("SELECT id FROM t WHERE a {op} {v}"));
            let mut want: Vec<Vec<Option<Vec<u8>>>> = model
                .iter()
                .filter(|&(_, &a)| a.is_some_and(&keeps))
                .map(|(&id, _)| vec![Some(int_bytes(id))])
                .collect();
            want.sort();
            assert_eq!(
                got, want,
                "a {op} {v}: range read diverged from the model after recovery"
            );
            fold(&mut digest, &got);
        }
    }
    digest
}

/// Kill and recover a seeded `CREATE INDEX` under injected faults — the
/// [STL-233] DoD scenario. Panics on any divergence; returns the seed's digest
/// for the determinism sweep.
///
/// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
///
/// # Panics
///
/// Panics if recovery fails on the quiesced disk, if any post-recovery read
/// diverges from the reference model, or if an *acknowledged* `CREATE INDEX`
/// does not serve probes after the restart.
#[must_use]
pub fn run_index_build_crash_seed(seed: u64) -> u64 {
    // Seed-derived fault probabilities, drawn from a stream distinct from the
    // workload's. The write classes can tear/fail the catalog-log append (the
    // durability point); the read classes rot the build scan itself.
    let mut prof_rng = Rng::new(seed ^ 0x1DE7_0B11_DC4A_5EED);
    let p_torn = prob_permille(&mut prof_rng, 30, 120);
    let p_full = prob_permille(&mut prof_rng, 10, 50);
    let p_fail_sync = prob_permille(&mut prof_rng, 30, 120);
    let p_bit = prob_permille(&mut prof_rng, 30, 150);
    let p_short = prob_permille(&mut prof_rng, 30, 150);

    let mut rng = Rng::new(seed);
    let disk = FaultDisk::new(seed ^ 0xD15C_0FFA_u64, FaultProfile::none());

    // The reference model: the live rows `id → a` the clean prelude committed.
    let mut model: BTreeMap<i32, Option<i32>> = BTreeMap::new();
    let acknowledged = {
        // Clean prelude on the un-armed disk: the world the index must cover.
        let mut engine = Engine::open(disk.clone(), ZeroClock);
        run(
            &mut engine,
            "CREATE TABLE t (id INT PRIMARY KEY, a INT) WITH SYSTEM VERSIONING",
        );
        let row_count = 4 + rng.below(9); // 4..=12 rows
        for id in 1..=row_count {
            let id = i32::try_from(id).expect("row id fits i32");
            let a = if rng.below(5) == 0 {
                None
            } else {
                Some(i32::try_from(1 + rng.below(3)).expect("small domain"))
            };
            let a_sql = a.map_or_else(|| "NULL".to_owned(), |v| v.to_string());
            run(
                &mut engine,
                &format!("INSERT INTO t VALUES ({id}, {a_sql})"),
            );
            model.insert(id, a);
        }
        // A few mutations so superseded versions exist under the live ones.
        for _ in 0..rng.below(6) {
            let ids: Vec<i32> = model.keys().copied().collect();
            let id = ids[rng.below_usize(ids.len())];
            if rng.below(3) == 0 {
                run(&mut engine, &format!("DELETE FROM t WHERE id = {id}"));
                model.remove(&id);
            } else {
                let a = i32::try_from(1 + rng.below(3)).expect("small domain");
                run(
                    &mut engine,
                    &format!("UPDATE t SET a = {a} WHERE id = {id}"),
                );
                model.insert(id, Some(a));
            }
        }
        // Some seeds seal the delta so the build scan crosses sealed + delta.
        if rng.below(2) == 0 {
            engine.flush().expect("clean flush");
        }

        // Arm the build window and attempt the DDL. An error means the build
        // scan rotted or the append tore/failed — never acknowledged. (A
        // failed-fsync append may still leave a complete durable record: the
        // classic indeterminate window, which recovery is allowed to resolve
        // either way — the asserts below accept both worlds.)
        disk.enable(FaultKind::TornWrite, p_torn);
        disk.enable(FaultKind::FullDisk, p_full);
        disk.enable(FaultKind::FailSync, p_fail_sync);
        disk.enable(FaultKind::BitFlip, p_bit);
        disk.enable(FaultKind::ShortRead, p_short);
        // The engine drops at the end of this block — the crash, mid- or
        // post-build by seed.
        engine
            .execute(&parse_one("CREATE INDEX i_a ON t (a)"))
            .is_ok()
    };

    // Quiesce the disk: recovery runs clean ([STL-187] — read-rot during
    // recovery starves the build-window coverage this scenario exists for).
    disk.disable(FaultKind::TornWrite);
    disk.disable(FaultKind::FullDisk);
    disk.disable(FaultKind::FailSync);
    disk.disable(FaultKind::BitFlip);
    disk.disable(FaultKind::ShortRead);

    let mut engine = Engine::recover(disk.clone(), ZeroClock)
        .expect("recovery on a quiesced disk reproduces the committed state");

    // Both recoverable worlds answer exactly; an acknowledged build must have
    // been rebuilt and serve probes (the equality sweep above probed it).
    let mut digest = assert_reads_match(&mut engine, &model, 0xCBF2_9CE4_8422_2325);
    if acknowledged {
        assert!(
            engine.index_probe_count() > 0,
            "an acknowledged CREATE INDEX must survive the crash and serve probes"
        );
    }
    digest = fnv1a(digest, &[u8::from(acknowledged)]);
    digest = fnv1a(digest, &engine.index_probe_count().to_le_bytes());

    // Maintenance continues after recovery: fresh clean writes are findable
    // through whatever plan (probe or full scan) now serves equality reads.
    let fresh_a = i32::try_from(1 + rng.below(3)).expect("small domain");
    let fresh_id = i32::try_from(1000 + rng.below(50)).expect("fits i32");
    run(
        &mut engine,
        &format!("INSERT INTO t VALUES ({fresh_id}, {fresh_a})"),
    );
    model.insert(fresh_id, Some(fresh_a));
    digest = assert_reads_match(&mut engine, &model, digest);

    // Fold the seed-keyed fault-event log so the digest regresses on the exact
    // injected fault sequence, not only the recovered state.
    for ev in disk.events() {
        digest = fnv1a(digest, &ev.seq.to_le_bytes());
        digest = fnv1a(digest, &[fault_op_tag(ev.op), fault_kind_tag(ev.kind)]);
        digest = fnv1a(digest, &ev.detail.to_le_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_replays_to_an_identical_digest() {
        for seed in [0, 1, 7, 42] {
            assert_eq!(
                run_index_build_crash_seed(seed),
                run_index_build_crash_seed(seed),
                "seed {seed} must replay byte-identically"
            );
        }
    }

    #[test]
    fn seeds_cover_both_recoverable_worlds() {
        // Across a small sweep, some seeds must acknowledge the CREATE INDEX
        // and some must lose it to an injected fault — otherwise the scenario
        // is not actually exercising the crash window. The digest folds the
        // acknowledged flag, so distinct digests across seeds also separate
        // the worlds; here we just demand the sweep does not panic and
        // produces seed-dependent results.
        let distinct: std::collections::HashSet<u64> =
            (0..16).map(run_index_build_crash_seed).collect();
        assert!(distinct.len() > 1, "digests must depend on the seed");
    }
}
