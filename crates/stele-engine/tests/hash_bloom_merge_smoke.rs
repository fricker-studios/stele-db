//! Perf smoke for the per-segment bloom filters and the hash index ([STL-238]).
//!
//! Two halves of the ticket, end to end through SQL:
//!
//! * **`MERGE` / point reads over a large, scattered keyspace probe via the
//!   per-segment blooms.** The business key is always indexed, so a point probe
//!   pushes down to it and the blooms skip whole segments the key is absent from
//!   — observable as `scan_segments_pruned_bloom` in the metrics. The indexed
//!   `MERGE` plan (per source key) and the full-keyset plan must agree
//!   byte-for-byte.
//! * **`CREATE INDEX … USING HASH` on a value column accelerates its equality
//!   probes.** Observable as `index_probe_count`.
//!
//! The keys are zero-padded text (`k0000`..`k0599`) so their canonical byte order
//! equals their numeric order, and they are *interleaved* across six flushed
//! segments — segment `s` holds every sixth key. Every segment's business-key
//! zone map therefore spans almost the whole `[k0000, k0599]` range, so a point
//! probe for a key present in only one segment **survives every segment's zone
//! map** and can only be pruned by the bloom — exactly the random/hash-key case
//! zone maps cannot handle. The exact ScanStats partition is pinned at the scan
//! layer (`stele-exec/tests/snapshot_scan.rs`); this is the end-to-end proof.
//!
//! [STL-238]: https://allegromusic.atlassian.net/browse/STL-238

use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::{SelectResult, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

#[derive(Clone)]
struct ZeroClock;
impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

const SEGMENTS: i64 = 6;
const PER_SEGMENT: i64 = 100;

/// The `MERGE` both plans run: two matched updates (`k0150`, `k0151` — present,
/// in different segments) and one not-matched insert (`k0900` — beyond the
/// domain). Shared so the per-source-key and full-keyset runs are provably equal.
const MERGE_SQL: &str = "MERGE INTO t USING (VALUES ('k0150', 1), ('k0151', 2), ('k0900', 3)) \
     AS s (id, v) ON t.id = s.id \
     WHEN MATCHED THEN UPDATE SET v = s.v \
     WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)";

/// An `INT` (`Int4`) result cell's bytes — the engine returns the value as raw
/// little-endian bytes, not a rendered string (rendering is the wire encoder's
/// job). Wrap in `Some` at the call site for a non-NULL cell.
fn int4(v: i32) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

struct Db {
    engine: SessionEngine<ZeroClock, MemDisk>,
}

impl Db {
    fn fresh() -> Self {
        Self {
            engine: SessionEngine::open(MemDisk::new(), ZeroClock),
        }
    }

    fn run(&mut self, sql: &str) -> StatementOutcome {
        let stmt = parse(sql).expect("parse").remove(0);
        self.engine
            .execute(&stmt)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Option<Vec<u8>>>> {
        let StatementOutcome::Rows(SelectResult { rows, .. }) = self.run(sql) else {
            panic!("`{sql}` must return rows");
        };
        rows
    }
}

/// Load the scattered keyspace: segment `s` holds every sixth key across
/// `[k0000, k0599]`, each batch sealed into its own segment — six overlapping
/// segments, the on-disk, bloom-prunable shape every test here needs.
fn load_scattered(db: &mut Db) {
    db.run("CREATE TABLE t (id TEXT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    for seg in 0..SEGMENTS {
        let values: Vec<String> = (0..PER_SEGMENT)
            .map(|k| {
                let n = seg + SEGMENTS * k;
                format!("('k{n:04}', {n})")
            })
            .collect();
        db.run(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        db.run("FLUSH");
    }
}

#[test]
fn merge_runs_the_cost_chosen_plan_and_both_yield_the_same_upsert() {
    // STL-312: the MERGE probe plan is chosen by cost — point-probe each source
    // key when the source is smaller than the live keyspace, else read every live
    // key in one full-keyset scan. The same three-row upsert must land identically
    // under either plan; the plan changes speed, never the result.

    // Indexed (per-key) arm: a large flushed keyspace (600 keys across six
    // overlapping segments) and a three-row source — 3 < 600, so each source key
    // point-probes the always-indexed business key and the blooms skip the
    // segments it is absent from. Each matched key lives in one of six overlapping
    // segments, so its probe bloom-prunes the other five — skips zone maps cannot
    // give.
    let mut indexed = Db::fresh();
    load_scattered(&mut indexed);
    let bloom_before = indexed.engine.metrics().scan_segments_pruned_bloom.get();
    indexed.run(MERGE_SQL);
    let bloom_after = indexed.engine.metrics().scan_segments_pruned_bloom.get();
    assert!(
        bloom_after > bloom_before,
        "the small-source MERGE must point-probe and bloom-prune segments (pruned {bloom_before} → {bloom_after})",
    );

    // Full-keyset arm: a flushed target holding only the two keys the source
    // matches — 3 ≥ 2, so the cost choice flips to a single scan of the whole
    // keyspace (the corner the old "any sealed segment ⇒ probe" heuristic got
    // wrong: it would have point-probed here too).
    let mut full_keyset = Db::fresh();
    full_keyset.run("CREATE TABLE t (id TEXT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING");
    full_keyset.run("INSERT INTO t VALUES ('k0150', 150), ('k0151', 151)");
    full_keyset.run("FLUSH");
    full_keyset.run(MERGE_SQL);

    // Both plans resolved the same arms: k0150/k0151 matched (updated to 1/2),
    // k0900 not matched (inserted as 3). Compare the touched keys key-for-key.
    for (id, want) in [("k0150", 1), ("k0151", 2), ("k0900", 3)] {
        let from_probe = indexed.rows(&format!("SELECT v FROM t WHERE id = '{id}'"));
        let from_scan = full_keyset.rows(&format!("SELECT v FROM t WHERE id = '{id}'"));
        assert_eq!(
            from_probe,
            vec![vec![Some(int4(want))]],
            "per-key plan upserted {id}",
        );
        assert_eq!(from_scan, from_probe, "the full-keyset plan agrees on {id}");
    }

    // Sanity: the indexed arm's upsert grew the table by exactly the one insert.
    let all = indexed.rows("SELECT id FROM t");
    assert_eq!(
        all.len(),
        usize::try_from(SEGMENTS * PER_SEGMENT + 1).unwrap()
    );
}

#[test]
fn point_select_over_a_scattered_keyspace_skips_segments_via_the_bloom() {
    // The single-key read path: a point `SELECT` over the overlapping segments
    // pushes down to the always-indexed business key and bloom-prunes the
    // segments that do not hold it, returning exactly the one row.
    let mut db = Db::fresh();
    load_scattered(&mut db);

    let before = db.engine.metrics().scan_segments_pruned_bloom.get();
    let row = db.rows("SELECT v FROM t WHERE id = 'k0150'");
    let after = db.engine.metrics().scan_segments_pruned_bloom.get();

    assert_eq!(row, vec![vec![Some(int4(150))]], "the one live row");
    assert!(
        after > before,
        "the point read must bloom-prune the segments without the key ({before} → {after})",
    );
}

#[test]
fn blooms_survive_compaction() {
    // History-preserving compaction (STL-231) rewrites segments through the same
    // SegmentWriter, so the merged segments carry fresh blooms ([STL-238]). After
    // compacting the scattered keyspace, a point read still finds its row — the
    // new bloom never wrongly prunes a present key — and an absent key still reads
    // empty. The bloom survived the rewrite.
    let mut db = Db::fresh();
    load_scattered(&mut db);
    db.run("COMPACT");

    let present = db.rows("SELECT v FROM t WHERE id = 'k0303'");
    assert_eq!(
        present,
        vec![vec![Some(int4(303))]],
        "a present key survives compaction's rebuilt bloom",
    );
    let absent = db.rows("SELECT v FROM t WHERE id = 'k9999'");
    assert!(absent.is_empty(), "an absent key still reads empty");
}

#[test]
fn hash_index_on_a_value_column_is_consulted_for_equality() {
    // The hash access structure ([STL-238]): `USING HASH` on a *value* column
    // (the business key is always indexed and cannot take a secondary index)
    // serves equality probes — observable via the index-probe counter — and
    // returns the same rows a full scan would.
    let mut db = Db::fresh();
    load_scattered(&mut db);
    db.run("CREATE INDEX i_v ON t USING HASH (v)");

    let before = db.engine.index_probe_count();
    let hit = db.rows("SELECT id FROM t WHERE v = 150");
    let miss = db.rows("SELECT id FROM t WHERE v = 99999");
    let after = db.engine.index_probe_count();

    assert_eq!(
        hit,
        vec![vec![Some(b"k0150".to_vec())]],
        "the one matching row"
    );
    assert!(miss.is_empty(), "no row has v = 99999");
    assert!(
        after >= before + 2,
        "both equality probes must consult the hash index ({before} → {after})",
    );
}
