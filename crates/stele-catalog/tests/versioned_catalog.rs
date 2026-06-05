//! Versioned-catalog correctness tests (STL-98).
//!
//! Pins the two properties the ticket's Definition of Done calls for, through
//! the public API only:
//!
//! * **AS-OF across a schema change (DoD bullet 1).** Add a column, then resolve
//!   the table as of a snapshot *before* the change: the old, narrower schema
//!   comes back with no error — the guarantee that makes time-travel survive
//!   schema evolution.
//! * **Resolution matches a reference model.** Over a randomized DDL history, a
//!   deliberately dumb in-memory model (a list of `(effective_at, columns)`
//!   breakpoints, resolved by linear scan) and the real catalog return the same
//!   schema for thousands of random snapshots — a differential oracle in the
//!   house style ([testing strategy §4](../../../docs/06-testing-strategy.md#4-correctness-oracles-the-temporal-heart)).
//!
//! The structural invariant (gap-free / non-overlapping intervals, DoD bullet 2)
//! is asserted as a unit test next to the code, where the version chain is
//! visible.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;

use stele_catalog::{Catalog, ColumnDef, TableTemporal};
use stele_common::time::SystemTimeMicros;
use stele_common::types::LogicalType;

/// Tiny xorshift64* — deterministic, dependency-free (the workspace keeps no
/// proptest/quickcheck dep; seeded determinism is the house style, ADR-0010).
struct Rng(u64);
impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(name, LogicalType::Int8).expect("non-empty name")
}

#[test]
fn as_of_before_add_column_returns_the_old_schema() {
    let mut cat = Catalog::new();
    let t0 = SystemTimeMicros(1_000);
    let v0 = cat
        .create_table(
            "accounts",
            vec![col("id")],
            TableTemporal::system_only(),
            t0,
        )
        .expect("create");

    let t1 = SystemTimeMicros(2_000);
    let v1 = cat
        .add_column(
            "accounts",
            ColumnDef::new("email", LogicalType::Text).unwrap(),
            t1,
        )
        .expect("add column");

    // Before the table existed: nothing resolves (and no panic).
    assert!(cat.resolve("accounts", SystemTimeMicros(999)).is_none());

    // After creation, before the column was added: the original one-column
    // schema, returned cleanly — this is the DoD's "old schema is returned, no
    // errors".
    let old = cat
        .resolve("accounts", SystemTimeMicros(1_500))
        .expect("resolves");
    assert_eq!(old.schema_id(), v0);
    assert_eq!(old.columns().len(), 1);
    assert!(old.column("id").is_some());
    assert!(old.column("email").is_none());

    // At the change instant and after: the evolved two-column schema (half-open
    // interval — the new version owns `t1`).
    let new = cat.resolve("accounts", t1).expect("resolves");
    assert_eq!(new.schema_id(), v1);
    assert_eq!(new.columns().len(), 2);
    assert!(new.column("email").is_some());

    // The two snapshots resolved to genuinely different schema versions.
    assert_ne!(old.schema_id(), new.schema_id());
}

#[test]
fn catalog_resolves_identically_to_a_naive_reference_model() {
    let mut rng = Rng::new(0x5EED_0098);
    let tables = ["orders", "users"];

    for _ in 0..100 {
        let mut cat = Catalog::new();
        // Reference model: per table, the breakpoints `(effective_at, columns)`
        // in increasing time order. Resolution is a linear scan for the latest
        // breakpoint at or before the snapshot — too simple to be wrong.
        let mut model: BTreeMap<&str, Vec<(i64, Vec<String>)>> = BTreeMap::new();

        let mut clock = 1_i64;
        for t in tables {
            clock += 1 + rng.range(10) as i64;
            cat.create_table(
                t,
                vec![col("k")],
                TableTemporal::system_only(),
                SystemTimeMicros(clock),
            )
            .expect("create");
            model.insert(t, vec![(clock, vec!["k".to_string()])]);
        }

        let adds = rng.range(30);
        for i in 0..adds {
            clock += 1 + rng.range(10) as i64;
            let t = tables[rng.range(tables.len() as u64) as usize];
            let name = format!("f{i}");
            cat.add_column(t, col(&name), SystemTimeMicros(clock))
                .expect("add column");
            let history = model.get_mut(t).expect("table in model");
            let mut cols = history.last().expect("≥1 breakpoint").1.clone();
            cols.push(name);
            history.push((clock, cols));
        }

        // Fire random snapshots spanning before-the-first-table to after-the-last
        // change, and assert the catalog and the model agree exactly.
        let horizon = (clock as u64) + 10;
        for _ in 0..500 {
            let t = tables[rng.range(tables.len() as u64) as usize];
            let snap = rng.range(horizon) as i64;
            let got = cat.resolve(t, SystemTimeMicros(snap));
            let expect = model[t].iter().rev().find(|(at, _)| *at <= snap);
            match (got, expect) {
                (None, None) => {}
                (Some(schema), Some((_, want_cols))) => {
                    let mut got_names: Vec<&str> =
                        schema.columns().iter().map(ColumnDef::name).collect();
                    let mut want_names: Vec<&str> = want_cols.iter().map(String::as_str).collect();
                    got_names.sort_unstable();
                    want_names.sort_unstable();
                    assert_eq!(
                        got_names, want_names,
                        "schema mismatch at snapshot {snap} for {t}"
                    );
                }
                (g, e) => panic!(
                    "presence mismatch at snapshot {snap} for {t}: catalog={}, model={}",
                    g.is_some(),
                    e.is_some()
                ),
            }
        }
    }
}
