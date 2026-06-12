//! A light metrics facade — atomic counters, gauges, and fixed-bucket
//! histograms with Prometheus text exposition ([STL-253]).
//!
//! Deliberately not a general-purpose metrics framework: the workspace's
//! whole metric set is the named fields of [`Metrics`], registered nowhere
//! and discovered by nothing. That keeps the facade dependency-free, keeps
//! exposition a plain string walk over known fields,
//! and keeps every instrumentation site a lock-free atomic bump that the
//! deterministic core can execute under the simulation scheduler without
//! observable side effects ([ADR-0010]). Exposition is [`Metrics::render`].
//!
//! **No global registry.** This crate avoids global state (see the crate
//! docs), so a [`Metrics`] is plain instance state: the session engine owns
//! one and shares it by [`Arc`] ([`SharedMetrics`]) with the storage tiers,
//! the wire front end, and the ops HTTP listener that renders it.
//!
//! **Time is injected, never read.** Latency observations need a clock, and
//! the storage/txn core must not read one ([ADR-0010]). So durations come
//! from an installable monotonic [time source](Metrics::install_time_source):
//! the production server installs one at boot; under the simulator (or any
//! test that installs none) [`Metrics::now_micros`] returns `0`, every
//! observed duration is zero, and behavior stays bit-identical across runs —
//! the same injection seam as [`Clock`](crate::time::Clock) /
//! [`SystemClock`](crate::time::SystemClock).
//!
//! [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
//! [ADR-0010]: ../../../docs/adr/0010-deterministic-simulation-testing.md

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// A shared handle to the one [`Metrics`] instance a server process exposes.
pub type SharedMetrics = Arc<Metrics>;

/// A monotonically increasing event count.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Add one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Add `n`.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// The current count.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A value that can go up and down (e.g. active connections).
#[derive(Debug, Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    /// Add one.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Subtract one.
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    /// The current value.
    #[must_use]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// The shared latency bucket bounds, as `(upper bound in micros, Prometheus
/// `le` label)` pairs. Spanning 100µs–5s covers everything from a delta-tier
/// point read to a pathological flush; the label strings are exact decimal
/// renderings of the bound in **seconds**, paired here so exposition never
/// goes through floating point.
const BUCKETS: [(u64, &str); 15] = [
    (100, "0.0001"),
    (250, "0.00025"),
    (500, "0.0005"),
    (1_000, "0.001"),
    (2_500, "0.0025"),
    (5_000, "0.005"),
    (10_000, "0.01"),
    (25_000, "0.025"),
    (50_000, "0.05"),
    (100_000, "0.1"),
    (250_000, "0.25"),
    (500_000, "0.5"),
    (1_000_000, "1"),
    (2_500_000, "2.5"),
    (5_000_000, "5"),
];

/// A fixed-bucket latency histogram (bounds: the crate-private `BUCKETS`
/// table, plus `+Inf`).
///
/// Internally counts **microseconds** in per-bucket atomics; [`Metrics::render`]
/// exposes it in Prometheus convention — cumulative `_bucket{le="…"}` series in
/// seconds, plus `_sum` (seconds) and `_count`. The `_count` doubles as the
/// operation counter, so instrumented operations need no separate `_total`.
#[derive(Debug, Default)]
pub struct Histogram {
    /// Non-cumulative per-bucket hit counts; index i ↔ `BUCKETS[i]`, with one
    /// extra overflow bucket (`+Inf`) at the end.
    buckets: [Counter; BUCKETS.len() + 1],
    sum_micros: Counter,
    count: Counter,
}

impl Histogram {
    /// Record one observation of `micros`.
    pub fn observe_micros(&self, micros: u64) {
        let idx = BUCKETS
            .iter()
            .position(|&(bound, _)| micros <= bound)
            .unwrap_or(BUCKETS.len());
        self.buckets[idx].inc();
        self.sum_micros.add(micros);
        self.count.inc();
    }

    /// Total number of observations.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.get()
    }
}

/// The statement classes the per-statement series are labeled by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// A table or constant `SELECT`.
    Select,
    /// `INSERT`.
    Insert,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
    /// `CREATE TABLE` / `DROP TABLE`.
    Ddl,
    /// An operator admin command (`CHECKPOINT` / `FLUSH`).
    Admin,
}

impl StatementKind {
    /// Every kind, in stable exposition order.
    const ALL: [Self; 6] = [
        Self::Select,
        Self::Insert,
        Self::Update,
        Self::Delete,
        Self::Ddl,
        Self::Admin,
    ];

    /// The `kind` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Select => "select",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Ddl => "ddl",
            Self::Admin => "admin",
        }
    }

    /// Index into the per-kind counter array.
    const fn idx(self) -> usize {
        match self {
            Self::Select => 0,
            Self::Insert => 1,
            Self::Update => 2,
            Self::Delete => 3,
            Self::Ddl => 4,
            Self::Admin => 5,
        }
    }

    /// Index into the coarser latency-histogram array: the ticket's
    /// `SELECT` / DML / DDL split (admin folds into DDL — its real cost is
    /// broken out by the dedicated flush/checkpoint histograms).
    const fn latency_idx(self) -> usize {
        match self {
            Self::Select => 0,
            Self::Insert | Self::Update | Self::Delete => 1,
            Self::Ddl | Self::Admin => 2,
        }
    }

    /// The `kind` label of the latency histogram at `latency_idx` `i`.
    const fn latency_label(i: usize) -> &'static str {
        match i {
            0 => "select",
            1 => "dml",
            _ => "ddl",
        }
    }
}

/// The process's metric registry — every series the `/metrics` endpoint
/// exposes, as one struct of named atomics ([STL-253]).
///
/// Construction is [`Default`]; share it as a [`SharedMetrics`]. Compaction
/// and backup series land with their features (STL-231 / STL-249).
///
/// [STL-253]: https://allegromusic.atlassian.net/browse/STL-253
#[derive(Debug, Default)]
pub struct Metrics {
    /// The installed monotonic time source ([`Self::install_time_source`]).
    time_source: OnceLock<fn() -> u64>,

    /// Connections accepted by the pg-wire listener, ever.
    pub connections_total: Counter,
    /// Currently open pg-wire connections.
    pub connections_active: Gauge,

    /// Successfully executed statements by kind (`select` / `insert` /
    /// `update` / `delete` / `ddl` / `admin`); indexed by
    /// [`StatementKind::idx`]. Errored statements count in
    /// [`statement_errors`](Self::statement_errors) instead.
    statements: [Counter; 6],
    /// Statement latency by coarse kind (`select` / `dml` / `ddl`); indexed
    /// by [`StatementKind::latency_idx`].
    statement_seconds: [Histogram; 3],
    /// Statements that returned an error (any kind).
    pub statement_errors: Counter,
    /// Rows returned to clients by `SELECT` statements.
    pub rows_returned: Counter,
    /// Rows written by `INSERT` / `UPDATE` / `DELETE` statements, counted at
    /// statement execution (a buffered transactional write counts when staged,
    /// even if the transaction later rolls back — see `stele_txn_rollbacks_total`).
    pub rows_written: Counter,

    /// Multi-statement transactions committed.
    pub txn_commits: Counter,
    /// Multi-statement transactions rolled back (explicit `ROLLBACK`, or a
    /// `COMMIT` of an aborted block).
    pub txn_rollbacks: Counter,
    /// Commits refused by first-committer-wins conflict detection.
    pub txn_conflicts: Counter,

    /// WAL records staged (`Wal::append`), across every table's WAL.
    pub wal_appends: Counter,
    /// WAL fsync latency — the group-commit durability point. `_count` is the
    /// number of fsyncs (group-commit ticks and segment-rotation syncs).
    pub wal_fsync_seconds: Histogram,

    /// Flush (seal delta → sealed segment) duration; `_count` is successful runs.
    pub flush_seconds: Histogram,
    /// Checkpoint (durability fence) duration; `_count` is successful runs.
    pub checkpoint_seconds: Histogram,

    /// Sealed segments actually scanned by snapshot reads.
    pub scan_segments_scanned: Counter,
    /// Sealed segments skipped by zone-map pruning.
    pub scan_segments_pruned_zone: Counter,
    /// Sealed segments skipped because every version in them is superseded at
    /// the read snapshot (validity-index prune).
    pub scan_segments_pruned_superseded: Counter,
    /// Row groups scanned within non-pruned segments.
    pub scan_row_groups_scanned: Counter,
    /// Row groups skipped by per-row-group zone-map pruning.
    pub scan_row_groups_pruned_zone: Counter,
}

impl Metrics {
    /// Install the monotonic time source latency observations read, e.g. a
    /// process-uptime-in-micros reader. First caller wins; later calls are
    /// no-ops. Hosts that never install one (the simulator, unit tests) get
    /// zero from [`now_micros`](Self::now_micros) everywhere — durations all
    /// observe as zero and behavior stays deterministic.
    pub fn install_time_source(&self, source: fn() -> u64) {
        let _ = self.time_source.set(source);
    }

    /// Microseconds from the installed time source, or `0` when none is
    /// installed. Only differences are meaningful.
    #[must_use]
    pub fn now_micros(&self) -> u64 {
        self.time_source.get().map_or(0, |f| f())
    }

    /// Record one completed statement of `kind` taking `elapsed_micros`.
    pub fn observe_statement(&self, kind: StatementKind, elapsed_micros: u64) {
        self.statements[kind.idx()].inc();
        self.statement_seconds[kind.latency_idx()].observe_micros(elapsed_micros);
    }

    /// Completed-statement count for `kind` (test/inspection accessor).
    #[must_use]
    pub fn statements(&self, kind: StatementKind) -> u64 {
        self.statements[kind.idx()].get()
    }

    /// Render the whole registry in the Prometheus text exposition format
    /// (version 0.0.4 — the stable `text/plain` format every scraper accepts).
    // One linear walk over every registered family; splitting it would only
    // scatter the exposition order.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);

        gauge(
            &mut out,
            "stele_connections_active",
            "Currently open pg-wire connections.",
            self.connections_active.get(),
        );
        counter(
            &mut out,
            "stele_connections_total",
            "Connections accepted by the pg-wire listener.",
            self.connections_total.get(),
        );

        header(
            &mut out,
            "stele_statements_total",
            "Successfully executed statements by kind (errors count in stele_statement_errors_total).",
            "counter",
        );
        for kind in StatementKind::ALL {
            let _ = writeln!(
                out,
                "stele_statements_total{{kind=\"{}\"}} {}",
                kind.as_str(),
                self.statements[kind.idx()].get()
            );
        }
        header(
            &mut out,
            "stele_statement_seconds",
            "Statement latency by kind.",
            "histogram",
        );
        for (i, hist) in self.statement_seconds.iter().enumerate() {
            let label = format!("kind=\"{}\"", StatementKind::latency_label(i));
            histogram_series(&mut out, "stele_statement_seconds", Some(&label), hist);
        }
        counter(
            &mut out,
            "stele_statement_errors_total",
            "Statements that returned an error.",
            self.statement_errors.get(),
        );
        counter(
            &mut out,
            "stele_rows_returned_total",
            "Rows returned by SELECT statements.",
            self.rows_returned.get(),
        );
        counter(
            &mut out,
            "stele_rows_written_total",
            "Rows written by INSERT/UPDATE/DELETE statements.",
            self.rows_written.get(),
        );

        counter(
            &mut out,
            "stele_txn_commits_total",
            "Multi-statement transactions committed.",
            self.txn_commits.get(),
        );
        counter(
            &mut out,
            "stele_txn_rollbacks_total",
            "Multi-statement transactions rolled back.",
            self.txn_rollbacks.get(),
        );
        counter(
            &mut out,
            "stele_txn_conflicts_total",
            "Commits refused by write-write conflict detection.",
            self.txn_conflicts.get(),
        );

        counter(
            &mut out,
            "stele_wal_appends_total",
            "WAL records staged, across every table's WAL.",
            self.wal_appends.get(),
        );
        header(
            &mut out,
            "stele_wal_fsync_seconds",
            "WAL fsync latency (the durability point).",
            "histogram",
        );
        histogram_series(
            &mut out,
            "stele_wal_fsync_seconds",
            None,
            &self.wal_fsync_seconds,
        );

        header(
            &mut out,
            "stele_flush_seconds",
            "Flush (seal delta into sealed segment) duration.",
            "histogram",
        );
        histogram_series(&mut out, "stele_flush_seconds", None, &self.flush_seconds);
        header(
            &mut out,
            "stele_checkpoint_seconds",
            "Checkpoint (durability fence) duration.",
            "histogram",
        );
        histogram_series(
            &mut out,
            "stele_checkpoint_seconds",
            None,
            &self.checkpoint_seconds,
        );

        counter(
            &mut out,
            "stele_scan_segments_scanned_total",
            "Sealed segments scanned by snapshot reads.",
            self.scan_segments_scanned.get(),
        );
        counter(
            &mut out,
            "stele_scan_segments_pruned_zone_total",
            "Sealed segments skipped by zone-map pruning.",
            self.scan_segments_pruned_zone.get(),
        );
        counter(
            &mut out,
            "stele_scan_segments_pruned_superseded_total",
            "Sealed segments skipped as fully superseded at the read snapshot.",
            self.scan_segments_pruned_superseded.get(),
        );
        counter(
            &mut out,
            "stele_scan_row_groups_scanned_total",
            "Row groups scanned within non-pruned segments.",
            self.scan_row_groups_scanned.get(),
        );
        counter(
            &mut out,
            "stele_scan_row_groups_pruned_zone_total",
            "Row groups skipped by per-row-group zone-map pruning.",
            self.scan_row_groups_pruned_zone.get(),
        );

        out
    }
}

/// Render micros as an exact decimal seconds string (no floating point):
/// `1_500_000` → `"1.5"`, `250` → `"0.00025"`, `2_000_000` → `"2"`.
fn micros_as_seconds(micros: u64) -> String {
    let whole = micros / 1_000_000;
    let frac = micros % 1_000_000;
    if frac == 0 {
        return whole.to_string();
    }
    let mut s = format!("{whole}.{frac:06}");
    while s.ends_with('0') {
        s.pop();
    }
    s
}

fn header(out: &mut String, name: &str, help: &str, ty: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {ty}");
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    header(out, name, help, "counter");
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: i64) {
    header(out, name, help, "gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// Render one histogram's `_bucket`/`_sum`/`_count` series, with `extra_label`
/// (e.g. `kind="select"`) folded into each series' label set when present.
fn histogram_series(out: &mut String, name: &str, extra_label: Option<&str>, h: &Histogram) {
    let prefix = extra_label.map_or(String::new(), |l| format!("{l},"));
    let mut cumulative = 0u64;
    for (i, &(_, le)) in BUCKETS.iter().enumerate() {
        cumulative += h.buckets[i].get();
        let _ = writeln!(out, "{name}_bucket{{{prefix}le=\"{le}\"}} {cumulative}");
    }
    cumulative += h.buckets[BUCKETS.len()].get();
    let _ = writeln!(out, "{name}_bucket{{{prefix}le=\"+Inf\"}} {cumulative}");
    let suffix_labels = extra_label.map_or(String::new(), |l| format!("{{{l}}}"));
    let _ = writeln!(
        out,
        "{name}_sum{suffix_labels} {}",
        micros_as_seconds(h.sum_micros.get())
    );
    let _ = writeln!(out, "{name}_count{suffix_labels} {}", h.count.get());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_and_gauge_basics() {
        let c = Counter::default();
        c.inc();
        c.add(4);
        assert_eq!(c.get(), 5);

        let g = Gauge::default();
        g.inc();
        g.inc();
        g.dec();
        assert_eq!(g.get(), 1);
    }

    #[test]
    fn histogram_buckets_are_cumulative_in_render() {
        let m = Metrics::default();
        m.wal_fsync_seconds.observe_micros(50); // ≤ 100µs bucket
        m.wal_fsync_seconds.observe_micros(200_000); // ≤ 0.25s bucket
        m.wal_fsync_seconds.observe_micros(60_000_000); // overflow → +Inf only

        let text = m.render();
        assert!(text.contains("stele_wal_fsync_seconds_bucket{le=\"0.0001\"} 1"));
        // Cumulative: the 0.25s bucket counts the 100µs observation too.
        assert!(text.contains("stele_wal_fsync_seconds_bucket{le=\"0.25\"} 2"));
        assert!(text.contains("stele_wal_fsync_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(text.contains("stele_wal_fsync_seconds_count 3"));
        // Sum: 50 + 200_000 + 60_000_000 micros = 60.20005 s, exactly.
        assert!(text.contains("stele_wal_fsync_seconds_sum 60.20005"));
    }

    #[test]
    fn statement_kinds_render_with_labels() {
        let m = Metrics::default();
        m.observe_statement(StatementKind::Select, 1_000);
        m.observe_statement(StatementKind::Insert, 2_000);
        m.observe_statement(StatementKind::Insert, 2_000);

        assert_eq!(m.statements(StatementKind::Insert), 2);
        let text = m.render();
        assert!(text.contains("stele_statements_total{kind=\"select\"} 1"));
        assert!(text.contains("stele_statements_total{kind=\"insert\"} 2"));
        assert!(text.contains("stele_statements_total{kind=\"ddl\"} 0"));
        // Latency folds insert/update/delete into the dml histogram.
        assert!(text.contains("stele_statement_seconds_count{kind=\"dml\"} 2"));
        assert!(text.contains("stele_statement_seconds_bucket{kind=\"select\",le=\"0.001\"} 1"));
    }

    #[test]
    fn time_source_defaults_to_zero_and_installs_once() {
        let m = Metrics::default();
        assert_eq!(m.now_micros(), 0, "no source installed ⇒ deterministic 0");
        m.install_time_source(|| 42);
        assert_eq!(m.now_micros(), 42);
        m.install_time_source(|| 7); // second install is a no-op
        assert_eq!(m.now_micros(), 42);
    }

    #[test]
    fn micros_render_as_exact_decimal_seconds() {
        assert_eq!(micros_as_seconds(0), "0");
        assert_eq!(micros_as_seconds(250), "0.00025");
        assert_eq!(micros_as_seconds(1_500_000), "1.5");
        assert_eq!(micros_as_seconds(2_000_000), "2");
        assert_eq!(micros_as_seconds(60_200_050), "60.20005");
    }
}
