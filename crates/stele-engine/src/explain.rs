//! `EXPLAIN [ANALYZE]` — the query-plan tree and its rendering ([STL-260]).
//!
//! `EXPLAIN` renders the bound plan as an indented operator tree; `EXPLAIN
//! ANALYZE` executes the statement for real and annotates each operator with its
//! true row count and wall time. Both render to the Postgres convention — one
//! text column named `QUERY PLAN`, one row per line — so `psql` (and the Stele
//! shell) display it natively with no special-casing ([SessionEngine::execute]
//! returns it as an ordinary [`StatementOutcome::Rows`](crate::StatementOutcome)).
//!
//! This module owns the *structure and rendering* only: a [`PlanNode`] is a label,
//! a set of attribute detail lines, optional measured actuals, and children. The
//! engine ([`crate`]) assembles the tree — it is the one place with the catalog,
//! the resolved snapshot, and the schema needed to name the chosen index, the
//! prune push-down, and the projected columns — and (under `ANALYZE`) fills the
//! [`Measured`] actuals from the [`Profiler`] it installs around execution.
//!
//! ## Timing and determinism
//!
//! Per-operator wall time is measured engine-side via the metric registry's
//! installed time source ([`Metrics::now_micros`](stele_common::metrics::Metrics::now_micros)),
//! never inside `stele-exec`: the operator pipeline runs under the deterministic
//! simulation scheduler and must stay clock-free
//! ([ADR-0027](../../../docs/adr/0027-vectorized-execution-model.md), architecture
//! invariant 7). Tests and the simulator leave the registry sourceless, so every
//! measured time is `0us` there — which makes `EXPLAIN ANALYZE` output
//! deterministic under test while still reporting real microseconds on the
//! production server.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write as _;

use stele_common::metrics::SharedMetrics;
use stele_exec::{Batch, Operator, ScanError, ScanStats};

/// The single output column name an `EXPLAIN` result carries, per the Postgres
/// convention `psql` renders natively.
pub(crate) const QUERY_PLAN_COLUMN: &str = "QUERY PLAN";

/// Which operator a [`Profiler`] measurement belongs to. Each plan has at most
/// one of most kinds, so the tag is enough to reunite a measurement with the
/// [`PlanNode`] the engine builds for it; a join's per-side scans are
/// distinguished by their position in the left-deep chain.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OpTag {
    /// The single-table snapshot scan source.
    Scan,
    /// The payload→value-columns explode above the scan.
    Explode,
    /// The vectorized `WHERE` filter above the explode.
    Filter,
    /// A correlated-subquery `WHERE`, applied row-by-row in the engine tail.
    SubqueryFilter,
    /// The hash aggregate / `GROUP BY`.
    Aggregate,
    /// The projection + result-shaping (`DISTINCT` / `ORDER BY` / `LIMIT`) tail.
    Project,
    /// The hash join: the whole left-deep chain's combine cost and its combined
    /// per-input scan accounting. The individual input scans are shown structurally
    /// under it (a per-side row/time split is a tracked follow-up).
    Join,
    /// A source that is not the single-table operator pipeline — a temporal range
    /// scan, an overlay (read-your-own-writes) read, or a materialized CTE.
    Source,
}

/// Accumulated actuals for one operator across an `EXPLAIN ANALYZE` run.
#[derive(Default)]
struct Acc {
    rows: u64,
    time_us: u64,
    scan: Option<ScanStats>,
}

/// The per-operator measurement sink installed around an `EXPLAIN ANALYZE`
/// execution ([STL-260]).
///
/// Lives behind a [`RefCell`] on the engine (like `index_probes`' [`Cell`]), so
/// the read path's `&self` methods record without a new `&mut`. Disabled by
/// default — a non-analyzed read records nothing and the operator pipeline is
/// built without the measuring [`Probe`], so it pays no overhead.
#[derive(Default)]
pub(crate) struct Profiler {
    enabled: bool,
    ops: BTreeMap<OpTag, Acc>,
}

impl Profiler {
    /// Arm the sink for one `EXPLAIN ANALYZE` run, clearing any prior state.
    pub(crate) fn enable(&mut self) {
        self.enabled = true;
        self.ops.clear();
    }

    /// Disarm the sink once the run's tree has been read back.
    pub(crate) fn disable(&mut self) {
        self.enabled = false;
        self.ops.clear();
    }

    /// Whether the read path should wrap operators in a [`Probe`] and record.
    pub(crate) const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Add a measurement for `tag` — additive in both rows and time, so the
    /// per-batch [`Probe`] and the one-shot engine-tail stages record alike.
    pub(crate) fn add(&mut self, tag: OpTag, rows: u64, time_us: u64) {
        let acc = self.ops.entry(tag).or_default();
        acc.rows += rows;
        acc.time_us += time_us;
    }

    /// Attach a scan node's pruning accounting (set once, after the drain).
    pub(crate) fn set_scan(&mut self, tag: OpTag, scan: ScanStats) {
        self.ops.entry(tag).or_default().scan = Some(scan);
    }

    /// The [`Measured`] actuals recorded for `tag`, or `None` if that operator
    /// did not run (so its [`PlanNode`] renders structurally, without actuals).
    pub(crate) fn measured(&self, tag: OpTag) -> Option<Measured> {
        self.ops.get(&tag).map(|a| Measured {
            rows: a.rows,
            time_us: a.time_us,
            scan: a.scan,
        })
    }
}

/// A measuring operator decorator ([STL-260]): times each `next` of its child and
/// attributes the elapsed wall time and emitted rows to `tag` in the shared
/// [`Profiler`].
///
/// Engine-side, so the wall-clock read (the metric registry's installed time
/// source, [`SharedMetrics::now_micros`]) stays out of `stele-exec`, which runs
/// under the deterministic simulation scheduler. The time is **inclusive** of the
/// child's inputs (the pull model nests `next` calls), matching Postgres's
/// per-node "actual time"; the engine also reports a whole-statement
/// `Execution Time` footer for the true total.
pub(crate) struct Probe<'a, C: Operator> {
    child: C,
    tag: OpTag,
    profiler: &'a RefCell<Profiler>,
    metrics: &'a SharedMetrics,
}

impl<'a, C: Operator> Probe<'a, C> {
    /// Wrap `child`, attributing its work to `tag`.
    pub(crate) const fn new(
        child: C,
        tag: OpTag,
        profiler: &'a RefCell<Profiler>,
        metrics: &'a SharedMetrics,
    ) -> Self {
        Self {
            child,
            tag,
            profiler,
            metrics,
        }
    }
}

impl<C: Operator> Operator for Probe<'_, C> {
    fn next(&mut self) -> Result<Option<Batch>, ScanError> {
        let t0 = self.metrics.now_micros();
        let batch = self.child.next()?;
        let elapsed = self.metrics.now_micros().saturating_sub(t0);
        let rows = batch.as_ref().map_or(0, |b| b.rows as u64);
        self.profiler.borrow_mut().add(self.tag, rows, elapsed);
        Ok(batch)
    }

    fn stats(&self) -> Option<ScanStats> {
        self.child.stats()
    }
}

/// One node in a rendered query plan: an operator label, its attribute detail
/// lines, the measured actuals (`EXPLAIN ANALYZE` only), and its child operators.
pub(crate) struct PlanNode {
    /// The operator label, e.g. `"Snapshot Scan on account"`, `"Filter"`,
    /// `"Hash Join (inner)"`.
    label: String,
    /// Attribute lines shown indented under the label — `"AS OF system 123"`,
    /// `"Index: i_balance (btree, =)"`, `"Group Key: region"`, … Static facts of
    /// the bound plan, present for a bare `EXPLAIN` too.
    details: Vec<String>,
    /// The measured actuals, present only under `EXPLAIN ANALYZE`.
    measured: Option<Measured>,
    /// Child operators, rendered indented below this node (consumer-on-top: a
    /// node's children are the inputs it pulls from).
    children: Vec<PlanNode>,
}

/// Per-node actuals captured by the [`Profiler`] under `EXPLAIN ANALYZE`.
#[derive(Clone, Copy, Default)]
pub(crate) struct Measured {
    /// Rows this operator emitted to its consumer.
    pub rows: u64,
    /// Wall time spent in this operator (inclusive of its children, like
    /// Postgres), in microseconds — `0` whenever the metric registry has no time
    /// source installed (tests / the simulator).
    pub time_us: u64,
    /// The scan's pruning accounting, attached to a scan node only ([STL-146] /
    /// [STL-173]); `None` on every non-scan node.
    pub scan: Option<ScanStats>,
}

impl PlanNode {
    /// A leaf operator (no children).
    pub(crate) fn leaf(label: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            label: label.into(),
            details,
            measured: None,
            children: Vec::new(),
        }
    }

    /// An operator with one input.
    pub(crate) fn unary(label: impl Into<String>, details: Vec<String>, child: Self) -> Self {
        Self {
            label: label.into(),
            details,
            measured: None,
            children: vec![child],
        }
    }

    /// An operator with several inputs (a join).
    pub(crate) fn nary(
        label: impl Into<String>,
        details: Vec<String>,
        children: Vec<Self>,
    ) -> Self {
        Self {
            label: label.into(),
            details,
            measured: None,
            children,
        }
    }

    /// Attach measured actuals (`EXPLAIN ANALYZE`); a no-op given `None`, so a
    /// bare `EXPLAIN` call site can pass its (absent) measurement uniformly.
    #[must_use]
    pub(crate) const fn with_measured(mut self, measured: Option<Measured>) -> Self {
        self.measured = measured;
        self
    }

    /// Append one attribute line to this node (e.g. the root's `Output:` list).
    pub(crate) fn push_detail(&mut self, detail: String) {
        self.details.push(detail);
    }

    /// Set the measured actuals only if none are present — the fallback the engine
    /// uses to give a DML root (which runs no measured operator) its headline row
    /// count and the statement's total time.
    pub(crate) const fn set_measured_if_absent(&mut self, measured: Measured) {
        if self.measured.is_none() {
            self.measured = Some(measured);
        }
    }

    /// Render the tree to `QUERY PLAN` lines — one string per output row.
    ///
    /// `analyze` gates the measured suffix: under `EXPLAIN ANALYZE` each operator
    /// line ends with `(actual rows=N time=Xus)` and a scan node gains a prune
    /// accounting line; a bare `EXPLAIN` shows the static plan only.
    pub(crate) fn render(&self, analyze: bool) -> Vec<String> {
        let mut out = Vec::new();
        // The root prints flush-left; its details and children indent two spaces.
        self.render_into("", "  ", analyze, &mut out);
        out
    }

    /// Render this node at `head_pad` (the indentation before its label), with
    /// `child_pad` the indentation for its detail lines and the base for its
    /// children's `->` arrows.
    ///
    /// Consumer-on-top: a child's arrow sits at the parent's `child_pad`, so it
    /// lines up directly under the parent's detail lines, and the child's own
    /// details indent two more under its label.
    fn render_into(&self, head_pad: &str, child_pad: &str, analyze: bool, out: &mut Vec<String>) {
        let mut head = format!("{head_pad}{}", self.label);
        if analyze && let Some(m) = &self.measured {
            let _ = write!(head, "  (actual rows={} time={}us)", m.rows, m.time_us);
        }
        out.push(head);

        for detail in &self.details {
            out.push(format!("{child_pad}{detail}"));
        }
        // The scan's prune accounting is an actual, so it rides the ANALYZE path.
        if analyze
            && let Some(m) = &self.measured
            && let Some(scan) = &m.scan
        {
            out.push(format!("{child_pad}{}", render_scan_stats(scan)));
        }

        let arrow_pad = format!("{child_pad}->  ");
        let grandchild_pad = format!("{child_pad}      ");
        for child in &self.children {
            child.render_into(&arrow_pad, &grandchild_pad, analyze, out);
        }
    }
}

/// The one-line prune accounting shown under a scan node by `EXPLAIN ANALYZE`:
/// how many segments and row-groups were scanned versus skipped, by which proof.
fn render_scan_stats(s: &ScanStats) -> String {
    format!(
        "Buffers: segments {scanned}/{total} scanned \
         (pruned {zone} zone, {bloom} bloom, {sup} superseded, {valid} valid); \
         row-groups {rg_scanned}/{rg_total} scanned \
         (pruned {rg_zone} zone, {rg_valid} valid)",
        scanned = s.segments_scanned,
        total = s.segments_total,
        zone = s.segments_pruned_zone,
        bloom = s.segments_pruned_bloom,
        sup = s.segments_pruned_superseded,
        valid = s.segments_pruned_valid,
        rg_scanned = s.row_groups_scanned,
        rg_total = s.row_groups_total,
        rg_zone = s.row_groups_pruned_zone,
        rg_valid = s.row_groups_pruned_valid,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_a_nested_tree_without_analyze() {
        let scan = PlanNode::leaf(
            "Snapshot Scan on account",
            vec![
                "AS OF system 100".to_owned(),
                "Index: i_balance (btree, =)".to_owned(),
            ],
        );
        let filter = PlanNode::unary("Filter", vec!["Cond: balance > 100".to_owned()], scan);
        let root = PlanNode::unary("Aggregate", vec!["Group Key: region".to_owned()], filter);

        let lines = root.render(false);
        assert_eq!(
            lines,
            vec![
                "Aggregate".to_owned(),
                "  Group Key: region".to_owned(),
                "  ->  Filter".to_owned(),
                "        Cond: balance > 100".to_owned(),
                "        ->  Snapshot Scan on account".to_owned(),
                "              AS OF system 100".to_owned(),
                "              Index: i_balance (btree, =)".to_owned(),
            ]
        );
    }

    #[test]
    fn analyze_appends_actuals_and_buffers() {
        let scan = PlanNode::leaf("Snapshot Scan on t", vec![]).with_measured(Some(Measured {
            rows: 100,
            time_us: 0,
            scan: Some(ScanStats {
                segments_total: 3,
                segments_scanned: 2,
                segments_pruned_zone: 1,
                row_groups_total: 5,
                row_groups_scanned: 4,
                row_groups_pruned_zone: 1,
                ..ScanStats::default()
            }),
        }));
        let root = PlanNode::unary("Filter", vec![], scan).with_measured(Some(Measured {
            rows: 50,
            time_us: 0,
            scan: None,
        }));

        let lines = root.render(true);
        assert_eq!(lines[0], "Filter  (actual rows=50 time=0us)");
        assert_eq!(
            lines[1],
            "  ->  Snapshot Scan on t  (actual rows=100 time=0us)"
        );
        assert!(
            lines[2].contains("segments 2/3 scanned"),
            "scan node carries a buffers line: {:?}",
            lines[2]
        );
        // A non-scan node carries no buffers line.
        assert_eq!(lines.len(), 3);
    }
}
