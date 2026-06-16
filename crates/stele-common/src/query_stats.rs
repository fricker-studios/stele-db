//! Per-query execution stats ([STL-201]) — the "see the engine" query-stats
//! footer's wire contract, defined once.
//!
//! One [`QueryStats`] is produced by the engine (folded from a scan's
//! `ScanStats`, STL-146), serialized into a `NoticeResponse` message field by the
//! pg-wire front end ([`QueryStats::to_notice`]), and parsed back by the
//! `stele shell` wire client ([`QueryStats::parse_notice`]) — so the encode and
//! decode sides can never drift.
//!
//! The trailer is **opt-in**: the server only emits it to a connection that asked
//! for it (the `stele shell` sets a startup parameter; no other client does), so
//! psql and the JDBC / psycopg driver gate never see it. A client that receives a
//! notice it does not recognize ignores it, and [`parse_notice`](QueryStats::parse_notice)
//! returns `None` for any notice whose message is not a stats line, so an
//! unrelated `NOTICE` is never misread as stats.
//!
//! [STL-201]: https://allegromusic.atlassian.net/browse/STL-201

/// The leading tag of a stats `NoticeResponse` message. Versioned so a future
/// shape change is recognizable; a parser keyed on it ignores every other notice.
pub const NOTICE_PREFIX: &str = "stele stats v1:";

/// One query's execution accounting, as carried over the wire to the shell.
///
/// The scan-level counts mirror the executor's `ScanStats` (STL-146): a segment
/// is either *scanned* (its columns materialized) or *pruned* by one of three
/// proofs — a zone map, a footer bloom, or the validity index (superseded). The
/// row-group counts (STL-173) partition the row-groups of the segment-level zone
/// survivors. `rows` is the count the query actually returned (post-filter,
/// post-aggregate), not the number of versions examined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryStats {
    /// Rows the query returned to the client.
    pub rows: u64,
    /// The resolved system-time snapshot the read ran at, microseconds since the
    /// Unix epoch.
    pub system_snapshot: i64,
    /// Whether the read time-traveled the system axis — a `FOR SYSTEM_TIME AS OF`
    /// qualifier was given (so the footer reads "snapshot @ …" rather than
    /// "live @ now()").
    pub time_travel: bool,
    /// Sealed segments offered to the scan.
    pub segments_total: u64,
    /// Segments whose columns were materialized.
    pub segments_scanned: u64,
    /// Segments a zone map proved hold no visible match (no read I/O).
    pub segments_pruned_zone: u64,
    /// Segments a footer bloom proved hold the probed key in no version.
    pub segments_pruned_bloom: u64,
    /// Segments the validity index proved wholly superseded at the snapshot.
    pub segments_pruned_superseded: u64,
    /// Row-groups across the segment-level zone survivors (the denominator the two
    /// row-group counts below partition).
    pub row_groups_total: u64,
    /// Row-groups whose identity columns were read to resolve the snapshot.
    pub row_groups_scanned: u64,
    /// Row-groups a per-row-group zone map proved hold no visible match.
    pub row_groups_pruned_zone: u64,
}

impl QueryStats {
    /// Segments skipped by any prune — never had their bulk columns materialized.
    #[must_use]
    pub const fn segments_pruned(&self) -> u64 {
        self.segments_pruned_zone + self.segments_pruned_bloom + self.segments_pruned_superseded
    }

    /// Serialize as the body of a stats `NoticeResponse` message: the
    /// [`NOTICE_PREFIX`] tag followed by space-separated `key=value` pairs.
    ///
    /// The format is intentionally human-legible (a raw client that opts in sees a
    /// readable `NOTICE`) and trivially parseable ([`parse_notice`](Self::parse_notice)).
    #[must_use]
    pub fn to_notice(&self) -> String {
        format!(
            "{NOTICE_PREFIX} rows={} sys={} tt={} \
             seg_total={} seg_scanned={} seg_zone={} seg_bloom={} seg_super={} \
             rg_total={} rg_scanned={} rg_zone={}",
            self.rows,
            self.system_snapshot,
            u8::from(self.time_travel),
            self.segments_total,
            self.segments_scanned,
            self.segments_pruned_zone,
            self.segments_pruned_bloom,
            self.segments_pruned_superseded,
            self.row_groups_total,
            self.row_groups_scanned,
            self.row_groups_pruned_zone,
        )
    }

    /// Parse a stats line produced by [`to_notice`](Self::to_notice).
    ///
    /// Returns `None` when `message` is not a stats notice (it does not begin with
    /// [`NOTICE_PREFIX`]), so an unrelated `NOTICE` is never misread. A recognized
    /// line is parsed leniently: an unknown key is ignored (forward-compatible), a
    /// missing key keeps its zero default, and an unparsable value is treated as
    /// zero rather than failing the whole footer.
    #[must_use]
    pub fn parse_notice(message: &str) -> Option<Self> {
        let body = message.trim().strip_prefix(NOTICE_PREFIX)?;
        let mut stats = Self::default();
        for token in body.split_whitespace() {
            let Some((key, value)) = token.split_once('=') else {
                continue;
            };
            match key {
                "rows" => stats.rows = value.parse().unwrap_or(0),
                "sys" => stats.system_snapshot = value.parse().unwrap_or(0),
                "tt" => stats.time_travel = value == "1",
                "seg_total" => stats.segments_total = value.parse().unwrap_or(0),
                "seg_scanned" => stats.segments_scanned = value.parse().unwrap_or(0),
                "seg_zone" => stats.segments_pruned_zone = value.parse().unwrap_or(0),
                "seg_bloom" => stats.segments_pruned_bloom = value.parse().unwrap_or(0),
                "seg_super" => stats.segments_pruned_superseded = value.parse().unwrap_or(0),
                "rg_total" => stats.row_groups_total = value.parse().unwrap_or(0),
                "rg_scanned" => stats.row_groups_scanned = value.parse().unwrap_or(0),
                "rg_zone" => stats.row_groups_pruned_zone = value.parse().unwrap_or(0),
                _ => {}
            }
        }
        Some(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> QueryStats {
        QueryStats {
            rows: 3,
            system_snapshot: 1_718_553_600_000_000,
            time_travel: true,
            segments_total: 12,
            segments_scanned: 3,
            segments_pruned_zone: 4,
            segments_pruned_bloom: 1,
            segments_pruned_superseded: 0,
            row_groups_total: 20,
            row_groups_scanned: 15,
            row_groups_pruned_zone: 5,
        }
    }

    #[test]
    fn notice_round_trips_every_field() {
        let stats = sample();
        let parsed = QueryStats::parse_notice(&stats.to_notice()).expect("a stats notice");
        assert_eq!(parsed, stats);
    }

    #[test]
    fn live_read_round_trips_with_tt_zero() {
        let stats = QueryStats {
            time_travel: false,
            ..sample()
        };
        let line = stats.to_notice();
        assert!(line.contains("tt=0"), "{line}");
        assert_eq!(QueryStats::parse_notice(&line), Some(stats));
    }

    #[test]
    fn a_non_stats_notice_is_not_parsed() {
        assert_eq!(
            QueryStats::parse_notice("database \"stele\" does not exist"),
            None
        );
    }

    #[test]
    fn unknown_keys_are_ignored_and_missing_keys_default_to_zero() {
        // Forward-compatibility: a newer server adds a key, drops one we expect.
        let line = format!("{NOTICE_PREFIX} rows=7 seg_future=99 seg_total=2");
        let parsed = QueryStats::parse_notice(&line).expect("recognized prefix");
        assert_eq!(parsed.rows, 7);
        assert_eq!(parsed.segments_total, 2);
        assert_eq!(parsed.segments_scanned, 0); // missing → default
    }

    #[test]
    fn segments_pruned_sums_the_three_proofs() {
        assert_eq!(sample().segments_pruned(), 5);
    }
}
