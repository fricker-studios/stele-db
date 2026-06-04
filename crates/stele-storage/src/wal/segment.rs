//! Segment file naming.
//!
//! Segments are numbered monotonically from 0 and named `wal-{idx:020}.log` so
//! they sort lexicographically the same way they sort numerically.

/// Width of the zero-padded index in a segment filename.
const INDEX_WIDTH: usize = 20;

const PREFIX: &str = "wal-";
const SUFFIX: &str = ".log";

/// Build the segment filename for a given index.
pub(crate) fn name_for(index: u64) -> String {
    format!("{PREFIX}{index:0INDEX_WIDTH$}{SUFFIX}")
}

/// Parse a segment filename into its index. Returns `None` for any name that
/// doesn't match the expected shape — useful for skipping unrelated files in a
/// shared directory.
pub(crate) fn index_of(name: &str) -> Option<u64> {
    let stem = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    if stem.len() != INDEX_WIDTH {
        return None;
    }
    stem.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_round_trip() {
        for idx in [0u64, 1, 42, u64::MAX] {
            let name = name_for(idx);
            assert_eq!(index_of(&name), Some(idx), "round-trip for {idx}");
        }
    }

    #[test]
    fn lexicographic_matches_numeric() {
        let a = name_for(2);
        let b = name_for(10);
        assert!(a < b, "{a} should sort before {b}");
    }

    #[test]
    fn rejects_foreign_names() {
        assert_eq!(index_of("readme.txt"), None);
        assert_eq!(index_of("wal-.log"), None);
        assert_eq!(index_of("wal-abc.log"), None);
    }
}
