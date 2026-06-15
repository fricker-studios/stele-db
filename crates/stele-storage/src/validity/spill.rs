//! Local-disk spill for the validity index.
//!
//! Mirrors the delta tier's spill ([`crate::delta`]): when the resident entries
//! exceed `ValidityConfig::spill_threshold_bytes`, the index freezes them into a
//! numbered file (`validity-spill-NNNN.row`) and clears memory; lookups merge
//! the resident entries with every spill. A spill file is **not durable** — the
//! WAL is the canonical truth and stale spills are discarded on
//! [`ValidityIndex::open`](super::ValidityIndex::open).
//!
//! On-disk layout: a concatenation of [`Close`] frames (the same encoding the
//! WAL uses for a close redo), streamed back with a cursor; a malformed frame is
//! [`ValidityError::Corrupt`].

use std::io;

use crate::backend::{Disk, DiskFile};
use crate::bloom::{DEFAULT_BITS_PER_KEY, KeyBloom};
use crate::delta::BusinessKey;

use super::index::{Close, ValidityError};

/// Filename prefix for validity-index spill files — distinct from the delta
/// tier's `delta-spill-` so the two never alias even if pointed at one disk.
const SPILL_FILENAME_PREFIX: &str = "validity-spill-";

/// In-memory summary of one live spill file, built when the spill freezes, so a
/// point / small-key lookup can skip a spill that cannot hold the key **without
/// reading it from disk** ([STL-142]).
///
/// It is deliberately *not* persisted: spills carry no durability claim and are
/// discarded on [`ValidityIndex::open`](super::ValidityIndex::open), so the
/// summary only ever describes a spill this process wrote and can be rebuilt for
/// free on the next replay. The two filters compose as a conservative
/// "might-contain" — a [`BusinessKey`] range (exact, since a spill is written in
/// `(business_key, sys_from)` order) gates the common disjoint case cheaply, and
/// a [`KeyBloom`] catches the overlapping-range case a full-snapshot spill
/// produces. Neither admits a false negative, so a `false` from
/// [`Self::may_contain`] is always a sound skip.
pub(super) struct SpillMeta {
    /// The spill's numeric index — the handle [`read_spill`] reads by.
    pub(super) index: u64,
    /// Least business key in the spill (inclusive).
    min_key: BusinessKey,
    /// Greatest business key in the spill (inclusive).
    max_key: BusinessKey,
    /// Membership filter over the spill's business keys.
    bloom: KeyBloom,
}

impl SpillMeta {
    /// Whether this spill could hold a close for `key`. Range first (a pair of
    /// comparisons), then the bloom — both must pass. Never a false negative.
    pub(super) fn may_contain(&self, key: &BusinessKey) -> bool {
        key >= &self.min_key && key <= &self.max_key && self.bloom.maybe_contains(key.as_bytes())
    }
}

/// Build the canonical spill filename for an index.
fn spill_name(index: u64) -> String {
    format!("{SPILL_FILENAME_PREFIX}{index:020}.row")
}

/// Parse an index back out of a spill filename. `None` for anything that doesn't
/// match.
fn index_of(name: &str) -> Option<u64> {
    let stem = name.strip_prefix(SPILL_FILENAME_PREFIX)?;
    let digits = stem.strip_suffix(".row")?;
    digits.parse().ok()
}

/// Write `closes` to a new spill file and return its in-memory [`SpillMeta`]
/// (key range + bloom) so later point lookups can skip it without a read. `sync`
/// is a tidiness measure only — spill files carry no durability claim.
///
/// `closes` must be non-empty: an empty spill carries no key range and the
/// caller ([`spill_in_memory`](super::ValidityIndex)) never freezes empty memory.
pub(super) fn write_spill<D: Disk>(
    disk: &D,
    index: u64,
    closes: &[Close],
) -> Result<SpillMeta, ValidityError> {
    let name = spill_name(index);
    let mut file = disk.create(&name)?;
    // Checked accumulation: a plain `.sum()` over `u64` wraps in release on a
    // pathologically large batch, which would under-size the buffer below and
    // defeat its own length check. Overflow is surfaced as a typed error instead.
    let total_u64 = closes
        .iter()
        .try_fold(0u64, |acc, c| acc.checked_add(c.encoded_size() as u64))
        .ok_or_else(|| {
            ValidityError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("validity spill {name} total encoded length overflows u64"),
            ))
        })?;
    let total = usize::try_from(total_u64).map_err(|_| {
        ValidityError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("validity spill {name} buffer length {total_u64} exceeds usize"),
        ))
    })?;
    let mut buf = Vec::with_capacity(total);
    for close in closes {
        close.encode(&mut buf)?;
    }
    file.append(&buf)?;
    file.sync()?;
    // `closes` is written in `(business_key, sys_from)` order (it is drained from
    // a `BTreeMap`), so the first/last keys are the exact range extremes.
    let min_key = closes
        .first()
        .expect("write_spill called with no closes")
        .business_key
        .clone();
    let max_key = closes
        .last()
        .expect("write_spill called with no closes")
        .business_key
        .clone();
    let bloom = KeyBloom::build(
        DEFAULT_BITS_PER_KEY,
        closes.iter().map(|c| c.business_key.as_bytes()),
    );
    Ok(SpillMeta {
        index,
        min_key,
        max_key,
        bloom,
    })
}

/// Load every [`Close`] from a spill file, in stored order.
pub(super) fn read_spill<D: Disk>(disk: &D, index: u64) -> Result<Vec<Close>, ValidityError> {
    let name = spill_name(index);
    let file = disk.open(&name)?;
    let len = file.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    let len_usize = usize::try_from(len).map_err(|_| {
        ValidityError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("validity spill {name} length {len} exceeds usize"),
        ))
    })?;
    let mut buf = vec![0u8; len_usize];
    let read = file.read_at(0, &mut buf)?;
    if read != buf.len() {
        return Err(ValidityError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("validity spill {name} short read: {read} of {}", buf.len()),
        )));
    }
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor < buf.len() {
        let (close, consumed) = Close::decode(&buf[cursor..])?;
        out.push(close);
        cursor += consumed;
    }
    Ok(out)
}

/// List every validity spill file on `disk`, ascending by index.
pub(super) fn list_spills<D: Disk>(disk: &D) -> io::Result<Vec<u64>> {
    let mut indices: Vec<u64> = disk.list()?.iter().filter_map(|n| index_of(n)).collect();
    indices.sort_unstable();
    Ok(indices)
}

/// Delete every validity spill on `disk` — called by
/// [`ValidityIndex::open`](super::ValidityIndex::open) to drop stale state left
/// by a prior (crashed) process. The canonical truth is the WAL.
pub(super) fn discard_stale_spills<D: Disk>(disk: &D) -> io::Result<()> {
    for idx in list_spills(disk)? {
        match disk.remove(&spill_name(idx)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stele_common::provenance::{Principal, Provenance, TxnId};
    use stele_common::time::SystemTimeMicros;

    fn close(key: &[u8], sys_from: i64) -> Close {
        Close {
            business_key: BusinessKey::new(key.to_vec()),
            sys_from: SystemTimeMicros(sys_from),
            seq: 0,
            sys_to: SystemTimeMicros(sys_from + 1),
            closed_by: Provenance::new(
                TxnId(1),
                SystemTimeMicros(sys_from),
                Principal::new(b"p".to_vec()),
            ),
        }
    }

    #[test]
    fn meta_may_contain_combines_range_and_bloom() {
        // A spill holding only the extremes of a wide range. Members pass; a key
        // outside the range is always pruned by the range; and across many absent
        // keys *inside* the range, the bloom prunes at least one (existential, to
        // avoid coupling to a specific key the bloom might false-positive on).
        let meta = {
            let closes = [close(b"k000", 1), close(b"k999", 1)];
            // write_spill needs a disk; build the meta directly from its parts.
            let bloom = KeyBloom::build(
                DEFAULT_BITS_PER_KEY,
                closes.iter().map(|c| c.business_key.as_bytes()),
            );
            SpillMeta {
                index: 0,
                min_key: closes[0].business_key.clone(),
                max_key: closes[1].business_key.clone(),
                bloom,
            }
        };
        assert!(
            meta.may_contain(&BusinessKey::new(b"k000".to_vec())),
            "member"
        );
        assert!(
            meta.may_contain(&BusinessKey::new(b"k999".to_vec())),
            "member"
        );
        assert!(
            !meta.may_contain(&BusinessKey::new(b"z99".to_vec())),
            "out of range → range prunes",
        );
        let in_range_pruned = (0..1000)
            .map(|i| BusinessKey::new(format!("k{i:03}").into_bytes()))
            .filter(|k| !meta.may_contain(k))
            .count();
        assert!(
            in_range_pruned > 0,
            "in range, absent → the bloom prunes at least one",
        );
    }

    #[test]
    fn name_round_trip() {
        let n = spill_name(7);
        assert_eq!(
            n,
            format!("{SPILL_FILENAME_PREFIX}00000000000000000007.row")
        );
        assert_eq!(index_of(&n), Some(7));
        assert_eq!(index_of("delta-spill-00000000000000000001.row"), None);
        assert_eq!(index_of(&format!("{SPILL_FILENAME_PREFIX}nan.row")), None);
    }
}
