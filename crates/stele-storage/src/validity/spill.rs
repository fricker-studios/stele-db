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

use super::index::{Close, ValidityError};

/// Filename prefix for validity-index spill files — distinct from the delta
/// tier's `delta-spill-` so the two never alias even if pointed at one disk.
const SPILL_FILENAME_PREFIX: &str = "validity-spill-";

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

/// Write `closes` to a new spill file. `sync` is a tidiness measure only — spill
/// files carry no durability claim.
pub(super) fn write_spill<D: Disk>(
    disk: &D,
    index: u64,
    closes: &[Close],
) -> Result<(), ValidityError> {
    let name = spill_name(index);
    let mut file = disk.create(&name)?;
    let total_u64: u64 = closes.iter().map(|c| c.encoded_size() as u64).sum();
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
    Ok(())
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
