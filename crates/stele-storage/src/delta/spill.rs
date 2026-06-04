//! Local-disk spill for the delta tier.
//!
//! When the in-memory store exceeds `delta.spill_threshold_bytes`, the delta
//! freezes its current contents into a numbered spill file on the same
//! storage handle (`delta-spill-NNNN.row`) and clears the in-memory store.
//! Reads load each spill on demand and merge it with the in-memory store.
//!
//! ## What spill *is not*
//!
//! A spill file is **not durable** in the sense the WAL is durable. It is an
//! ephemeral memory-overflow mechanism whose only crash-recovery contract is
//! "the delta is rebuilt by WAL replay" ([STL-87 scope]). Spill files left
//! behind by a prior process are discarded on [`super::Delta::open`]; the
//! on-disk representation carries no version of its own and is therefore
//! allowed to change without a migration story.
//!
//! ## On-disk layout
//!
//! A spill file is a concatenation of [`Version`] frames in
//! `(business_key, sys_from)` order — the same encoding the WAL uses for a
//! delta-tier record (see [`super::version`]). The reader streams them with a
//! cursor; a short or oversized frame is reported as
//! [`DeltaError::Corrupt`], because a malformed spill file means we cannot
//! trust the post-flush read path until the delta is rebuilt from WAL.

use std::io;

use crate::backend::{Disk, DiskFile};

use super::version::Version;
use super::{DeltaError, SPILL_FILENAME_PREFIX};

/// Build the canonical spill filename for an index.
pub(super) fn spill_name(index: u64) -> String {
    // Width matches the WAL's segment naming so directory listings sort the
    // same way under `ls`.
    format!("{SPILL_FILENAME_PREFIX}{index:020}.row")
}

/// Parse an index back out of a spill filename. Returns `None` for anything
/// that doesn't match.
pub(super) fn index_of(name: &str) -> Option<u64> {
    let stem = name.strip_prefix(SPILL_FILENAME_PREFIX)?;
    let digits = stem.strip_suffix(".row")?;
    digits.parse().ok()
}

/// Write `rows` (already in `(business_key, sys_from)` order) to a new spill
/// file. `fsync` is invoked at the end *as a tidiness measure only* — there is
/// no durability claim on spill files. A failure here surfaces as
/// [`DeltaError::Io`].
pub(super) fn write_spill<D: Disk>(
    disk: &D,
    index: u64,
    rows: &[Version],
) -> Result<(), DeltaError> {
    let name = spill_name(index);
    let mut file = disk.create(&name)?;
    // Buffer the whole spill before writing so a partially-written file is
    // never observable mid-flush. Spills are bounded by `spill_threshold_bytes`
    // and we're already prepared to hold the data in memory.
    //
    // Sum as `u64` (not `usize`) so the addition can't overflow on a 32-bit
    // host even if `spill_threshold_bytes` is configured higher than 4 GiB,
    // then `try_from` to `usize` for the allocation. Mirrors the symmetric
    // guard in [`read_spill`].
    let total_u64: u64 = rows.iter().map(|r| r.encoded_size() as u64).sum();
    let total = usize::try_from(total_u64).map_err(|_| {
        DeltaError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("spill {name} buffer length {total_u64} exceeds usize"),
        ))
    })?;
    let mut buf = Vec::with_capacity(total);
    for row in rows {
        row.encode(&mut buf)?;
    }
    file.append(&buf)?;
    file.sync()?;
    Ok(())
}

/// Load every record from a spill file into memory, in stored order. Returns
/// [`DeltaError::Corrupt`] on a malformed frame; partial decodes are never
/// returned silently.
pub(super) fn read_spill<D: Disk>(disk: &D, index: u64) -> Result<Vec<Version>, DeltaError> {
    let name = spill_name(index);
    let file = disk.open(&name)?;
    let len = file.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    // Spill files are bounded by `spill_threshold_bytes`, well under usize::MAX
    // on any host we target — but use `try_from` so a corrupt 4 GiB file on a
    // 32-bit platform errors cleanly instead of truncating.
    let len_usize = usize::try_from(len).map_err(|_| {
        DeltaError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("spill {name} length {len} exceeds usize"),
        ))
    })?;
    let mut buf = vec![0u8; len_usize];
    let read = file.read_at(0, &mut buf)?;
    if read != buf.len() {
        return Err(DeltaError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("spill {name} short read: {read} of {}", buf.len()),
        )));
    }
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor < buf.len() {
        let (version, consumed) = Version::decode(&buf[cursor..])?;
        out.push(version);
        cursor += consumed;
    }
    Ok(out)
}

/// List every spill file currently on `disk`, sorted ascending by index.
pub(super) fn list_spills<D: Disk>(disk: &D) -> io::Result<Vec<u64>> {
    let mut indices: Vec<u64> = disk.list()?.iter().filter_map(|n| index_of(n)).collect();
    indices.sort_unstable();
    Ok(indices)
}

/// Delete every spill file currently on `disk`. Called by [`super::Delta::open`]
/// to drop stale state left by a prior (crashed) process — the canonical
/// truth is the WAL, never the spill.
pub(super) fn discard_stale_spills<D: Disk>(disk: &D) -> io::Result<()> {
    for idx in list_spills(disk)? {
        // A concurrent removal between `list` and `remove` would be a single
        // `NotFound`; we ignore it for the same reason a `rm -f` ignores it.
        match disk.remove(&spill_name(idx)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Remove a single spill file by index. Used after [`super::Delta::flush_to_segment`]
/// promotes spilled rows into a sealed segment.
pub(super) fn remove_spill<D: Disk>(disk: &D, index: u64) -> io::Result<()> {
    match disk.remove(&spill_name(index)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
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
        assert_eq!(index_of("wal-00000000000000000001.log"), None);
        assert_eq!(index_of(&format!("{SPILL_FILENAME_PREFIX}nan.row")), None);
    }
}
