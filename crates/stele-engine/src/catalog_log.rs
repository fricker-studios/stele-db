//! The durable session **catalog log** ([ADR-0028], [STL-210]).
//!
//! The catalog — which tables exist, their versioned schemas, the valid-time
//! policy, and each table's on-disk namespace — lives in [`Catalog`] memory;
//! this module is what makes it survive a restart. Every acknowledged DDL
//! mutation appends one self-checksummed record to `stele.catalog` on the
//! session's **shared** (un-namespaced) disk and fsyncs it *before* the
//! statement returns: the catalog log fsync is the durability point for DDL,
//! exactly the invariant-2 shape the row WAL gives DML. On boot,
//! [`SessionEngine::recover`](crate::SessionEngine::recover) [`replay`]s the
//! records in order — at their recorded system-time instants — which
//! reproduces the schema-version chains and the `SchemaId` allocation order
//! exactly, and tells recovery which per-table namespaces to reopen.
//!
//! ## Framing and the torn-tail contract
//!
//! Each record is `magic(4) | payload_len(4 LE) | payload | crc32c(4 LE)`
//! (the CRC covers magic + length + payload). [`replay`] validates records in
//! sequence and distinguishes two failure shapes:
//!
//! * a **torn tail** — the file ends mid-record, or the next bytes do not
//!   begin with the magic (a crashed append's partial frame, or the zero/
//!   garbage fill some filesystems leave past the last durable write). The
//!   record's fsync never returned, so the DDL was never acknowledged;
//!   replay stops cleanly and the prior records stand.
//! * **corruption** — a *complete* record with intact magic whose CRC fails.
//!   That record was acknowledged, so serving without it would silently
//!   change the table set; replay fails closed instead
//!   (mirrors how committed-segment corruption is refused on boot).
//!
//! The log is **authoritative for DDL** — unlike the validity index it is not
//! derived from anything else — and append-only: records are never rewritten,
//! and a `DROP TABLE` is a new record, not an erasure.
//!
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
//! [STL-210]: https://allegromusic.atlassian.net/browse/STL-210
//! [`Catalog`]: stele_catalog::Catalog

use std::io;

use stele_catalog::{ColumnDef, TableTemporal, ValidTimeSpec};
use stele_common::time::SystemTimeMicros;
use stele_common::types::LogicalType;
use stele_storage::backend::{Disk, DiskFile};
use stele_storage::checksum::crc32c;

/// The canonical catalog-log filename on the session's shared disk. A single
/// normal path component; per-table files all carry a `t{idx:020}-` namespace
/// prefix ([`NamespacedDisk`](crate::NamespacedDisk)), so the bare name can
/// never collide with (or leak into) a table's tier.
pub(crate) const CATALOG_LOG_FILENAME: &str = "stele.catalog";

/// Four-byte record magic — `b"STCG"` (STele CataloG). Distinguishes a record
/// from a torn/zero-filled tail and is folded into the CRC.
const MAGIC: [u8; 4] = *b"STCG";

/// Bytes before the payload: magic + payload length.
const HEADER_LEN: usize = 8;

/// Bytes of the trailing CRC32C.
const CRC_LEN: usize = 4;

/// One durable DDL mutation — the unit [`append`] writes and [`replay`]
/// returns. Mirrors the [`Catalog`](stele_catalog::Catalog) mutations the SQL
/// surface can produce (`ALTER` gets its kind when it becomes SQL-reachable;
/// the kind byte is reserved for it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CatalogRecord {
    /// A `CREATE TABLE` took effect at `at` — including the re-creation of a
    /// previously dropped name.
    CreateTable {
        /// The system time the creation took effect (the catalog version's
        /// `sys_from`).
        at: SystemTimeMicros,
        /// The table's on-disk namespace index — which `t{idx:020}-` slice of
        /// the shared disk its tiers live on. A re-created name records the
        /// *same* namespace as its prior life (the tier is reused, so retained
        /// history is neither duplicated nor orphaned); recovery reopens
        /// exactly the recorded namespaces and resumes allocation past them.
        namespace: u64,
        /// The table name.
        name: String,
        /// The columns, in declaration order.
        columns: Vec<ColumnDef>,
        /// The temporal configuration (system-only, or + valid-time period).
        temporal: TableTemporal,
    },
    /// A `DROP TABLE` took effect at `at` (a catalog version transition — the
    /// table's history and tier remain).
    DropTable {
        /// The system time the drop took effect.
        at: SystemTimeMicros,
        /// The dropped table's name.
        name: String,
    },
}

/// Record-kind discriminants. `0` is deliberately unused so a zero-filled
/// region can never decode as a record even if its CRC were somehow valid.
const KIND_CREATE_TABLE: u8 = 1;
const KIND_DROP_TABLE: u8 = 2;

/// Map a [`LogicalType`] to its stable on-log tag. Exhaustive: adding a
/// variant to [`LogicalType`] fails compilation here, forcing a conscious tag
/// assignment (tags are append-only; never renumber).
const fn type_tag(ty: LogicalType) -> u8 {
    match ty {
        LogicalType::Int4 => 1,
        LogicalType::Int8 => 2,
        LogicalType::Text => 3,
        LogicalType::Bool => 4,
        LogicalType::Timestamp => 5,
        LogicalType::TimestampTz => 6,
        LogicalType::Date => 7,
        LogicalType::Period => 8,
        LogicalType::Uuid => 9,
        LogicalType::Bytea => 10,
        LogicalType::Float8 => 11,
    }
}

/// The inverse of [`type_tag`], or [`None`] for a tag this build does not
/// know (a log written by a newer build).
const fn type_from_tag(tag: u8) -> Option<LogicalType> {
    Some(match tag {
        1 => LogicalType::Int4,
        2 => LogicalType::Int8,
        3 => LogicalType::Text,
        4 => LogicalType::Bool,
        5 => LogicalType::Timestamp,
        6 => LogicalType::TimestampTz,
        7 => LogicalType::Date,
        8 => LogicalType::Period,
        9 => LogicalType::Uuid,
        10 => LogicalType::Bytea,
        11 => LogicalType::Float8,
        _ => return None,
    })
}

/// `InvalidData` with a context message — the shape every decode failure maps
/// to, so callers can surface one coherent "catalog log corrupt" error.
fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Append a length-prefixed UTF-8 string (`u16 LE` length + bytes).
fn put_str(buf: &mut Vec<u8>, s: &str) -> io::Result<()> {
    let len = u16::try_from(s.len()).map_err(|_| {
        corrupt(format!(
            "identifier too long for the catalog log ({})",
            s.len()
        ))
    })?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

/// Encode one record's payload (everything between the header and the CRC).
fn encode_payload(record: &CatalogRecord) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    match record {
        CatalogRecord::CreateTable {
            at,
            namespace,
            name,
            columns,
            temporal,
        } => {
            buf.push(KIND_CREATE_TABLE);
            buf.extend_from_slice(&at.0.to_le_bytes());
            buf.extend_from_slice(&namespace.to_le_bytes());
            put_str(&mut buf, name)?;
            let count = u16::try_from(columns.len())
                .map_err(|_| corrupt(format!("too many columns ({})", columns.len())))?;
            buf.extend_from_slice(&count.to_le_bytes());
            for col in columns {
                put_str(&mut buf, col.name())?;
                buf.push(type_tag(col.ty()));
            }
            match temporal.valid_time() {
                None => buf.push(0),
                Some(spec) => {
                    buf.push(1);
                    put_str(&mut buf, spec.from_column())?;
                    put_str(&mut buf, spec.to_column())?;
                }
            }
        }
        CatalogRecord::DropTable { at, name } => {
            buf.push(KIND_DROP_TABLE);
            buf.extend_from_slice(&at.0.to_le_bytes());
            put_str(&mut buf, name)?;
        }
    }
    Ok(buf)
}

/// Encode one record as a complete frame: header, payload, trailing CRC.
fn encode_frame(record: &CatalogRecord) -> io::Result<Vec<u8>> {
    let payload = encode_payload(record)?;
    let len = u32::try_from(payload.len()).map_err(|_| corrupt("record payload too large"))?;
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len() + CRC_LEN);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&payload);
    let crc = crc32c(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    Ok(frame)
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// A sequential reader over one record's payload bytes.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| corrupt("record payload truncated"))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn string(&mut self) -> io::Result<String> {
        let len = usize::from(self.u16()?);
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("identifier is not UTF-8"))
    }

    const fn finished(&self) -> bool {
        self.at == self.bytes.len()
    }
}

/// Decode one CRC-verified payload back into a [`CatalogRecord`].
fn decode_payload(payload: &[u8]) -> io::Result<CatalogRecord> {
    let mut cur = Cursor::new(payload);
    let kind = cur.u8()?;
    let record = match kind {
        KIND_CREATE_TABLE => {
            let at = SystemTimeMicros(cur.i64()?);
            let namespace = cur.u64()?;
            let name = cur.string()?;
            let count = usize::from(cur.u16()?);
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                let col_name = cur.string()?;
                let tag = cur.u8()?;
                let ty = type_from_tag(tag)
                    .ok_or_else(|| corrupt(format!("unknown column type tag {tag}")))?;
                columns.push(
                    ColumnDef::new(col_name, ty).map_err(|e| corrupt(format!("column: {e}")))?,
                );
            }
            let temporal = match cur.u8()? {
                0 => TableTemporal::system_only(),
                1 => {
                    let from = cur.string()?;
                    let to = cur.string()?;
                    TableTemporal::with_valid_time(
                        ValidTimeSpec::new(from, to)
                            .map_err(|e| corrupt(format!("valid-time: {e}")))?,
                    )
                }
                other => return Err(corrupt(format!("bad valid-time flag {other}"))),
            };
            CatalogRecord::CreateTable {
                at,
                namespace,
                name,
                columns,
                temporal,
            }
        }
        KIND_DROP_TABLE => CatalogRecord::DropTable {
            at: SystemTimeMicros(cur.i64()?),
            name: cur.string()?,
        },
        other => return Err(corrupt(format!("unknown record kind {other}"))),
    };
    if !cur.finished() {
        return Err(corrupt("trailing bytes after record payload"));
    }
    Ok(record)
}

// ---------------------------------------------------------------------------
// Durable log operations
// ---------------------------------------------------------------------------

/// Append one record to the catalog log and **fsync** it — the durability
/// point for the DDL it describes. The caller acknowledges the statement only
/// after this returns ([ADR-0028] write-ahead ordering).
///
/// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
///
/// # Errors
///
/// [`io::Error`] if the record cannot be encoded, or the log file cannot be
/// created/opened, appended, or synced. Nothing is acknowledged on error: a
/// partially-appended frame is exactly the torn tail [`replay`] tolerates.
pub(crate) fn append<D: Disk>(disk: &D, record: &CatalogRecord) -> io::Result<()> {
    let frame = encode_frame(record)?;
    let mut file = match disk.open(CATALOG_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => disk.create(CATALOG_LOG_FILENAME)?,
        Err(e) => return Err(e),
    };
    file.append(&frame)?;
    file.sync()?;
    Ok(())
}

/// Replay the catalog log: every acknowledged DDL mutation, oldest first. An
/// absent log (a fresh disk) is the empty history.
///
/// Applies the torn-tail contract from the [module docs](self): a partial
/// trailing frame — or a tail that does not begin with the record magic — is
/// the unacknowledged debris of a crashed append and is ignored; a *complete*
/// frame whose CRC fails is corruption of acknowledged history and fails
/// closed.
///
/// # Errors
///
/// [`io::Error`] if the file cannot be read, or holds a corrupt
/// (CRC-failing/undecodable) complete record.
pub(crate) fn replay<D: Disk>(disk: &D) -> io::Result<Vec<CatalogRecord>> {
    let file = match disk.open(CATALOG_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let len = file.len();
    let mut records = Vec::new();
    let mut offset = 0u64;

    loop {
        // Header: magic + payload length. A shorter remainder is a torn tail.
        let mut header = [0u8; HEADER_LEN];
        if offset + (HEADER_LEN as u64) > len || file.read_at(offset, &mut header)? < HEADER_LEN {
            break;
        }
        if header[0..4] != MAGIC {
            // Not a record boundary: the zero/garbage fill of a torn append.
            // Nothing past it was ever acknowledged (every acknowledged record
            // was fsynced *in order* before the next was written), so stop.
            break;
        }
        let payload_len = u64::from(u32::from_le_bytes(
            header[4..8].try_into().expect("4 bytes"),
        ));
        let frame_len = (HEADER_LEN as u64) + payload_len + (CRC_LEN as u64);
        if offset + frame_len > len {
            break; // torn tail: the frame's fsync never completed
        }

        // The frame is complete, so it was acknowledged — from here on,
        // damage is corruption and fails closed.
        let payload_bytes = usize::try_from(payload_len)
            .map_err(|_| corrupt("catalog log: record too large for this platform"))?;
        let mut body = vec![0u8; payload_bytes + CRC_LEN];
        if file.read_at(offset + (HEADER_LEN as u64), &mut body)? < body.len() {
            return Err(corrupt("catalog log: short read inside a complete record"));
        }
        let (payload, crc_bytes) = body.split_at(payload_bytes);
        let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
        let mut covered = Vec::with_capacity(HEADER_LEN + payload.len());
        covered.extend_from_slice(&header);
        covered.extend_from_slice(payload);
        if crc32c(&covered) != stored {
            return Err(corrupt(
                "catalog log: CRC mismatch on a complete record — acknowledged DDL is corrupt",
            ));
        }
        records.push(decode_payload(payload)?);
        offset += frame_len;
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    use stele_storage::backend::MemDisk;

    fn col(name: &str, ty: LogicalType) -> ColumnDef {
        ColumnDef::new(name, ty).expect("valid column")
    }

    fn create_record() -> CatalogRecord {
        CatalogRecord::CreateTable {
            at: SystemTimeMicros(42),
            namespace: 7,
            name: "account".to_owned(),
            columns: vec![
                col("id", LogicalType::Int4),
                col("balance", LogicalType::Int8),
            ],
            temporal: TableTemporal::system_only(),
        }
    }

    #[test]
    fn an_absent_log_replays_empty() {
        let disk = MemDisk::new();
        assert_eq!(replay(&disk).expect("replay"), Vec::new());
    }

    #[test]
    fn records_round_trip_in_order() {
        let disk = MemDisk::new();
        let records = vec![
            create_record(),
            CatalogRecord::DropTable {
                at: SystemTimeMicros(100),
                name: "account".to_owned(),
            },
            CatalogRecord::CreateTable {
                at: SystemTimeMicros(200),
                namespace: 7,
                name: "account".to_owned(),
                columns: vec![col("id", LogicalType::Int4)],
                temporal: TableTemporal::system_only(),
            },
        ];
        for r in &records {
            append(&disk, r).expect("append");
        }
        assert_eq!(replay(&disk).expect("replay"), records);
    }

    #[test]
    fn every_logical_type_and_the_valid_time_spec_round_trip() {
        // The tag table must invert exactly for the whole type vocabulary —
        // LogicalType::ALL is exhaustive, so a new variant lands here too.
        let disk = MemDisk::new();
        let columns: Vec<ColumnDef> = LogicalType::ALL
            .iter()
            .enumerate()
            .map(|(i, &ty)| col(&format!("c{i}"), ty))
            .collect();
        let record = CatalogRecord::CreateTable {
            at: SystemTimeMicros(1),
            namespace: 0,
            name: "wide".to_owned(),
            columns,
            temporal: TableTemporal::with_valid_time(
                ValidTimeSpec::new("vf", "vt").expect("valid spec"),
            ),
        };
        append(&disk, &record).expect("append");
        assert_eq!(replay(&disk).expect("replay"), vec![record]);
    }

    #[test]
    fn a_torn_trailing_frame_is_ignored() {
        // A crashed append leaves a partial frame; its fsync never returned,
        // so the DDL was never acknowledged — replay keeps the prior records.
        let disk = MemDisk::new();
        append(&disk, &create_record()).expect("append");
        let torn = encode_frame(&CatalogRecord::DropTable {
            at: SystemTimeMicros(99),
            name: "account".to_owned(),
        })
        .expect("encode");
        let mut file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        file.append(&torn[..torn.len() - 5]).expect("append torn");
        assert_eq!(replay(&disk).expect("replay"), vec![create_record()]);
    }

    #[test]
    fn a_zero_filled_tail_is_ignored() {
        // Some filesystems extend the file with zeros past the last durable
        // write on a crash; zeros are not a record boundary (and kind 0 is
        // reserved), so replay stops cleanly at the last good record.
        let disk = MemDisk::new();
        append(&disk, &create_record()).expect("append");
        let mut file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        file.append(&[0u8; 64]).expect("append zeros");
        assert_eq!(replay(&disk).expect("replay"), vec![create_record()]);
    }

    #[test]
    fn a_corrupt_complete_record_fails_closed() {
        // Flip a payload byte inside a complete, previously-fsynced frame:
        // that record was acknowledged, so replay must refuse rather than
        // silently serve a different table set.
        let disk = MemDisk::new();
        append(&disk, &create_record()).expect("append");
        let file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        let len = file.len();
        let mut bytes = vec![0u8; usize::try_from(len).expect("small file")];
        file.read_at(0, &mut bytes).expect("read");
        bytes[HEADER_LEN + 1] ^= 0xFF;
        // MemDisk files are append-only; rebuild the file with the damage.
        disk.remove(CATALOG_LOG_FILENAME).expect("remove");
        let mut rebuilt = disk.create(CATALOG_LOG_FILENAME).expect("create");
        rebuilt.append(&bytes).expect("append");
        let err = replay(&disk).expect_err("corruption must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn an_unknown_record_kind_fails_closed() {
        // A log written by a newer build (a kind this build does not know) is
        // not silently skipped: dropping an acknowledged mutation would change
        // the table set.
        let disk = MemDisk::new();
        let mut frame = encode_frame(&create_record()).expect("encode");
        frame[HEADER_LEN] = 0xEE; // overwrite the kind byte…
        let crc = crc32c(&frame[..frame.len() - CRC_LEN]);
        let crc_at = frame.len() - CRC_LEN;
        frame[crc_at..].copy_from_slice(&crc.to_le_bytes()); // …with a valid CRC
        let mut file = disk.create(CATALOG_LOG_FILENAME).expect("create");
        file.append(&frame).expect("append");
        let err = replay(&disk).expect_err("unknown kind must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
