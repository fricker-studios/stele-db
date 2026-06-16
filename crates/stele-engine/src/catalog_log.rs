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
//! Each record is `magic(4) | payload_len(4 LE) | prev_hash(32) | payload |
//! crc32c(4 LE)` (the CRC covers magic + length + prev_hash + payload).
//! [`replay`] validates records in sequence and distinguishes two failure
//! shapes:
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
//! ## Tamper-evidence: the hash chain ([ADR-0031], [STL-307])
//!
//! The CRC catches *accidental* damage, but an operator forging history can
//! recompute it — so the catalog log is also a **hash chain**, the same shape
//! the [commit log](crate::commit_log) is ([ADR-0031], STL-302). Each frame
//! carries the [SHA-256](stele_common::hash) `prev_hash` of the record before
//! it — `sha256(prev_hash ‖ payload)`, the prior record's link, anchored at
//! [`Digest::ZERO`](stele_common::hash::Digest::ZERO) for the first record.
//! Altering any historical record changes its hash, so the *next* record's
//! stored `prev_hash` no longer matches and [`replay`] fails closed at the
//! break — extending invariant 10's tamper-evidence from the commit log (data
//! commits) to the catalog log (DDL). [`SessionEngine::recover`](crate::SessionEngine::recover)
//! verifies this chain on every boot, so a tampered DDL record refuses recovery
//! rather than serving forged catalog history.
//!
//! Like the commit log, a genesis walk catches an *interior* tamper (a broken
//! link); a **wholly rewritten** chain (every link recomputed from genesis)
//! needs an external anchor to detect — the durably-remembered head a future
//! catalog `\audit` surface or unified log would supply ([`chain`](stele_txn::verify_chain_to),
//! the Merkle work of [ADR-0026], ~v0.5). Recovery walks from genesis, exactly
//! as the commit log's does.
//!
//! [ADR-0026]: ../../../docs/adr/0026-verifiable-audit-log.md
//! [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
//! [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
//! [STL-210]: https://allegromusic.atlassian.net/browse/STL-210
//! [STL-307]: https://allegromusic.atlassian.net/browse/STL-307
//! [`Catalog`]: stele_catalog::Catalog

use std::io;

use stele_catalog::{ColumnDef, IndexKind, TableTemporal, ValidTimeSpec};
use stele_common::hash::{Digest, SHA256_LEN, sha256};
use stele_common::scram::ScramVerifier;
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

/// Bytes before the `prev_hash`: magic + payload length.
const HEADER_LEN: usize = 8;

/// Bytes of the per-record `prev_hash` link — a SHA-256 digest ([ADR-0031]).
/// Sits between the header and the payload so the logical [`encode_payload`] /
/// [`decode_payload`] stay purely about the record's content.
///
/// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
const PREV_HASH_LEN: usize = SHA256_LEN;

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
    /// A `CREATE INDEX` was acknowledged at `at` ([STL-233]). Only the
    /// *metadata* is durable: the access structure itself is derived,
    /// rebuildable state ([ADR-0023]) that recovery reconstructs from the
    /// table's tiers — so a crash at any point during the build leaves either
    /// no record (the statement was never acknowledged, no index exists) or
    /// this record (recovery rebuilds the structure). `at` doubles as
    /// provenance: the instant from which the maintained structure covers the
    /// table's writes.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    /// [ADR-0023]: ../../../docs/adr/0023-append-only-record-model-validity-index.md
    CreateIndex {
        /// The system time the index DDL was acknowledged.
        at: SystemTimeMicros,
        /// The index name — unique across the live index set.
        name: String,
        /// The table the index accelerates.
        table: String,
        /// The access-structure family.
        kind: IndexKind,
        /// The indexed value column names, in declaration order.
        columns: Vec<String>,
    },
    /// A `DROP INDEX` was acknowledged at `at` ([STL-233]). A `DROP TABLE`
    /// carries **no** per-index records: replay re-derives the cascade (the
    /// dropped table's indexes go with it), exactly as the live session did.
    ///
    /// [STL-233]: https://allegromusic.atlassian.net/browse/STL-233
    DropIndex {
        /// The system time the index DDL was acknowledged.
        at: SystemTimeMicros,
        /// The dropped index's name.
        name: String,
    },
    /// A `CREATE USER` was acknowledged at `at` ([STL-252]): the durable user
    /// store is this log. The record carries the derived **SCRAM verifier** —
    /// salted and iterated key material, never the password — so a restart
    /// recovers exactly the authentication state that was acknowledged.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    CreateUser {
        /// The system time the user DDL was acknowledged.
        at: SystemTimeMicros,
        /// The user name.
        name: String,
        /// The stored SCRAM-SHA-256 verifier.
        verifier: ScramVerifier,
    },
    /// An `ALTER USER … PASSWORD` was acknowledged at `at` ([STL-252]): a
    /// fresh salt + verifier replace the user's stored one on replay. The old
    /// record remains in the log (append-only — the rotation history is part
    /// of the audit trail), but only the latest verifier authenticates.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    AlterUser {
        /// The system time the user DDL was acknowledged.
        at: SystemTimeMicros,
        /// The user name.
        name: String,
        /// The replacement SCRAM-SHA-256 verifier.
        verifier: ScramVerifier,
    },
    /// A `DROP USER` was acknowledged at `at` ([STL-252]): replay removes the
    /// user from the recovered store.
    ///
    /// [STL-252]: https://allegromusic.atlassian.net/browse/STL-252
    DropUser {
        /// The system time the user DDL was acknowledged.
        at: SystemTimeMicros,
        /// The dropped user's name.
        name: String,
    },
}

/// Record-kind discriminants. `0` is deliberately unused so a zero-filled
/// region can never decode as a record even if its CRC were somehow valid.
const KIND_CREATE_TABLE: u8 = 1;
const KIND_DROP_TABLE: u8 = 2;
const KIND_CREATE_INDEX: u8 = 3;
const KIND_DROP_INDEX: u8 = 4;
const KIND_CREATE_USER: u8 = 5;
const KIND_ALTER_USER: u8 = 6;
const KIND_DROP_USER: u8 = 7;

/// Map an [`IndexKind`] to its stable on-log tag. Exhaustive, like
/// [`type_tag`]: a sibling ticket adding a kind fails compilation here until
/// it assigns a tag (append-only; never renumber).
const fn index_kind_tag(kind: IndexKind) -> u8 {
    match kind {
        IndexKind::BTree => 1,
        IndexKind::Hash => 2,
    }
}

/// The inverse of [`index_kind_tag`], or [`None`] for a tag this build does
/// not know (a log written by a newer build).
const fn index_kind_from_tag(tag: u8) -> Option<IndexKind> {
    Some(match tag {
        1 => IndexKind::BTree,
        2 => IndexKind::Hash,
        _ => return None,
    })
}

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

/// Append a length-prefixed byte string (`u16 LE` length + bytes) — the SCRAM
/// salt's shape ([STL-252]).
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> io::Result<()> {
    let len = u16::try_from(bytes.len()).map_err(|_| {
        corrupt(format!(
            "byte field too long for the catalog log ({})",
            bytes.len()
        ))
    })?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Encode a [`ScramVerifier`]: `iterations u32 LE | salt (u16-prefixed) |
/// stored_key (32 raw) | server_key (32 raw)`.
fn put_verifier(buf: &mut Vec<u8>, verifier: &ScramVerifier) -> io::Result<()> {
    buf.extend_from_slice(&verifier.iterations.to_le_bytes());
    put_bytes(buf, &verifier.salt)?;
    buf.extend_from_slice(&verifier.stored_key);
    buf.extend_from_slice(&verifier.server_key);
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
        CatalogRecord::CreateIndex {
            at,
            name,
            table,
            kind,
            columns,
        } => {
            buf.push(KIND_CREATE_INDEX);
            buf.extend_from_slice(&at.0.to_le_bytes());
            put_str(&mut buf, name)?;
            put_str(&mut buf, table)?;
            buf.push(index_kind_tag(*kind));
            let count = u16::try_from(columns.len())
                .map_err(|_| corrupt(format!("too many index columns ({})", columns.len())))?;
            buf.extend_from_slice(&count.to_le_bytes());
            for column in columns {
                put_str(&mut buf, column)?;
            }
        }
        CatalogRecord::DropIndex { at, name } => {
            buf.push(KIND_DROP_INDEX);
            buf.extend_from_slice(&at.0.to_le_bytes());
            put_str(&mut buf, name)?;
        }
        CatalogRecord::CreateUser { at, name, verifier } => {
            buf.push(KIND_CREATE_USER);
            buf.extend_from_slice(&at.0.to_le_bytes());
            put_str(&mut buf, name)?;
            put_verifier(&mut buf, verifier)?;
        }
        CatalogRecord::AlterUser { at, name, verifier } => {
            buf.push(KIND_ALTER_USER);
            buf.extend_from_slice(&at.0.to_le_bytes());
            put_str(&mut buf, name)?;
            put_verifier(&mut buf, verifier)?;
        }
        CatalogRecord::DropUser { at, name } => {
            buf.push(KIND_DROP_USER);
            buf.extend_from_slice(&at.0.to_le_bytes());
            put_str(&mut buf, name)?;
        }
    }
    Ok(buf)
}

/// The chain link a record contributes: `sha256(prev_hash ‖ payload)` — the
/// hash the *next* record stores as its `prev_hash` ([ADR-0031]). The first
/// record chains from [`Digest::ZERO`]. The CRC framing is deliberately *not*
/// hashed: the chain is over the log's logical content + its back-link, so it
/// is the same evidence whichever backend reframes it.
///
/// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
fn record_hash(prev_hash: Digest, payload: &[u8]) -> Digest {
    let mut buf = Vec::with_capacity(PREV_HASH_LEN + payload.len());
    buf.extend_from_slice(prev_hash.as_bytes());
    buf.extend_from_slice(payload);
    sha256(&buf)
}

/// Encode one record as a complete frame chaining from `prev_hash`: header,
/// `prev_hash`, payload, trailing CRC. Returns the frame and the record's own
/// link hash — the `prev_hash` the next record chains from.
fn encode_frame(record: &CatalogRecord, prev_hash: Digest) -> io::Result<(Vec<u8>, Digest)> {
    let payload = encode_payload(record)?;
    let len = u32::try_from(payload.len()).map_err(|_| corrupt("record payload too large"))?;
    let mut frame = Vec::with_capacity(HEADER_LEN + PREV_HASH_LEN + payload.len() + CRC_LEN);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(prev_hash.as_bytes());
    frame.extend_from_slice(&payload);
    let crc = crc32c(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    Ok((frame, record_hash(prev_hash, &payload)))
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

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn string(&mut self) -> io::Result<String> {
        let len = usize::from(self.u16()?);
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("identifier is not UTF-8"))
    }

    fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = usize::from(self.u16()?);
        Ok(self.take(len)?.to_vec())
    }

    fn key32(&mut self) -> io::Result<[u8; 32]> {
        Ok(self.take(32)?.try_into().expect("32 bytes"))
    }

    fn verifier(&mut self) -> io::Result<ScramVerifier> {
        Ok(ScramVerifier {
            iterations: self.u32()?,
            salt: self.bytes()?,
            stored_key: self.key32()?,
            server_key: self.key32()?,
        })
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
        KIND_CREATE_INDEX => {
            let at = SystemTimeMicros(cur.i64()?);
            let name = cur.string()?;
            let table = cur.string()?;
            let tag = cur.u8()?;
            let kind = index_kind_from_tag(tag)
                .ok_or_else(|| corrupt(format!("unknown index kind tag {tag}")))?;
            let count = usize::from(cur.u16()?);
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(cur.string()?);
            }
            CatalogRecord::CreateIndex {
                at,
                name,
                table,
                kind,
                columns,
            }
        }
        KIND_DROP_INDEX => CatalogRecord::DropIndex {
            at: SystemTimeMicros(cur.i64()?),
            name: cur.string()?,
        },
        KIND_CREATE_USER => CatalogRecord::CreateUser {
            at: SystemTimeMicros(cur.i64()?),
            name: cur.string()?,
            verifier: cur.verifier()?,
        },
        KIND_ALTER_USER => CatalogRecord::AlterUser {
            at: SystemTimeMicros(cur.i64()?),
            name: cur.string()?,
            verifier: cur.verifier()?,
        },
        KIND_DROP_USER => CatalogRecord::DropUser {
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

/// Append one record to the catalog log chaining from `prev_hash` and **fsync**
/// it — the durability point for the DDL it describes. The caller acknowledges
/// the statement only after this returns ([ADR-0028] write-ahead ordering), and
/// adopts the returned hash as the running chain head ([ADR-0031]).
///
/// [ADR-0028]: ../../../docs/adr/0028-durable-catalog-log.md
/// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
///
/// # Errors
///
/// [`io::Error`] if the record cannot be encoded, or the log file cannot be
/// created/opened, appended, or synced. Nothing is acknowledged on error: a
/// partially-appended frame is exactly the torn tail [`replay`] tolerates, and
/// the caller keeps its prior chain head (the record was never linked in).
pub(crate) fn append<D: Disk>(
    disk: &D,
    record: &CatalogRecord,
    prev_hash: Digest,
) -> io::Result<Digest> {
    let (frame, head) = encode_frame(record, prev_hash)?;
    let mut file = match disk.open(CATALOG_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let file = disk.create(CATALOG_LOG_FILENAME)?;
            // Directory fence ([STL-232]): the first acknowledged DDL is only
            // as durable as the log file's directory entry. On fence failure,
            // undo the create (best-effort) so a retry re-creates and
            // re-fences — otherwise the retry would take the `open` path,
            // which never fences, and could acknowledge onto an entry no
            // fence ever vouched for.
            if let Err(e) = disk.sync_dir() {
                drop(file);
                let _ = disk.remove(CATALOG_LOG_FILENAME);
                return Err(e);
            }
            file
        }
        Err(e) => return Err(e),
    };
    file.append(&frame)?;
    file.sync()?;
    Ok(head)
}

/// Replay the catalog log: every acknowledged DDL mutation, oldest first, plus
/// the verified chain head (the [`Digest`] the next append chains from). An
/// absent log (a fresh disk) is the empty history chaining from [`Digest::ZERO`].
///
/// Applies the torn-tail contract from the [module docs](self): a partial
/// trailing frame — or a tail that does not begin with the record magic — is
/// the unacknowledged debris of a crashed append and is ignored; a *complete*
/// frame whose CRC fails is corruption of acknowledged history and fails
/// closed. A complete, CRC-valid frame whose `prev_hash` does not match the
/// running chain head is a **tampered** record ([ADR-0031]) — an operator's
/// re-CRC'd forgery — and also fails closed, at the break.
///
/// [ADR-0031]: ../../../docs/adr/0031-live-server-verifiable-commit-log.md
///
/// # Errors
///
/// [`io::Error`] if the file cannot be read, or holds a corrupt
/// (CRC-failing/undecodable) or chain-broken complete record.
pub(crate) fn replay<D: Disk>(disk: &D) -> io::Result<(Vec<CatalogRecord>, Digest)> {
    let file = match disk.open(CATALOG_LOG_FILENAME) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), Digest::ZERO)),
        Err(e) => return Err(e),
    };
    let len = file.len();
    let mut records = Vec::new();
    let mut offset = 0u64;
    // The running chain head: the first record must chain from genesis, and each
    // later record from the prior record's hash ([ADR-0031]).
    let mut head = Digest::ZERO;

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
        let frame_len =
            (HEADER_LEN as u64) + (PREV_HASH_LEN as u64) + payload_len + (CRC_LEN as u64);
        if offset + frame_len > len {
            break; // torn tail: the frame's fsync never completed
        }

        // The frame is complete, so it was acknowledged — from here on,
        // damage is corruption and fails closed. Body: prev_hash + payload + CRC.
        let payload_bytes = usize::try_from(payload_len)
            .map_err(|_| corrupt("catalog log: record too large for this platform"))?;
        let mut body = vec![0u8; PREV_HASH_LEN + payload_bytes + CRC_LEN];
        if file.read_at(offset + (HEADER_LEN as u64), &mut body)? < body.len() {
            return Err(corrupt("catalog log: short read inside a complete record"));
        }
        let (prev_hash_bytes, rest) = body.split_at(PREV_HASH_LEN);
        let (payload, crc_bytes) = rest.split_at(payload_bytes);
        let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
        let mut covered = Vec::with_capacity(HEADER_LEN + PREV_HASH_LEN + payload.len());
        covered.extend_from_slice(&header);
        covered.extend_from_slice(prev_hash_bytes);
        covered.extend_from_slice(payload);
        if crc32c(&covered) != stored {
            return Err(corrupt(
                "catalog log: CRC mismatch on a complete record — acknowledged DDL is corrupt",
            ));
        }
        // Tamper check ([ADR-0031]): the stored back-link must match the running
        // head. A re-CRC'd forgery passes the CRC above but breaks the chain here.
        let prev_hash = Digest(prev_hash_bytes.try_into().expect("32 bytes"));
        if prev_hash != head {
            return Err(corrupt(format!(
                "catalog log: hash-chain broken at record {} — \
                 prev_hash {} does not match predecessor {} (tampered DDL history)",
                records.len(),
                prev_hash.to_hex(),
                head.to_hex(),
            )));
        }
        head = record_hash(prev_hash, payload);
        records.push(decode_payload(payload)?);
        offset += frame_len;
    }
    Ok((records, head))
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

    /// Append `records` as one chain from genesis, returning the running head —
    /// the shape `SessionEngine` drives the log with ([ADR-0031]).
    fn append_chain(disk: &MemDisk, records: &[CatalogRecord]) -> Digest {
        let mut head = Digest::ZERO;
        for r in records {
            head = append(disk, r, head).expect("append");
        }
        head
    }

    #[test]
    fn an_absent_log_replays_empty() {
        let disk = MemDisk::new();
        assert_eq!(replay(&disk).expect("replay"), (Vec::new(), Digest::ZERO));
    }

    #[test]
    fn records_round_trip_and_advance_the_chain_head() {
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
        let head = append_chain(&disk, &records);
        let (replayed, replayed_head) = replay(&disk).expect("replay");
        assert_eq!(replayed, records);
        // Replay re-walks the chain and recovers the same head the appends ran to,
        // and a non-empty log has advanced past genesis ([ADR-0031]).
        assert_eq!(replayed_head, head);
        assert_ne!(replayed_head, Digest::ZERO);
    }

    #[test]
    fn the_first_record_fences_the_directory_entry() {
        // STL-232: the log file's directory entry is fenced at creation — a
        // failed fence fails the append before any DDL is acknowledged and
        // undoes the create, so a retry re-creates and re-fences rather than
        // acknowledging onto an entry no fence ever vouched for.
        use stele_storage::backend::{FaultOp, Faults};

        let faults = Faults::new();
        let disk = MemDisk::with_faults(faults.clone());
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(
            append(&disk, &create_record(), Digest::ZERO).is_err(),
            "fence failure surfaces"
        );
        assert_eq!(replay(&disk).expect("replay"), (Vec::new(), Digest::ZERO));

        // The failed create was undone — the retry re-creates and re-fences,
        // consuming a second scheduled fault.
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        assert!(
            append(&disk, &create_record(), Digest::ZERO).is_err(),
            "the retry re-fences"
        );
        assert_eq!(faults.pending(), 0);

        // Healthy disk: create + fence + acknowledge; append-path records
        // never re-fence (a pending SyncDir fault stays unconsumed).
        let head = append(&disk, &create_record(), Digest::ZERO).expect("record");
        faults.schedule(FaultOp::SyncDir, io::ErrorKind::Other);
        append(&disk, &create_record(), head).expect("append-path record");
        assert_eq!(faults.pending(), 1, "no fence on the append path");
        assert_eq!(
            replay(&disk).expect("replay").0,
            vec![create_record(), create_record()]
        );
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
        append(&disk, &record, Digest::ZERO).expect("append");
        assert_eq!(replay(&disk).expect("replay").0, vec![record]);
    }

    #[test]
    fn index_records_round_trip_in_order() {
        // The STL-233 records intermix with table DDL in one log; replay must
        // reproduce the exact sequence (the engine re-derives the live index
        // set, including the drop-table cascade, from this order).
        let disk = MemDisk::new();
        let records = vec![
            create_record(),
            CatalogRecord::CreateIndex {
                at: SystemTimeMicros(50),
                name: "i_balance".to_owned(),
                table: "account".to_owned(),
                kind: IndexKind::BTree,
                columns: vec!["balance".to_owned()],
            },
            CatalogRecord::DropIndex {
                at: SystemTimeMicros(60),
                name: "i_balance".to_owned(),
            },
            CatalogRecord::DropTable {
                at: SystemTimeMicros(70),
                name: "account".to_owned(),
            },
        ];
        append_chain(&disk, &records);
        assert_eq!(replay(&disk).expect("replay").0, records);
    }

    #[test]
    fn user_records_round_trip_in_order() {
        // The STL-252 user store intermixes with table/index DDL in one log;
        // replay must reproduce the exact sequence (create → rotate → drop),
        // verifier bytes included.
        let disk = MemDisk::new();
        let verifier = ScramVerifier::derive("s3cret", b"0123456789abcdef", 4096);
        let rotated = ScramVerifier::derive("rotated", b"fedcba9876543210", 4096);
        let records = vec![
            create_record(),
            CatalogRecord::CreateUser {
                at: SystemTimeMicros(10),
                name: "alice".to_owned(),
                verifier,
            },
            CatalogRecord::AlterUser {
                at: SystemTimeMicros(20),
                name: "alice".to_owned(),
                verifier: rotated.clone(),
            },
            CatalogRecord::DropUser {
                at: SystemTimeMicros(30),
                name: "alice".to_owned(),
            },
        ];
        append_chain(&disk, &records);
        let (replayed, _) = replay(&disk).expect("replay");
        assert_eq!(replayed, records);
        // The verifier's key material survives byte-for-byte (PartialEq above
        // covers it, but pin the fields the SASL exchange actually reads).
        let CatalogRecord::AlterUser {
            verifier: replayed_verifier,
            ..
        } = &replayed[2]
        else {
            panic!("expected the AlterUser record");
        };
        assert_eq!(replayed_verifier.iterations, 4096);
        assert_eq!(replayed_verifier.salt, rotated.salt);
        assert_eq!(replayed_verifier.stored_key, rotated.stored_key);
        assert_eq!(replayed_verifier.server_key, rotated.server_key);
    }

    #[test]
    fn an_unknown_index_kind_tag_fails_closed() {
        // A log written by a newer build (an index kind this build does not
        // know) must refuse rather than silently dropping the index — the
        // sibling-kind seam mirrors the record-kind contract.
        let disk = MemDisk::new();
        let (mut frame, _) = encode_frame(
            &CatalogRecord::CreateIndex {
                at: SystemTimeMicros(1),
                name: "i".to_owned(),
                table: "account".to_owned(),
                kind: IndexKind::BTree,
                columns: vec!["balance".to_owned()],
            },
            Digest::ZERO,
        )
        .expect("encode");
        // The kind tag sits after the prev_hash, the record kind byte, the i64
        // instant, and the two length-prefixed strings ("i", "account").
        let tag_at = HEADER_LEN + PREV_HASH_LEN + 1 + 8 + (2 + 1) + (2 + 7);
        frame[tag_at] = 0xEE;
        let crc = crc32c(&frame[..frame.len() - CRC_LEN]);
        let crc_at = frame.len() - CRC_LEN;
        frame[crc_at..].copy_from_slice(&crc.to_le_bytes());
        let mut file = disk.create(CATALOG_LOG_FILENAME).expect("create");
        file.append(&frame).expect("append");
        let err = replay(&disk).expect_err("unknown index kind must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_torn_trailing_frame_is_ignored() {
        // A crashed append leaves a partial frame; its fsync never returned,
        // so the DDL was never acknowledged — replay keeps the prior records.
        let disk = MemDisk::new();
        let head = append(&disk, &create_record(), Digest::ZERO).expect("append");
        let (torn, _) = encode_frame(
            &CatalogRecord::DropTable {
                at: SystemTimeMicros(99),
                name: "account".to_owned(),
            },
            head,
        )
        .expect("encode");
        let mut file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        file.append(&torn[..torn.len() - 5]).expect("append torn");
        assert_eq!(replay(&disk).expect("replay").0, vec![create_record()]);
    }

    #[test]
    fn a_zero_filled_tail_is_ignored() {
        // Some filesystems extend the file with zeros past the last durable
        // write on a crash; zeros are not a record boundary (and kind 0 is
        // reserved), so replay stops cleanly at the last good record.
        let disk = MemDisk::new();
        append(&disk, &create_record(), Digest::ZERO).expect("append");
        let mut file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        file.append(&[0u8; 64]).expect("append zeros");
        assert_eq!(replay(&disk).expect("replay").0, vec![create_record()]);
    }

    #[test]
    fn a_corrupt_complete_record_fails_closed() {
        // Flip a payload byte inside a complete, previously-fsynced frame:
        // that record was acknowledged, so replay must refuse rather than
        // silently serve a different table set.
        let disk = MemDisk::new();
        append(&disk, &create_record(), Digest::ZERO).expect("append");
        let file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        let len = file.len();
        let mut bytes = vec![0u8; usize::try_from(len).expect("small file")];
        file.read_at(0, &mut bytes).expect("read");
        // A payload byte (past the header and prev_hash); not re-CRC'd, so the
        // CRC check — not the hash chain — is what fails it closed.
        bytes[HEADER_LEN + PREV_HASH_LEN + 1] ^= 0xFF;
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
        let (mut frame, _) = encode_frame(&create_record(), Digest::ZERO).expect("encode");
        frame[HEADER_LEN + PREV_HASH_LEN] = 0xEE; // overwrite the kind byte…
        let crc = crc32c(&frame[..frame.len() - CRC_LEN]);
        let crc_at = frame.len() - CRC_LEN;
        frame[crc_at..].copy_from_slice(&crc.to_le_bytes()); // …with a valid CRC
        let mut file = disk.create(CATALOG_LOG_FILENAME).expect("create");
        file.append(&frame).expect("append");
        let err = replay(&disk).expect_err("unknown kind must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_tampered_historical_record_breaks_the_chain() {
        // The catalog-log tamper oracle ([ADR-0031], testing-strategy §4). Two
        // chained records; rewrite the *first* record's payload in place and
        // re-CRC it — the well-framed forgery a privileged operator could
        // attempt. `replay`'s CRC check passes (the forger fixed it), but the
        // second record's stored prev_hash still expects the original first
        // record's hash, so the genesis walk breaks the chain at record 1.
        let disk = MemDisk::new();
        append_chain(
            &disk,
            &[
                create_record(),
                CatalogRecord::DropTable {
                    at: SystemTimeMicros(100),
                    name: "account".to_owned(),
                },
            ],
        );

        // Forge record 0 in place: flip a `CreateTable.at` byte (still decodes,
        // just different content) and recompute its CRC, leaving its genesis
        // prev_hash untouched so only the hash chain — not the CRC — catches it.
        let file = disk.open(CATALOG_LOG_FILENAME).expect("open");
        let len = usize::try_from(file.len()).expect("small file");
        let mut bytes = vec![0u8; len];
        file.read_at(0, &mut bytes).expect("read");
        let payload_len = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")) as usize;
        let payload_start = HEADER_LEN + PREV_HASH_LEN;
        let crc_start = payload_start + payload_len;
        bytes[payload_start + 1] ^= 0xFF; // a `CreateTable.at` byte
        let crc = crc32c(&bytes[..crc_start]);
        bytes[crc_start..crc_start + CRC_LEN].copy_from_slice(&crc.to_le_bytes());
        disk.remove(CATALOG_LOG_FILENAME).expect("remove");
        disk.create(CATALOG_LOG_FILENAME)
            .expect("create")
            .append(&bytes)
            .expect("append");

        let err = replay(&disk).expect_err("a tampered record breaks the chain");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("hash-chain broken"),
            "the verdict names the tamper, not a generic corruption: {err}",
        );
    }
}
