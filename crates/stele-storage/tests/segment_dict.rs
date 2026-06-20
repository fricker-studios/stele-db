//! Version-chain-aware dictionary segment encoding oracle ([STL-250],
//! [feature plan §A.2], [ADR-0002]).
//!
//! "Efficient historization": a key updated many times whose unchanged columns
//! are re-stored wholesale per version wastes the append-only model. The segment
//! writer's dictionary codec ([`SegmentWriter::with_dictionary`], format v13)
//! stores a value repeated across a key's version chain — the *identical*
//! `business_key`, a repeated `principal` / `payload` — **once** plus a narrow
//! code per row, picking dictionary or plain per chunk by which is smaller. This
//! file pins the two DoD halves the existing compaction / bitemporal oracles
//! (which now run over dict-encoded output) do not assert directly:
//!
//! 1. **Materially smaller, byte-identical reads.** A many-versions-per-key
//!    segment is materially smaller with dictionary encoding (a *measured* ratio),
//!    and decodes to exactly the same versions and columns as the plain encoding —
//!    the equivalence oracle.
//! 2. **Survives backup/restore and stays immutable.** A dictionary segment is
//!    an opaque immutable file: a byte-copy backup/restore round-trips it
//!    bit-for-bit, reading it never mutates it ([STL-186], invariant 1), and the
//!    restored copy decodes identically.
//!
//! Plus the encoding's edges: SQL `NULL` and an empty payload stay distinct
//! dictionary entries, and an all-distinct column declines the dictionary so a
//! segment never grows.
//!
//! [STL-250]: https://allegromusic.atlassian.net/browse/STL-250
//! [feature plan §A.2]: ../../../docs/01-feature-plan.md
//! [ADR-0002]: ../../../docs/adr/0002-on-disk-storage-format.md

#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use stele_common::hash::sha256;
use stele_common::provenance::{Principal, Provenance, TxnId};
use stele_common::time::SystemTimeMicros;
use stele_storage::backend::{Disk, DiskFile, MemDisk};
use stele_storage::delta::{BusinessKey, Version};
use stele_storage::segment::{ColumnData, ColumnId, SegmentReader, SegmentWriter};

// --- helpers ---------------------------------------------------------------

/// An open birth-state version with the given identity, provenance principal, and
/// (possibly NULL) payload. A segment stores only birth state (v6, ADR-0023), so
/// the round-trip compares against open versions.
fn version(
    key: &[u8],
    sys_from: i64,
    seq: u64,
    principal: &[u8],
    payload: Option<&[u8]>,
) -> Version {
    Version::open(
        BusinessKey::new(key.to_vec()),
        SystemTimeMicros(sys_from),
        seq,
        Provenance::new(
            TxnId(sys_from as u64),
            SystemTimeMicros(sys_from),
            Principal::new(principal.to_vec()),
        ),
        payload.map(<[u8]>::to_vec),
    )
}

/// Write `versions` into a sealed segment, with dictionary encoding on or off.
fn write(disk: &MemDisk, name: &str, versions: &[Version], dictionary: bool) {
    let mut w = SegmentWriter::create(disk, name)
        .expect("create writer")
        .with_dictionary(dictionary);
    for v in versions {
        w.push(v.clone()).expect("push");
    }
    w.finish().expect("finish");
}

/// The complete byte content of one file on `disk`.
fn read_all(disk: &MemDisk, name: &str) -> Vec<u8> {
    let file = disk.open(name).expect("open");
    let mut bytes = vec![0u8; usize::try_from(file.len()).expect("len fits usize")];
    let read = file.read_at(0, &mut bytes).expect("read");
    bytes.truncate(read);
    bytes
}

/// Project every always-on column and assert two readers agree cell-for-cell.
fn assert_columns_equal(a: &SegmentReader<impl DiskFile>, b: &SegmentReader<impl DiskFile>) {
    for col in [
        ColumnId::BusinessKey,
        ColumnId::Payload,
        ColumnId::Principal,
        ColumnId::SysFrom,
        ColumnId::Seq,
        ColumnId::TxnId,
        ColumnId::CommittedAt,
    ] {
        assert_eq!(
            a.read_column(col).expect("read a"),
            b.read_column(col).expect("read b"),
            "column {col:?} differs across codecs"
        );
    }
}

// --- DoD 1: materially smaller, byte-identical reads -----------------------

#[test]
fn dictionary_shrinks_a_version_chain_and_reads_identically() {
    let disk = MemDisk::new();
    // One business key updated 1,000 times — the case §A.2 names. Its key is
    // *identical* across every version, and a steady-state writer repeats the same
    // principal and payload, so all three bytes columns are dictionary-eligible;
    // only the i64 temporal/provenance columns differ per version.
    let n = 1_000i64;
    let key = b"account-000000000042"; // 20 bytes, identical every version
    let principal = b"svc-ledger-writer"; // identical every version
    let versions: Vec<Version> = (0..n)
        .map(|i| version(key, 10 + i, i as u64, principal, Some(b"balance=100")))
        .collect();

    write(&disk, "plain.seg", &versions, false);
    write(&disk, "dict.seg", &versions, true);

    let plain = SegmentReader::open(&disk, "plain.seg").expect("open plain");
    let dict = SegmentReader::open(&disk, "dict.seg").expect("open dict");

    // Byte-identical reads: the reconstructed versions match the input and each
    // other, whichever codec each column landed in.
    assert_eq!(dict.read_versions().expect("dict versions"), versions);
    assert_eq!(
        dict.read_versions().expect("dict versions"),
        plain.read_versions().expect("plain versions"),
    );
    assert_columns_equal(&dict, &plain);

    // Measured ratio: the identical business key collapses to a single dictionary
    // entry plus one byte per row — an order of magnitude smaller than re-storing
    // the 24-byte framed value 1,000 times.
    let bk_plain = plain
        .column_byte_len(ColumnId::BusinessKey)
        .expect("plain bk");
    let bk_dict = dict
        .column_byte_len(ColumnId::BusinessKey)
        .expect("dict bk");
    assert!(
        bk_dict.saturating_mul(10) < bk_plain,
        "business_key: dict {bk_dict} B should be <10% of plain {bk_plain} B"
    );

    // And the whole segment is materially smaller — under half the plain size,
    // even with the i64 columns (untouched by this codec) as a floor.
    assert!(
        dict.byte_size().saturating_mul(2) < plain.byte_size(),
        "segment: dict {} B should be <50% of plain {} B",
        dict.byte_size(),
        plain.byte_size(),
    );
}

#[test]
fn dictionary_round_trips_nulls_and_repeated_payloads() {
    let disk = MemDisk::new();
    // Payloads cycle among a few distinct values — including SQL NULL and a
    // *distinct* empty payload — across many rows, so the dictionary carries a
    // NULL entry and a zero-length entry and must keep them apart ([STL-154]).
    let palette: [Option<&[u8]>; 4] = [
        Some(b"a".as_slice()),
        None,
        Some(b"bb".as_slice()),
        Some(b"".as_slice()),
    ];
    let versions: Vec<Version> = (0..200i64)
        .map(|i| version(b"k", 10 + i, i as u64, b"p", palette[(i % 4) as usize]))
        .collect();

    write(&disk, "dict.seg", &versions, true);
    let dict = SegmentReader::open(&disk, "dict.seg").expect("open");

    assert_eq!(dict.read_versions().expect("versions"), versions);
    match dict.read_column(ColumnId::Payload).expect("payload") {
        ColumnData::NullableBytes(cells) => {
            assert_eq!(cells[1], None, "NULL payload survives as NULL");
            assert_eq!(
                cells[3],
                Some(Vec::new()),
                "empty payload stays distinct from NULL"
            );
        }
        other => panic!("expected NullableBytes, got {other:?}"),
    }
}

#[test]
fn an_all_distinct_column_declines_the_dictionary() {
    let disk = MemDisk::new();
    // Every business key, principal, and payload distinct ⇒ a dictionary can only
    // be larger, so the writer must fall back to plain for every bytes column. A
    // dict-enabled segment is then byte-identical to a plain one — the codec
    // choice never bloats.
    let versions: Vec<Version> = (0..256i64)
        .map(|i| {
            version(
                format!("key-{i:08}").as_bytes(),
                10 + i,
                i as u64,
                format!("svc-{i}").as_bytes(),
                Some(format!("pay-{i}").as_bytes()),
            )
        })
        .collect();

    write(&disk, "plain.seg", &versions, false);
    write(&disk, "dict.seg", &versions, true);

    assert_eq!(
        read_all(&disk, "dict.seg"),
        read_all(&disk, "plain.seg"),
        "an all-distinct segment must be byte-identical whether or not dictionary encoding is enabled"
    );
    let dict = SegmentReader::open(&disk, "dict.seg").expect("open dict");
    assert_eq!(dict.read_versions().expect("versions"), versions);
}

// --- DoD 2: survives backup/restore, stays immutable -----------------------

#[test]
fn a_dictionary_segment_survives_backup_restore_and_stays_immutable() {
    let src = MemDisk::new();
    let key = b"key-0001";
    let versions: Vec<Version> = (0..300i64)
        .map(|i| version(key, 10 + i, i as u64, b"svc", Some(b"v")))
        .collect();
    write(&src, "seg.seg", &versions, true);

    let original = read_all(&src, "seg.seg");
    let before = sha256(&original).to_hex();

    // Reading the dictionary segment never mutates it (invariant 1, STL-186).
    let reader = SegmentReader::open(&src, "seg.seg").expect("open");
    let read_back = reader.read_versions().expect("versions");
    assert_eq!(
        sha256(&read_all(&src, "seg.seg")).to_hex(),
        before,
        "reading must not mutate the sealed dictionary segment"
    );

    // Backup → restore is a byte copy into a fresh data directory; a real restore
    // verifies every byte against the segment's checksums on the way back in.
    let dst = MemDisk::new();
    {
        let mut f = dst.create("seg.seg").expect("create restore target");
        f.append(&original).expect("write restore");
        f.sync().expect("sync restore");
    }
    assert_eq!(
        sha256(&read_all(&dst, "seg.seg")).to_hex(),
        before,
        "backup/restore must be byte-identical"
    );

    // The restored copy decodes to exactly the same versions.
    let restored = SegmentReader::open(&dst, "seg.seg").expect("open restored");
    assert_eq!(
        restored.read_versions().expect("restored versions"),
        read_back
    );
    assert_eq!(
        restored.read_versions().expect("restored versions"),
        versions
    );
}
