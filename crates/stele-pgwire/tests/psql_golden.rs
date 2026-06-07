//! Postgres text-encoder **golden** over the wire (STL-150).
//!
//! Closes the end-to-end half of STL-105's Definition of Done. STL-105 (PR #51)
//! landed the per-type text encoders ([`stele_pgwire::text_format`]) and asserted
//! them against Postgres goldens in *in-crate unit tests*. The `psql -c`
//! round-trip that DoD actually asked for could not run then: there was no
//! table-read wire path. STL-131/147 stood that path up, so this test finally
//! drives a **stock `tokio-postgres` client** against a live [`Server`], reads one
//! row per v0.1 scalar type back over the wire, and diffs the rendered cells
//! against a committed baseline (`tests/golden/psql_types.txt`) — the exact
//! bytes Postgres prints for the same values.
//!
//! No external `psql` binary is required (the CI image ships none): the DoD's
//! "`psql -c` *or an equivalent in-process pg client*" is satisfied with
//! `tokio-postgres` over the simple-query protocol — the slice v0.1 speaks, the
//! same client the STL-147 CRUD test uses.
//!
//! ## Why the rows are staged in-process
//!
//! The *read* — the encoder under test — is over the wire for every type. The
//! *write* is in-process, because at v0.1 the SQL `INSERT` path cannot express
//! the full set: [`bind_dml`](stele_sql) folds only int/text/bool literals and
//! rejects a `TIMESTAMP`/`DATE` literal (no civil-time literal codec — it mirrors
//! the `AS OF` stance). Staging every row through the typed
//! [`SessionEngine::insert`] with the value's canonical encoding side-steps that
//! write-side gap so the test exercises the one thing it is about: the wire
//! rendering. (`INSERT` over the wire for the types it *does* support is already
//! covered by the STL-147 CRUD round-trip.)
//!
//! ## NULL is deliberately out of scope here
//!
//! The DoD also names a NULL cell. A genuine SQL `NULL` from a *table read* is
//! not representable at v0.1: the payload is `Vec<u8>` end to end (storage →
//! `Column::Bytes` → `SelectResult` → `decode_result_rows`, which always yields a
//! present value), so nullability would need plumbing across four crates — a
//! feature, not this test. The NULL wire sentinel (length `-1`) is already
//! covered by the in-crate `data_row_payload` unit tests; the end-to-end NULL
//! cell is filed as a follow-up (see the PR / STL-150 comment).

use std::sync::{Arc, Mutex};

use stele_common::provenance::{Principal, TxnId};
use stele_common::time::SystemClock;
use stele_common::types::ScalarValue;
use stele_engine::SessionEngine;
use stele_pgwire::SharedSession;
use stele_storage::backend::MemDisk;
use stele_storage::delta::BusinessKey;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

/// The committed Postgres baseline: one `label|rendered` line per scalar type,
/// where `rendered` is the exact text-format output Postgres prints for the
/// staged value. (The `label|` framing is test scaffolding; the bytes after the
/// `|` are the part held to the byte-for-byte contract.)
const GOLDEN: &str = include_str!("golden/psql_types.txt");

/// One sample per v0.1 scalar type: the table to stand up, its SQL column type,
/// the value to stage, and the golden label.
///
/// The values are the same ones the [`stele_pgwire::text_format`] unit tests pin
/// to known Postgres output, so the golden is anchored to a verified rendering:
/// `i32::MAX` / `i64::MAX`, a UTF-8 string carrying non-ASCII, `true` → `t`, and
/// the instant `2023-11-14 22:13:20Z` as both a microsecond timestamp and its
/// day-count date.
struct Case {
    table: &'static str,
    sql_type: &'static str,
    value: ScalarValue,
    label: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            table: "t_int4",
            sql_type: "INT",
            value: ScalarValue::Int4(i32::MAX),
            label: "int4",
        },
        Case {
            table: "t_int8",
            sql_type: "BIGINT",
            value: ScalarValue::Int8(i64::MAX),
            label: "int8",
        },
        Case {
            table: "t_text",
            sql_type: "TEXT",
            value: ScalarValue::Text("hello, 世界".to_owned()),
            label: "text",
        },
        Case {
            table: "t_bool",
            sql_type: "BOOL",
            value: ScalarValue::Bool(true),
            label: "bool",
        },
        Case {
            table: "t_ts",
            sql_type: "TIMESTAMP",
            value: ScalarValue::Timestamp(1_700_000_000_000_000),
            label: "timestamp",
        },
        Case {
            table: "t_date",
            sql_type: "DATE",
            value: ScalarValue::Date(19_675),
            label: "date",
        },
    ]
}

/// Stand up `table` as a two-column `(id INT, v <sql_type>)` system-versioned
/// table and stage a single row `(1, value)` through the typed write path, with
/// `value` in its canonical encoding — the same bytes the SQL `INSERT` path would
/// have written, so the wire read renders identically either way.
fn stage(engine: &mut SessionEngine<SystemClock, MemDisk>, case: &Case) {
    let create = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, v {}) WITH SYSTEM VERSIONING",
        case.table, case.sql_type
    );
    let stmt = stele_sql::parse(&create)
        .expect("parse CREATE")
        .into_iter()
        .next()
        .expect("one statement");
    engine.execute(&stmt).expect("create table");

    // Both columns are read back — and decoded — over the wire, so each must
    // carry its canonical encoding, exactly as the SQL `INSERT` path would write
    // it. The `id` key is `int4` 1; `v` is the sample value.
    let mut key = Vec::new();
    ScalarValue::Int4(1).encode(&mut key);
    let mut payload = Vec::new();
    case.value.encode(&mut payload);
    engine
        .insert(
            case.table,
            BusinessKey::new(key),
            None,
            payload,
            0,
            TxnId(1),
            Principal::new(b"stele".to_vec()),
        )
        .expect("stage row");
}

/// The text of column `col` in the single data row of a `simple_query` reply.
fn cell(messages: &[SimpleQueryMessage], col: &str) -> String {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(col).expect("a non-null cell").to_owned()),
            _ => None,
        })
        .expect("a data row in the reply")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn psql_text_format_matches_postgres_golden() {
    let cases = cases();

    // Stage every sample on one fresh session, then share it behind the server.
    let mut engine = SessionEngine::open(MemDisk::new(), SystemClock);
    for case in &cases {
        stage(&mut engine, case);
    }
    let session: SharedSession = Arc::new(Mutex::new(engine));
    let addr = common::spawn_server(session).await;

    let (client, connection) = tokio_postgres::connect(&common::conn_str(addr), NoTls)
        .await
        .expect("connect to the stele pgwire server");
    let driver = tokio::spawn(connection);

    // Read each sample back over the wire and render it as a golden line. The
    // engine projects `(id, v)` regardless of the SELECT list; we read the `v`
    // cell — the typed payload whose encoding is under test.
    let mut rendered = String::new();
    for case in &cases {
        let messages = client
            .simple_query(&format!("SELECT id, v FROM {}", case.table))
            .await
            .expect("select over the wire");
        rendered.push_str(case.label);
        rendered.push('|');
        rendered.push_str(&cell(&messages, "v"));
        rendered.push('\n');
    }

    assert_eq!(
        rendered, GOLDEN,
        "the wire text rendering must match the committed Postgres golden byte-for-byte"
    );

    drop(client);
    let _ = driver.await;
}
