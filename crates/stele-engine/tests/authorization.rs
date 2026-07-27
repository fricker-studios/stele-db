//! Authorization: roles, ownership, table privileges ([ADR-0034]).
//!
//! The July 2026 audit's headline finding was that authentication proved an
//! identity nothing then consumed — any session could `ALTER USER admin
//! PASSWORD` or `BACKUP` the database. These tests pin the boundary that
//! closes it, and in particular the two properties a privilege check is
//! worthless without:
//!
//! * **No unguarded entry point.** The engine has several public routes to a
//!   read or a write; a check on some of them is a check on none. Every route
//!   is exercised here against a role that holds nothing.
//! * **Policy is current, not historical.** An `AS OF` read resolves against
//!   the grants that exist *now*, so time travel cannot re-enter a revoked
//!   privilege ([10 §6]).
//!
//! [ADR-0034]: ../../../docs/adr/0034-role-based-access-control.md
//! [10 §6]: ../../../docs/10-security-and-compliance.md#6-authorization

use stele_common::provenance::Principal;
use stele_common::time::{Clock, SystemTimeMicros};
use stele_engine::{EngineError, SessionEngine, StatementOutcome};
use stele_sql::parse;
use stele_storage::backend::MemDisk;

/// A clock reading zero: the engine wraps it in its own monotonic clock, which
/// returns `max(inner, high_water + 1)`, so successive commits still get
/// distinct, increasing instants without depending on wall time.
#[derive(Clone)]
struct ZeroClock;

impl Clock for ZeroClock {
    fn now(&self) -> SystemTimeMicros {
        SystemTimeMicros(0)
    }
}

type Engine = SessionEngine<ZeroClock, MemDisk>;

fn engine() -> Engine {
    SessionEngine::open(MemDisk::new(), ZeroClock)
}

/// Run `sql` as `role`, returning the outcome.
fn run_as(engine: &mut Engine, role: &str, sql: &str) -> Result<StatementOutcome, EngineError> {
    engine.set_principal(Principal::new(role.as_bytes().to_vec()));
    let stmt = parse(sql).expect("parse").remove(0);
    engine.execute(&stmt)
}

/// Run `sql` as `role`, expecting success.
fn ok_as(engine: &mut Engine, role: &str, sql: &str) -> StatementOutcome {
    run_as(engine, role, sql).unwrap_or_else(|e| panic!("{role} should be allowed {sql:?}: {e}"))
}

/// Run `sql` as `role`, expecting a permission denial.
fn denied(engine: &mut Engine, role: &str, sql: &str) {
    match run_as(engine, role, sql) {
        Err(EngineError::PermissionDenied { .. }) => {}
        Err(other) => panic!("{role} running {sql:?}: expected a denial, got {other:?}"),
        Ok(_) => panic!("{role} running {sql:?}: expected a denial, but it succeeded"),
    }
}

/// A table owned by `alice`, with `bob` and `mallory` existing as roles.
fn seeded() -> Engine {
    let mut e = engine();
    for role in ["alice", "bob", "mallory"] {
        ok_as(
            &mut e,
            "stele",
            &format!("CREATE USER {role} PASSWORD 'pw-{role}'"),
        );
    }
    ok_as(
        &mut e,
        "alice",
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    );
    ok_as(&mut e, "alice", "INSERT INTO account VALUES (1, 100)");
    e
}

/// The creator owns what it creates and needs no grant to use it.
#[test]
fn the_creator_owns_the_table() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "SELECT id FROM account");
    ok_as(&mut e, "alice", "INSERT INTO account VALUES (2, 200)");
    ok_as(
        &mut e,
        "alice",
        "UPDATE account SET balance = 1 WHERE id = 1",
    );
    ok_as(&mut e, "alice", "DELETE FROM account WHERE id = 2");
}

/// A role with no grant reaches nothing — through **any** route.
///
/// This is the test that matters most: a privilege check placed on some entry
/// points and not others is not a boundary. Every public route to a read or a
/// write is listed here, so a future route added without a check fails loudly.
#[test]
fn a_role_without_a_grant_is_refused_on_every_route() {
    let mut e = seeded();
    for sql in [
        // Plain reads.
        "SELECT id FROM account",
        "SELECT COUNT(*) FROM account",
        // Time travel must not be a way around the check. (A bindable instant:
        // binding runs first, because the table set is only known post-bind, so
        // an out-of-range AS OF would report `BeforeHistory` before the
        // privilege check ever runs — see the audit backlog note on pre-bind
        // disclosure.)
        "SELECT id FROM account FOR SYSTEM_TIME AS OF now()",
        // Writes.
        "INSERT INTO account VALUES (9, 9)",
        "UPDATE account SET balance = 0 WHERE id = 1",
        "DELETE FROM account WHERE id = 1",
        // The introspection table functions read the same rows and their
        // provenance, so they need the same privilege.
        "SELECT * FROM stele_history('account')",
        "SELECT * FROM stele_segments('account')",
        "SELECT * FROM stele_audit('account')",
        // A join reads both sides; the unprivileged side must still refuse.
        "SELECT a.id FROM account a JOIN account b ON a.id = b.id",
        // A CTE and a subquery hide the base table one level down.
        "WITH c AS (SELECT id FROM account) SELECT id FROM c",
        "SELECT id FROM account WHERE id IN (SELECT id FROM account)",
        // EXPLAIN renders a plan over the table — and ANALYZE runs it.
        "EXPLAIN SELECT id FROM account",
        "EXPLAIN ANALYZE SELECT id FROM account",
    ] {
        denied(&mut e, "mallory", sql);
    }
}

/// A grant admits exactly the verb it names, and nothing else.
#[test]
fn a_grant_admits_only_the_granted_verb() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "GRANT SELECT ON account TO bob");

    ok_as(&mut e, "bob", "SELECT id FROM account");
    denied(&mut e, "bob", "INSERT INTO account VALUES (5, 5)");
    denied(&mut e, "bob", "UPDATE account SET balance = 0 WHERE id = 1");
    denied(&mut e, "bob", "DELETE FROM account WHERE id = 1");

    // …and the second grant admits the second verb, still not the rest.
    ok_as(&mut e, "alice", "GRANT INSERT ON account TO bob");
    ok_as(&mut e, "bob", "INSERT INTO account VALUES (5, 5)");
    denied(&mut e, "bob", "DELETE FROM account WHERE id = 5");
}

/// `REVOKE` takes the privilege back.
#[test]
fn revoke_takes_the_privilege_back() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "GRANT SELECT ON account TO bob");
    ok_as(&mut e, "bob", "SELECT id FROM account");
    ok_as(&mut e, "alice", "REVOKE SELECT ON account FROM bob");
    denied(&mut e, "bob", "SELECT id FROM account");
}

/// A `PUBLIC` grant reaches every role, including one created afterwards.
#[test]
fn a_public_grant_reaches_every_role_including_later_ones() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "GRANT SELECT ON account TO PUBLIC");
    ok_as(&mut e, "bob", "SELECT id FROM account");
    ok_as(&mut e, "mallory", "SELECT id FROM account");

    ok_as(&mut e, "stele", "CREATE USER carol PASSWORD 'pw'");
    ok_as(&mut e, "carol", "SELECT id FROM account");

    // Revoking PUBLIC closes it for everyone who had only that.
    ok_as(&mut e, "alice", "REVOKE SELECT ON account FROM PUBLIC");
    denied(&mut e, "carol", "SELECT id FROM account");
}

/// **Time travel is not a privilege-escalation channel** ([10 §6]).
///
/// An `AS OF` read resolves against the grants that exist *now*. If policy were
/// resolved at the snapshot instead, a revoked role could read history simply
/// by naming an instant before the `REVOKE` — which is precisely the escalation
/// the design forbids.
///
/// [10 §6]: ../../../docs/10-security-and-compliance.md#6-authorization
#[test]
fn an_as_of_read_enforces_current_policy_not_the_snapshots() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "GRANT SELECT ON account TO bob");
    // Take a snapshot instant while bob is allowed…
    let StatementOutcome::Rows(_) = ok_as(&mut e, "bob", "SELECT id FROM account") else {
        panic!("rows");
    };
    let while_allowed = e.commit_clock();

    ok_as(&mut e, "alice", "REVOKE SELECT ON account FROM bob");

    // …and bob cannot travel back to it.
    denied(
        &mut e,
        "bob",
        &format!(
            "SELECT id FROM account FOR SYSTEM_TIME AS OF {}",
            while_allowed.0
        ),
    );

    // The converse also holds: a *new* grant is effective for history the role
    // could not previously read. Policy is current in both directions.
    ok_as(&mut e, "alice", "GRANT SELECT ON account TO bob");
    ok_as(
        &mut e,
        "bob",
        &format!(
            "SELECT id FROM account FOR SYSTEM_TIME AS OF {}",
            while_allowed.0
        ),
    );
}

/// Holding a privilege never confers the right to pass it on.
#[test]
fn a_grantee_cannot_re_grant_or_drop() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "GRANT ALL PRIVILEGES ON account TO bob");
    ok_as(&mut e, "bob", "SELECT id FROM account");

    // Even with every privilege, bob is not the owner.
    denied(&mut e, "bob", "GRANT SELECT ON account TO mallory");
    denied(&mut e, "bob", "REVOKE SELECT ON account FROM bob");
    denied(&mut e, "bob", "DROP TABLE account");
    denied(&mut e, "bob", "CREATE INDEX ix ON account (balance)");
}

/// The audit's headline finding: role DDL and the admin verbs are
/// superuser-only.
#[test]
fn role_ddl_and_admin_verbs_are_superuser_only() {
    let mut e = seeded();
    for sql in [
        "CREATE USER eve PASSWORD 'pw'",
        // The account-takeover primitive that was wide open.
        "ALTER USER alice PASSWORD 'stolen'",
        "DROP USER alice",
        "CHECKPOINT",
        "FLUSH",
        "COMPACT",
        // Whole-database exfiltration to an arbitrary path.
        "BACKUP TO '/tmp/stele-authz-test-should-not-exist'",
    ] {
        denied(&mut e, "mallory", sql);
    }
    // The bootstrap superuser can do all of it.
    ok_as(&mut e, "stele", "CREATE USER eve PASSWORD 'pw'");
    ok_as(&mut e, "stele", "ALTER USER eve PASSWORD 'rotated'");
    ok_as(&mut e, "stele", "CHECKPOINT");
    ok_as(&mut e, "stele", "DROP USER eve");
}

/// A role may rotate **its own** password without being a superuser — that is
/// not escalation — but never anyone else's.
#[test]
fn a_role_may_rotate_only_its_own_password() {
    let mut e = seeded();
    ok_as(&mut e, "bob", "ALTER USER bob PASSWORD 'bobs-new'");
    denied(&mut e, "bob", "ALTER USER alice PASSWORD 'stolen'");
}

/// A minted superuser bypasses everything — the bootstrap path a SCRAM
/// deployment needs, since the built-in `stele` identity has no credential to
/// authenticate with.
#[test]
fn a_minted_superuser_bypasses_every_check() {
    let mut e = seeded();
    ok_as(&mut e, "stele", "CREATE USER admin SUPERUSER PASSWORD 'pw'");

    ok_as(&mut e, "admin", "SELECT id FROM account");
    ok_as(&mut e, "admin", "INSERT INTO account VALUES (7, 7)");
    ok_as(&mut e, "admin", "CREATE USER eve PASSWORD 'pw'");
    ok_as(&mut e, "admin", "CHECKPOINT");
    // Including administering a table it does not own.
    ok_as(&mut e, "admin", "GRANT SELECT ON account TO mallory");

    // Minting a superuser is itself superuser-only.
    denied(
        &mut e,
        "mallory",
        "CREATE USER eve2 SUPERUSER PASSWORD 'pw'",
    );
}

/// Privileges do not survive the object or the role they attach to.
///
/// A name re-created after a `DROP` starts a fresh era: inheriting the previous
/// era's grants would silently hand the new owner's table to whoever the old
/// owner had trusted.
#[test]
fn dropping_a_table_or_role_forgets_its_privileges() {
    let mut e = seeded();
    ok_as(&mut e, "alice", "GRANT SELECT ON account TO bob");
    ok_as(&mut e, "alice", "DROP TABLE account");
    ok_as(
        &mut e,
        "mallory",
        "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    );
    // mallory owns the new era; bob's grant on the old one is gone.
    ok_as(&mut e, "mallory", "SELECT id FROM account");
    denied(&mut e, "bob", "SELECT id FROM account");

    // Same for a re-created role.
    ok_as(&mut e, "mallory", "GRANT SELECT ON account TO bob");
    ok_as(&mut e, "stele", "DROP USER bob");
    ok_as(&mut e, "stele", "CREATE USER bob PASSWORD 'new'");
    denied(&mut e, "bob", "SELECT id FROM account");
}

/// `DROP TABLE IF EXISTS <absent>` is a no-op any role may issue.
///
/// Ownership of a table that does not exist cannot be held by anyone, so
/// demanding it would refuse the idempotent teardown every setup script — and
/// the project's own five-minute demo — opens with.
#[test]
fn drop_if_exists_on_an_absent_table_is_allowed_for_any_role() {
    let mut e = seeded();
    ok_as(&mut e, "mallory", "DROP TABLE IF EXISTS nosuchtable");
    // A *live* table still needs ownership, `IF EXISTS` or not.
    denied(&mut e, "mallory", "DROP TABLE IF EXISTS account");
    denied(&mut e, "mallory", "DROP TABLE account");
}

/// The bootstrap identity is reserved from role DDL.
///
/// Its entire security property is that it has **no stored credential**, so
/// under `auth = "scram"` no client can authenticate as it. `CREATE USER stele
/// PASSWORD …` would mint exactly that credential and hand the built-in
/// superuser to whoever chose the password — a superuser-only statement, but
/// one that converts "nobody can be `stele`" into "this person is `stele`",
/// permanently and durably.
#[test]
fn the_bootstrap_identity_cannot_be_created_rotated_or_dropped() {
    let mut e = seeded();
    for sql in [
        "CREATE USER stele PASSWORD 'hijack'",
        "ALTER USER stele PASSWORD 'hijack'",
        "DROP USER stele",
    ] {
        // Refused even for the superuser: the name belongs to the engine, not
        // to the role store.
        assert!(
            matches!(
                run_as(&mut e, "stele", sql),
                Err(EngineError::ReservedRole(_))
            ),
            "{sql} must be refused as a reserved name",
        );
    }
}

/// A nameless principal holds nothing.
///
/// Reachable directly — a client may send an empty `user` in its startup packet
/// — and it is also where a **non-UTF-8** principal lands: `current_role`
/// resolves undecodable bytes to the empty name rather than to the bootstrap
/// superuser, so a caller bug fails closed instead of becoming a total
/// authorization bypass. (`set_principal` `debug_assert`s UTF-8, so the
/// undecodable case cannot be reached from a test build; this pins the value it
/// falls back to.)
#[test]
fn a_nameless_principal_holds_nothing() {
    let mut e = seeded();
    denied(&mut e, "", "SELECT id FROM account");
    denied(&mut e, "", "INSERT INTO account VALUES (4, 4)");
    // …and specifically cannot reach the superuser-only surface.
    denied(&mut e, "", "CREATE USER eve PASSWORD 'pw'");
    denied(
        &mut e,
        "",
        "BACKUP TO '/tmp/stele-nameless-should-not-exist'",
    );
}

/// A grant must name a live table and a real role, so a typo cannot stash a
/// privilege that springs to life later.
#[test]
fn a_grant_must_name_a_live_table_and_a_real_role() {
    let mut e = seeded();
    assert!(matches!(
        run_as(&mut e, "alice", "GRANT SELECT ON nosuchtable TO bob"),
        Err(EngineError::UnknownTable(_))
    ));
    assert!(matches!(
        run_as(&mut e, "alice", "GRANT SELECT ON account TO nosuchrole"),
        Err(EngineError::UnknownUser(_))
    ));
}

/// Grants are durable: they survive a restart through the catalog log, on the
/// same hash-chained terms as schema history.
#[test]
fn grants_survive_a_restart() {
    let disk = MemDisk::new();
    {
        let mut e: Engine = SessionEngine::open(disk.clone(), ZeroClock);
        ok_as(&mut e, "stele", "CREATE USER alice PASSWORD 'pw'");
        ok_as(&mut e, "stele", "CREATE USER bob PASSWORD 'pw'");
        ok_as(
            &mut e,
            "alice",
            "CREATE TABLE account (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
        );
        ok_as(&mut e, "alice", "GRANT SELECT ON account TO bob");
    }

    let mut e: Engine = SessionEngine::recover(disk, ZeroClock).expect("recover the session");
    // The grant, the ownership, and the denial all replay.
    ok_as(&mut e, "bob", "SELECT id FROM account");
    denied(&mut e, "bob", "INSERT INTO account VALUES (1, 1)");
    ok_as(&mut e, "alice", "INSERT INTO account VALUES (1, 1)");
    // And the owner can still administer it after the restart.
    ok_as(&mut e, "alice", "REVOKE SELECT ON account FROM bob");
    denied(&mut e, "bob", "SELECT id FROM account");
}

/// A `MERGE` reads its source and writes its target, so it needs privileges on
/// both — checking only the target would let a role read a table it has no
/// access to.
#[test]
fn merge_requires_privileges_on_both_source_and_target() {
    let mut e = seeded();
    ok_as(
        &mut e,
        "alice",
        "CREATE TABLE staging (id INT PRIMARY KEY, balance INT) WITH SYSTEM VERSIONING",
    );
    ok_as(&mut e, "alice", "INSERT INTO staging VALUES (1, 500)");

    // bob may write the target but cannot read the source.
    ok_as(&mut e, "alice", "GRANT ALL PRIVILEGES ON account TO bob");
    denied(
        &mut e,
        "bob",
        "MERGE INTO account USING staging ON account.id = staging.id \
         WHEN MATCHED THEN UPDATE SET balance = staging.balance \
         WHEN NOT MATCHED THEN INSERT VALUES (staging.id, staging.balance)",
    );

    // With the source readable too, it goes through.
    ok_as(&mut e, "alice", "GRANT SELECT ON staging TO bob");
    ok_as(
        &mut e,
        "bob",
        "MERGE INTO account USING staging ON account.id = staging.id \
         WHEN MATCHED THEN UPDATE SET balance = staging.balance \
         WHEN NOT MATCHED THEN INSERT VALUES (staging.id, staging.balance)",
    );
}

/// Buffered (in-transaction) writes are authorized when they are **staged**,
/// against the role that issued the statement — not deferred to whoever
/// commits the block.
#[test]
fn transactional_writes_are_authorized_at_staging_time() {
    let mut e = seeded();
    let mut txn = e.begin();

    e.set_principal(Principal::new(b"mallory".to_vec()));
    let stmt = parse("INSERT INTO account VALUES (3, 3)")
        .expect("parse")
        .remove(0);
    assert!(
        matches!(
            e.stage_dml(&stmt, &mut txn),
            Err(EngineError::PermissionDenied { .. })
        ),
        "an unprivileged staged write must be refused at staging, not at commit"
    );
}
