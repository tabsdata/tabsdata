//
// Copyright 2026 Tabsdata Inc.
//

//! The SCD2 close-out trigger against a real database: interval chaining,
//! same-instant tie-break, out-of-order splice, concurrent inserts, the
//! insert-only guard's `valid_to` carve-out, and the attempt tables' void.

use crate::components::ApiserverComponent;
use crate::pool::TdPool;

use super::{at_rest, execute, scalars};

async fn seed_prelude(db: &TdPool) {
    execute(
        db,
        "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
           modified_on, modified_by_id) \
         VALUES ('p0', 'p0', '', '/p', now(), 'u0', now(), 'u0')",
    )
    .await;
    execute(
        db,
        "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
           modified_on, modified_by_id, system, kind) \
         VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false, 'custom')",
    )
    .await;
    execute(
        db,
        "INSERT INTO collections (id, project_id, name, description, created_on, \
           created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
         VALUES ('c0', 'p0', 'c0', '', now(), 'u0', now(), 'u0', 'g0', NULL, 'R')",
    )
    .await;
}

fn insert_function_version(counter: usize, function_id: &str, valid_from: &str) -> String {
    let id = at_rest(counter);
    format!(
        "INSERT INTO functions (id, project_id, collection_id, name, description, decorator, \
           runtime_values, function_id, stage_id, stage_base_path, storage_version, bundle_id, \
           snippet, valid_from, defined_by_id, status) \
         VALUES ({id}, 'p0', 'c0', 'f0', '', 'P', '', '{function_id}', 's0', '/s', 'V1', \
           'b0', '', '{valid_from}', 'u0', 'A')"
    )
}

async fn interval_chain(db: &TdPool, function_id: &str) -> Vec<String> {
    scalars(
        db,
        &format!(
            "SELECT id || '=' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'YYYY-MM-DD') END \
             FROM functions WHERE function_id = '{function_id}' ORDER BY valid_from, id"
        ),
    )
    .await
}

/// In-order inserts chain intervals: each insert closes its predecessor at
/// the new `valid_from` and lands open; a same-instant pair leaves the
/// lower-id row with a zero-length interval — invisible at every T.
#[tokio::test]
async fn scd2_close_out_chains_and_tie_breaks_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    seed_prelude(&db.serving).await;

    for (counter, on) in [
        (1, "2026-01-01T00:00:00Z"),
        (2, "2026-01-02T00:00:00Z"),
        (3, "2026-01-03T00:00:00Z"),
        (4, "2026-01-03T00:00:00Z"),
    ] {
        execute(&db.serving, &insert_function_version(counter, "f-a", on)).await;
    }
    assert_eq!(
        interval_chain(&db.serving, "f-a").await,
        [
            format!("{}=2026-01-02", at_rest(1)),
            format!("{}=2026-01-03", at_rest(2)),
            format!("{}=2026-01-03", at_rest(3)),
            format!("{}=infinity", at_rest(4))
        ]
    );

    let visible_at_tie = scalars(
        &db.serving,
        "SELECT id::text FROM functions WHERE function_id = 'f-a' \
           AND valid_from <= '2026-01-03T00:00:00Z' AND valid_to > '2026-01-03T00:00:00Z'",
    )
    .await;
    assert_eq!(
        visible_at_tie,
        [at_rest(4)],
        "at the shared instant only the highest-id row is visible"
    );
}

/// An out-of-order insert splices into the chain: it closes the covering
/// predecessor at its own `valid_from` and takes its successor's start as its
/// `valid_to` — the open row stays open.
#[tokio::test]
async fn scd2_close_out_splices_out_of_order_inserts_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    seed_prelude(&db.serving).await;

    execute(
        &db.serving,
        &insert_function_version(1, "f-a", "2026-01-01T00:00:00Z"),
    )
    .await;
    execute(
        &db.serving,
        &insert_function_version(3, "f-a", "2026-01-03T00:00:00Z"),
    )
    .await;
    execute(
        &db.serving,
        &insert_function_version(2, "f-a", "2026-01-02T00:00:00Z"),
    )
    .await;

    assert_eq!(
        interval_chain(&db.serving, "f-a").await,
        [
            format!("{}=2026-01-02", at_rest(1)),
            format!("{}=2026-01-03", at_rest(2)),
            format!("{}=infinity", at_rest(3))
        ]
    );
}

/// Two transactions inserting into the same partition serialize on the
/// trigger's per-partition advisory lock: afterwards the chain is intact and
/// exactly one row is open (the unique partial index enforces the same).
#[tokio::test]
async fn scd2_close_out_serializes_concurrent_inserts_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    seed_prelude(&db.serving).await;

    let mut tx1 = db.serving.as_ref().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(insert_function_version(
        1,
        "f-a",
        "2026-01-01T00:00:00Z",
    )))
    .execute(&mut *tx1)
    .await
    .unwrap();

    let pool = db.serving.as_ref().clone();
    let racer = tokio::spawn(async move {
        let mut tx2 = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(insert_function_version(
            2,
            "f-a",
            "2026-01-02T00:00:00Z",
        )))
        .execute(&mut *tx2)
        .await
        .unwrap();
        tx2.commit().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    tx1.commit().await.unwrap();
    racer.await.unwrap();

    assert_eq!(
        interval_chain(&db.serving, "f-a").await,
        [
            format!("{}=2026-01-02", at_rest(1)),
            format!("{}=infinity", at_rest(2))
        ]
    );
    let open = scalars(
        &db.serving,
        "SELECT id::text FROM functions WHERE function_id = 'f-a' AND valid_to = 'infinity'",
    )
    .await;
    assert_eq!(open, [at_rest(2)], "exactly one open row per partition");
}

/// The insert-only guard on event logs refuses real-column UPDATEs but lets
/// the close-out's `valid_to`-only UPDATE through: inserting a second patch
/// version must succeed (it updates the first row's `valid_to`), while a
/// direct UPDATE of any other column still raises.
#[tokio::test]
async fn scd2_immutable_guard_carves_out_valid_to_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    seed_prelude(&db.serving).await;
    execute(
        &db.serving,
        &insert_function_version(1, "f-a", "2026-01-01T00:00:00Z"),
    )
    .await;
    let parent = at_rest(1);
    for (counter, on) in [(11, "2026-01-02T00:00:00Z"), (12, "2026-01-03T00:00:00Z")] {
        let id = at_rest(counter);
        execute(
            &db.serving,
            &format!(
                "INSERT INTO function_patches (id, project_id, parent_function_version_id, \
                   function_id, collection_id, snippet, bundle_id, runtime_values, description, \
                   valid_from, defined_by_id, status) \
                 VALUES ({id}, 'p0', {parent}, 'f-a', 'c0', '', 'b0', '', '', '{on}', 'u0', 'A')"
            ),
        )
        .await;
    }

    let chains = scalars(
        &db.serving,
        "SELECT id || '=' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
           ELSE to_char(valid_to AT TIME ZONE 'UTC', 'YYYY-MM-DD') END \
         FROM function_patches ORDER BY id",
    )
    .await;
    assert_eq!(
        chains,
        [
            format!("{}=2026-01-03", at_rest(11)),
            format!("{}=infinity", at_rest(12))
        ],
        "the close-out UPDATE must pass the insert-only guard"
    );

    let patch = at_rest(12);
    let err = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE function_patches SET description = 'x' WHERE id = {patch}"
    )))
    .execute(db.serving.as_ref())
    .await
    .expect_err("real-column UPDATE must be refused");
    assert!(
        err.to_string().contains("function_patches is insert-only"),
        "{err}"
    );
    let err = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM function_patches WHERE id = {patch}"
    )))
    .execute(db.serving.as_ref())
    .await
    .expect_err("DELETE must be refused");
    assert!(
        err.to_string().contains("function_patches is insert-only"),
        "{err}"
    );
}

async fn seed_run_prelude(db: &TdPool) {
    seed_prelude(db).await;
    let (fv, ep, tx, fr) = (at_rest(1), at_rest(1), at_rest(1), at_rest(1));
    for stmt in [
        insert_function_version(1, "f-a", "2026-01-01T00:00:00Z"),
        format!(
            "INSERT INTO executions (id, project_id, name, collection_id, function_version_id, \
               triggered_on, triggered_by_id, function_id) \
             VALUES ({ep}, 'p0', 'e0', 'c0', {fv}, '2026-01-01T00:00:00Z', 'u0', 'f-a')"
        ),
        format!(
            "INSERT INTO transactions (id, project_id, collection_id, execution_id, transaction_by, \
               transaction_key, triggered_on, triggered_by_id, group_id) \
             VALUES ({tx}, 'p0', 'c0', {ep}, 'F', 'k0', '2026-01-01T00:00:00Z', 'u0', 'g0')"
        ),
        format!(
            "INSERT INTO logical_function_runs (id, project_id, collection_id, execution_id, \
               transaction_id, function_id, function_version_id, trigger, triggered_on, \
               triggered_by_id, status, group_id) \
             VALUES ({fr}, 'p0', 'c0', {ep}, {tx}, 'f-a', {fv}, 'M', '2026-01-01T00:00:00Z', \
               'u0', 'S', 'g0')"
        ),
    ] {
        execute(db, &stmt).await;
    }
}

fn insert_attempt(counter: usize, valid_from: &str) -> String {
    let (id, fv, ep, tx, fr) = (
        at_rest(counter),
        at_rest(1),
        at_rest(1),
        at_rest(1),
        at_rest(1),
    );
    format!(
        "INSERT INTO physical_function_runs (id, project_id, collection_id, \
           function_version_id, execution_id, transaction_id, valid_from, triggered_by_id, \
           trigger, function_id, status, base_path, logical_function_run_id) \
         VALUES ({id}, 'p0', 'c0', {fv}, {ep}, {tx}, '{valid_from}', 'u0', 'M', \
           'f-a', 'D', '/p', {fr})"
    )
}

async fn attempt_chain(db: &TdPool) -> Vec<String> {
    scalars(
        db,
        "SELECT id || '=' || CASE WHEN valid_to = 'infinity' THEN 'infinity' \
             ELSE to_char(valid_to AT TIME ZONE 'UTC', 'HH24') END \
           FROM physical_function_runs ORDER BY id",
    )
    .await
}

#[tokio::test]
async fn scd2_attempts_chain_on_valid_from_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    seed_run_prelude(&db.serving).await;

    execute(&db.serving, &insert_attempt(1, "2026-01-01T01:00:00Z")).await;
    assert_eq!(
        attempt_chain(&db.serving).await,
        [format!("{}=infinity", at_rest(1))]
    );

    execute(&db.serving, &insert_attempt(2, "2026-01-01T02:00:00Z")).await;
    assert_eq!(
        attempt_chain(&db.serving).await,
        [
            format!("{}=02", at_rest(1)),
            format!("{}=infinity", at_rest(2))
        ]
    );

    let at_first = scalars(
        &db.serving,
        "SELECT id::text FROM physical_function_runs \
           WHERE valid_from <= '2026-01-01T01:30:00Z' AND valid_to > '2026-01-01T01:30:00Z'",
    )
    .await;
    assert_eq!(at_first, [at_rest(1)]);
}

/// A void closes the open interval by hand and opens nothing.
#[tokio::test]
async fn scd2_attempts_void_leaves_no_current_row_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin.upgrade(&ApiserverComponent).await.unwrap();
    seed_run_prelude(&db.serving).await;
    execute(&db.serving, &insert_attempt(1, "2026-01-01T01:00:00Z")).await;
    execute(&db.serving, &insert_attempt(2, "2026-01-01T02:00:00Z")).await;

    let void = |counter: usize| {
        let id = at_rest(counter);
        format!(
            "UPDATE physical_function_runs SET valid_to = '2026-01-01T03:00:00Z' \
               WHERE id = {id} AND valid_to = 'infinity'"
        )
    };
    let closed = sqlx::query(sqlx::AssertSqlSafe(void(2)))
        .execute(db.serving.as_ref())
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(closed, 1);
    assert_eq!(
        attempt_chain(&db.serving).await,
        [format!("{}=02", at_rest(1)), format!("{}=03", at_rest(2))]
    );

    let open = scalars(
        &db.serving,
        "SELECT id::text FROM physical_function_runs WHERE valid_to = 'infinity'",
    )
    .await;
    assert!(
        open.is_empty(),
        "a voided run has no current attempt: {open:?}"
    );

    let again = sqlx::query(sqlx::AssertSqlSafe(void(1)))
        .execute(db.serving.as_ref())
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(again, 0, "an already closed interval is left alone");
}
