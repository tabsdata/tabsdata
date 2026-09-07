//
// Copyright 2026 Tabsdata Inc.
//

//! Deltas that migrate *data*, against a real database. `migration` proves the
//! ladder lands on the right schema; these prove the rows it rewrote hold the
//! right values, and that the upgrade seam leaves project scope open for them
//! — a data delta that silently matches nothing still reports success.

use crate::components::ApiserverComponent;
use crate::config::PostgresConnect;
use crate::connection::PgConnectionExt;
use crate::pool::TdPool;
use crate::pool::options::pg_connect_options;
use crate::test_utils::server;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use td_schema::schema::archive;
use td_schema::{BYPASS_RLS_GUC, PROJECT_ID_GUC};

use super::{execute, scalars};

async fn connect(identity: &PostgresConnect) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(identity))
        .await
        .unwrap()
}

/// Every FK-satisfying row a `physical_function_runs` insert needs, at the
/// point the v2 archive is installed. Three collections exercise the
/// backfill's COALESCE ladder: a function-version override, a collection's own
/// `base_path`, and the fall-through to the project's.
async fn seed_run_chain(db: &TdPool) {
    execute(
        db,
        "INSERT INTO projects (id, name, description, base_path, created_on, created_by_id, \
           modified_on, modified_by_id) \
         VALUES ('p0', 'p0', '', '/from-project', now(), 'u0', now(), 'u0')",
    )
    .await;
    execute(
        db,
        "INSERT INTO groups (id, project_id, name, description, created_on, created_by_id, \
           modified_on, modified_by_id, system) \
         VALUES ('g0', 'p0', 'g0', '', now(), 'u0', now(), 'u0', false)",
    )
    .await;
    for (id, base_path, fn_base_path) in [
        ("c-fn-override", "'/from-collection'", "'/from-function'"),
        ("c-own", "'/from-collection'", "NULL"),
        ("c-inherits", "NULL", "NULL"),
    ] {
        execute(
            db,
            &format!(
                "INSERT INTO collections (id, project_id, name, description, created_on, \
                   created_by_id, modified_on, modified_by_id, group_id, base_path, \
                   collection_type) \
                 VALUES ('{id}', 'p0', '{id}', '', now(), 'u0', now(), 'u0', 'g0', \
                   {base_path}, 'R')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO functions (id, project_id, collection_id, name, description, \
                   decorator, runtime_values, function_id, base_path, stage_id, \
                   stage_base_path, storage_version, bundle_id, snippet, defined_on, \
                   defined_by_id, status) \
                 VALUES ('fv-{id}', 'p0', '{id}', 'f0', '', 'P', '', 'f-{id}', \
                   {fn_base_path}, 's0', '/s', 'V1', 'b0', '', now(), 'u0', 'A')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO executions (id, project_id, collection_id, function_version_id, \
                   triggered_on, triggered_by_id) \
                 VALUES ('e-{id}', 'p0', '{id}', 'fv-{id}', now(), 'u0')"
            ),
        )
        .await;
        execute(
            db,
            &format!(
                "INSERT INTO physical_function_runs (id, project_id, collection_id, \
                   function_version_id, execution_id, transaction_id, triggered_on, \
                   triggered_by_id, trigger, function_id, status) \
                 VALUES ('r-{id}', 'p0', '{id}', 'fv-{id}', 'e-{id}', 'x0', now(), 'u0', \
                   'M', 'f-{id}', 'R')"
            ),
        )
        .await;
    }
}

/// v2 → v3 stamps every existing run with the prefix it resolves to today:
/// the collection's own `base_path`, falling back to the project's.
#[tokio::test]
async fn v2_to_v3_backfills_run_base_path_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    db.admin
        .upgrade_source(archive::source(2))
        .await
        .expect("installing archive v2");
    seed_run_chain(&db.serving).await;

    db.admin
        .upgrade(&ApiserverComponent)
        .await
        .expect("upgrading v2 -> current");

    let stamped = scalars(
        &db.serving,
        "SELECT id || '=' || base_path FROM physical_function_runs ORDER BY id",
    )
    .await;
    assert_eq!(
        stamped,
        [
            "r-c-fn-override=/from-function",
            "r-c-inherits=/from-project",
            "r-c-own=/from-collection"
        ],
        "the backfill must resolve the function override, then the collection's \
         prefix, then the project's"
    );
}

/// The upgrade transaction runs outside project scope, so a data delta over a
/// project-scoped table sees every row without opting in. Asserted from inside
/// the transaction, the way a delta would observe it.
#[tokio::test]
async fn the_upgrade_transaction_is_not_project_scoped_postgres() {
    let db = crate::test_utils::postgres::pool().await;
    let mut tx = db.admin.begin_upgrade().await.unwrap();
    tx.execute_batch(&[format!(
        "DO $$ BEGIN \
           IF current_setting('{BYPASS_RLS_GUC}', true) IS DISTINCT FROM 'on' THEN \
             RAISE EXCEPTION 'upgrade transaction is project-scoped: bypass_rls = %', \
               coalesce(current_setting('{BYPASS_RLS_GUC}', true), '<unset>'); \
           END IF; \
         END $$"
    )])
    .await
    .expect("the upgrade transaction must open with project scope bypassed");
    tx.commit().await.unwrap();
}

/// Why the seam has to set it: project-scoped tables are FORCE ROW LEVEL
/// SECURITY, which binds even the table owner — and an upgrade connects as the
/// schema owner. The policy is fail-closed on an unset `app.project_id`, so an
/// UPDATE would match nothing and the ladder would still report success. The
/// rest of this suite cannot show it: its factories keep the schema
/// superuser-owned, and a superuser bypasses RLS unconditionally.
#[tokio::test]
async fn force_rls_binds_a_non_superuser_owner_postgres() {
    server::ensure_started().await;
    let su = connect(&server::admin_base()).await;
    let uid = td_common::id::id();
    let (owner, schema) = (format!("own_{uid}"), format!("sch_{uid}"));

    for stmt in [
        format!("CREATE ROLE \"{owner}\" LOGIN PASSWORD 'own_pw'"),
        format!("CREATE SCHEMA \"{schema}\" AUTHORIZATION \"{owner}\""),
    ] {
        sqlx::query(&stmt).execute(&su).await.unwrap();
    }

    let mut identity = server::admin_base();
    identity.username = owner.clone();
    identity.password = "own_pw".to_owned();
    let as_owner = connect(&identity).await;
    let pred = format!(
        "current_setting('{BYPASS_RLS_GUC}', true) = 'on' \
         OR project_id = current_setting('{PROJECT_ID_GUC}', true)"
    );
    for stmt in [
        format!("CREATE TABLE \"{schema}\".scoped (project_id text NOT NULL)"),
        format!("INSERT INTO \"{schema}\".scoped VALUES ('p0')"),
        format!("ALTER TABLE \"{schema}\".scoped ENABLE ROW LEVEL SECURITY"),
        format!("ALTER TABLE \"{schema}\".scoped FORCE ROW LEVEL SECURITY"),
        format!(
            "CREATE POLICY isolation ON \"{schema}\".scoped USING ({pred}) WITH CHECK ({pred})"
        ),
    ] {
        sqlx::query(&stmt).execute(&as_owner).await.unwrap();
    }

    let visible_rows = async |bypass_rls: bool| -> u64 {
        let mut tx = as_owner.begin().await.unwrap();
        if bypass_rls {
            tx.bypass_project_scope().await.unwrap();
        }
        let updated = sqlx::query(&format!(
            "UPDATE \"{schema}\".scoped SET project_id = project_id"
        ))
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
        tx.rollback().await.unwrap();
        updated
    };

    assert_eq!(
        visible_rows(false).await,
        0,
        "in project scope, FORCE RLS hides every row from the table owner"
    );
    assert_eq!(visible_rows(true).await, 1);

    as_owner.close().await;
    for stmt in [
        format!("DROP SCHEMA \"{schema}\" CASCADE"),
        format!("DROP ROLE \"{owner}\""),
    ] {
        sqlx::query(&stmt).execute(&su).await.unwrap();
    }
}
