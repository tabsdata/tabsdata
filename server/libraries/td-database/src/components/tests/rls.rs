//
// Copyright 2026 Tabsdata Inc.
//

//! td-database's row-level-security machinery, once the apiserver schema is
//! applied: project isolation end-to-end, the serving-role bypass guard, the
//! maintain-current trigger under RLS, and the scope GUC primitives. PostgreSQL
//! only — this whole module is `#[cfg(postgres)]`.

use crate::components::{ApiserverComponent, SchemaComponent};
use crate::config::PostgresConnect;
use crate::dynamic::DynamicConnection;
use crate::error::DbError;
use crate::pool::TdPool;
use crate::pool::connect::pg_connect_options;
use crate::test_utils::server;
use sqlx::postgres::PgPoolOptions;

/// Row-level security end to end on real Postgres: reads are scoped to the
/// project, `WITH CHECK` rejects a cross-project insert, an unset GUC fails
/// closed, and `app.bypass_rls` spans projects.
#[tokio::test]
async fn test_rls_project_isolation() {
    const INSERT: &str = "INSERT INTO collections \
        (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
        VALUES ($1, $2, 'd', now(), 'u', now(), 'u', 'g', '/', 'F')";
    const SET_PROJECT: &str = "SELECT set_config('app.project_id', $1, true)";
    const COUNT: &str = "SELECT count(*) FROM collections";

    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();

    // Provision a NOBYPASSRLS app role and connect a second pool as it, via a
    // raw connection as the pool's bootstrap (superuser) role.
    let su = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(&server::admin_base()))
        .await
        .unwrap();
    let schema = db.schema();
    // A unique login role (roles are cluster-global on the shared server, so a fixed
    // name would collide across tests/runs); NOBYPASSRLS so RLS actually applies.
    let app_user = format!("u_{}", td_common::id::id());
    for stmt in [
        format!("CREATE ROLE \"{app_user}\" LOGIN PASSWORD 'app_pw' NOBYPASSRLS"),
        format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{app_user}\""),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{app_user}\""
        ),
        format!("ALTER ROLE \"{app_user}\" SET search_path TO \"{schema}\""),
        // Collections FK groups(project_id, id); the schema seeds no project on
        // Postgres, so seed a group per test project. `su` bypasses RLS, so set
        // project_id explicitly.
        format!(
            "INSERT INTO \"{schema}\".groups (id, project_id, name, description, created_on, created_by_id, modified_on, modified_by_id, system) VALUES ('g', 'A', 'g', 'd', now(), 'u', now(), 'u', false)"
        ),
        format!(
            "INSERT INTO \"{schema}\".groups (id, project_id, name, description, created_on, created_by_id, modified_on, modified_by_id, system) VALUES ('g', 'B', 'g', 'd', now(), 'u', now(), 'u', false)"
        ),
    ] {
        sqlx::query(&stmt).execute(&su).await.unwrap();
    }

    let app_connect = PostgresConnect {
        options: server::admin_base().options,
        username: app_user.clone(),
        password: "app_pw".to_string(),
    };
    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(pg_connect_options(&app_connect))
        .await
        .unwrap();

    // Project A inserts a collection, omitting project_id — the column default
    // reads app.project_id, so the row is tagged 'A'.
    let mut t = app.begin().await.unwrap();
    sqlx::query(SET_PROJECT)
        .bind("A")
        .execute(&mut *t)
        .await
        .unwrap();
    sqlx::query(INSERT)
        .bind("c_a")
        .bind("ca")
        .execute(&mut *t)
        .await
        .unwrap();
    let pid: String = sqlx::query_scalar("SELECT project_id FROM collections WHERE id = 'c_a'")
        .fetch_one(&mut *t)
        .await
        .unwrap();
    assert_eq!(pid, "A", "omitted project_id should default to the GUC");
    t.commit().await.unwrap();

    // Project B cannot see A's row.
    let mut t = app.begin().await.unwrap();
    sqlx::query(SET_PROJECT)
        .bind("B")
        .execute(&mut *t)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(n, 0, "project B must not see project A rows");
    t.rollback().await.unwrap();

    // WITH CHECK rejects an insert tagged for another project.
    let mut t = app.begin().await.unwrap();
    sqlx::query(SET_PROJECT)
        .bind("B")
        .execute(&mut *t)
        .await
        .unwrap();
    let bad = sqlx::query(
        "INSERT INTO collections \
         (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type, project_id) \
         VALUES ('x', 'x', 'd', now(), 'u', now(), 'u', 'g', '/', 'F', 'A')",
    )
    .execute(&mut *t)
    .await;
    assert!(
        bad.is_err(),
        "WITH CHECK must reject a cross-project insert"
    );
    t.rollback().await.unwrap();

    // Project B inserts its own collection.
    let mut t = app.begin().await.unwrap();
    sqlx::query(SET_PROJECT)
        .bind("B")
        .execute(&mut *t)
        .await
        .unwrap();
    sqlx::query(INSERT)
        .bind("c_b")
        .bind("cb")
        .execute(&mut *t)
        .await
        .unwrap();
    t.commit().await.unwrap();

    // Project A sees only its own row.
    let mut t = app.begin().await.unwrap();
    sqlx::query(SET_PROJECT)
        .bind("A")
        .execute(&mut *t)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(n, 1, "project A should see exactly its own collection");
    t.rollback().await.unwrap();

    // Unset GUC fails closed: zero rows, no error.
    let mut t = app.begin().await.unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(
        n, 0,
        "an unset app.project_id must see no rows (fail-closed)"
    );
    t.rollback().await.unwrap();

    // Bypass spans projects.
    let mut t = app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.bypass_rls', 'on', true)")
        .execute(&mut *t)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(n, 2, "bypass should see every project's rows");
    t.rollback().await.unwrap();

    // Scope GUCs are transaction-local: on an autocommit connection they do
    // not outlive their own statement, and the next read runs fail-closed.
    sqlx::query("SELECT set_config('app.bypass_rls', 'on', true)")
        .execute(&app)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&app).await.unwrap();
    assert_eq!(
        n, 0,
        "an autocommit-set bypass must not outlive its statement (fail-closed)"
    );

    // Drop the per-test login role. Its privileges depend on the schema's tables,
    // so `DROP OWNED BY` clears those first; the schema + per-test app role are
    // dropped by `TestDb::drop`.
    app.close().await;
    for stmt in [
        format!("DROP OWNED BY \"{app_user}\""),
        format!("DROP ROLE \"{app_user}\""),
    ] {
        sqlx::query(&stmt).execute(&su).await.unwrap();
    }
}

/// `check()` refuses to start if the **serving** role bypasses RLS. A
/// misconfigured `td_app` with `BYPASSRLS` would silently collapse project
/// isolation; `check_rls_posture` on the serving pool must catch it.
#[tokio::test]
async fn test_check_rejects_bypassing_serving_role() {
    use crate::config::{ConnectConfig, DatabaseConfig};

    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    // The serving role is NOBYPASSRLS, so check passes.
    ApiserverComponent
        .check(&db.serving)
        .await
        .expect("NOBYPASSRLS serving role should pass");

    // Create a **own** BYPASSRLS role (not the shared `td_app`) and build a serving
    // pool that `SET ROLE`s it, so the misconfiguration stays isolated to this test.
    // A unique name — roles are cluster-global on the shared server.
    let su = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(&server::admin_base()))
        .await
        .unwrap();
    let bypassing = format!("b_{}", td_common::id::id());
    sqlx::query(&format!("CREATE ROLE \"{bypassing}\" BYPASSRLS"))
        .execute(&su)
        .await
        .unwrap();

    // Pin the bypassing role so the pool's connections `SET ROLE` it (the
    // superuser may switch to any role).
    let cfg = DatabaseConfig {
        conn: ConnectConfig::Postgres(server::admin_base()),
        schema: Some(db.schema().clone()),
        app_role: Some(bypassing.clone()),
        pool: Default::default(),
    };
    let other = TdPool::connect(&cfg).await.unwrap();
    let res = other.check_rls_posture().await;
    assert!(
        matches!(res, Err(DbError::ConnectedRoleBypassesRls)),
        "a BYPASSRLS serving role must be rejected, got {res:?}"
    );

    // Drop the per-test role (close the probe pool first so it isn't in use).
    other.shutdown().await;
    sqlx::query(&format!("DROP ROLE IF EXISTS \"{bypassing}\""))
        .execute(&su)
        .await
        .unwrap();
}

/// A maintain-current trigger's write into the `_current` table succeeds under
/// RLS, and FK validation across RLS tables works; project B then sees neither
/// row.
#[tokio::test]
#[allow(irrefutable_let_patterns)]
async fn test_rls_maintain_current_trigger_under_role() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();

    let mut tx = db.begin().await.unwrap();
    {
        let mut c = tx.as_mut_conn();
        c.set_project_scope("A").await.unwrap();
        let DynamicConnection::Postgres(h) = &mut c else {
            unreachable!()
        };
        // Collections FK groups(project_id, id); project_id defaults to the scope
        // GUC ('A'), satisfying RLS WITH CHECK and the collection's FK.
        sqlx::query(
            "INSERT INTO groups (id, name, description, created_on, created_by_id, modified_on, modified_by_id, system) \
             VALUES ('g', 'g', 'd', now(), 'u', now(), 'u', false)",
        )
        .execute(&mut **h)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO collections (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
             VALUES ('col', 'c', 'd', now(), 'u', now(), 'u', 'g', '/', 'F')",
        )
        .execute(&mut **h)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO functions \
             (id, collection_id, name, description, decorator, runtime_values, function_id, \
              stage_id, stage_base_path, storage_version, bundle_id, snippet, defined_on, defined_by_id, status) \
             VALUES ('fn1', 'col', 'f', 'd', 'dec', '{}', 'fid', 'stg', '/', '1', 'bun', 'snip', now(), 'u', 'active')",
        )
        .execute(&mut **h)
        .await
        .unwrap();
        let cur: i64 = sqlx::query_scalar("SELECT count(*) FROM functions_current")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        assert_eq!(
            cur, 1,
            "maintain-current trigger wrote functions_current under RLS"
        );
        let pid: String = sqlx::query_scalar(
            "SELECT project_id FROM functions_current WHERE function_id = 'fid'",
        )
        .fetch_one(&mut **h)
        .await
        .unwrap();
        assert_eq!(pid, "A", "trigger copied the source project_id");
    }
    tx.commit().await.unwrap();

    let mut tx = db.begin().await.unwrap();
    {
        let mut c = tx.as_mut_conn();
        c.set_project_scope("B").await.unwrap();
        let DynamicConnection::Postgres(h) = &mut c else {
            unreachable!()
        };
        let f: i64 = sqlx::query_scalar("SELECT count(*) FROM functions")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        let fc: i64 = sqlx::query_scalar("SELECT count(*) FROM functions_current")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        assert_eq!(
            (f, fc),
            (0, 0),
            "project B sees neither functions nor functions_current rows"
        );
    }
    tx.rollback().await.unwrap();
}

/// The scope GUC primitives set the expected session variables, and
/// `set_project_scope` clears a prior bypass.
#[tokio::test]
#[allow(irrefutable_let_patterns)]
async fn test_set_rls_guc_primitive() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();

    {
        let mut tx = db.begin().await.unwrap();
        let mut conn = tx.as_mut_conn();
        conn.bypass_project_scope().await.unwrap();
        conn.set_project_scope("PROJ_X").await.unwrap();
        let DynamicConnection::Postgres(h) = &mut conn else {
            unreachable!()
        };
        let project: String = sqlx::query_scalar("SELECT current_setting('app.project_id', true)")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        let bypass: String = sqlx::query_scalar("SELECT current_setting('app.bypass_rls', true)")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        assert_eq!(project, "PROJ_X");
        assert_eq!(bypass, "off", "set_project_scope must clear bypass");
    }

    {
        let mut tx = db.begin().await.unwrap();
        let mut conn = tx.as_mut_conn();
        conn.bypass_project_scope().await.unwrap();
        let DynamicConnection::Postgres(h) = &mut conn else {
            unreachable!()
        };
        let bypass: String = sqlx::query_scalar("SELECT current_setting('app.bypass_rls', true)")
            .fetch_one(&mut **h)
            .await
            .unwrap();
        assert_eq!(bypass, "on");
    }
}
