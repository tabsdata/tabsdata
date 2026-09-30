//
// Copyright 2026 Tabsdata Inc.
//

//! td-database's row-level-security machinery, once the apiserver schema is
//! applied: project isolation end-to-end, the serving-role bypass guard, the
//! SCD2 close-out trigger under RLS, and the scope primitives. PostgreSQL
//! only.

use crate::components::{ApiserverComponent, SchemaComponent};
use crate::config::{PostgresConnect, RlsRoles};
use crate::connection::PgConnectionExt;
use crate::error::DbError;
use crate::pool::TdPool;
use crate::pool::options::pg_connect_options;
use crate::test_utils::domain;
use sqlx::postgres::PgPoolOptions;
use td_schema::ACROSS_PROJECTS_ROLE;

/// Scope one transaction the way a serving connection does.
async fn scope(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>, tenant: &str, project: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!("SET LOCAL ROLE \"{tenant}\"")))
        .execute(&mut **tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('app.project_id', $1, true)")
        .bind(project)
        .execute(&mut **tx)
        .await
        .unwrap();
}

/// Row-level security end to end on real Postgres: reads are scoped to the
/// project, `WITH CHECK` rejects a cross-project insert, an unset GUC fails
/// closed, and the `across_projects` role spans projects.
#[tokio::test]
async fn test_rls_project_isolation() {
    const INSERT: &str = "INSERT INTO collections \
        (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
        VALUES ($1, $2, 'd', now(), 'u', now(), 'u', $3, '/', 'F')";
    const COUNT: &str = "SELECT count(*) FROM collections";

    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();

    // Mirror the serving shape: a login that can switch into either RLS role
    // but inherits neither, so an unswitched transaction is fail-closed.
    let su = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(&domain::admin_base()))
        .await
        .unwrap();
    let schema = db.schema();
    // A unique login role (roles are cluster-global on the shared server, so a fixed
    // name would collide across tests/runs); NOBYPASSRLS so RLS actually applies.
    let uid = td_common::id::id();
    let app_user = format!("u_{uid}");
    let tenant = format!("ip_{uid}");
    let across = format!("ap_{uid}");
    for stmt in [
        format!("CREATE ROLE \"{app_user}\" LOGIN PASSWORD 'app_pw' NOBYPASSRLS"),
        format!("CREATE ROLE \"{tenant}\" NOBYPASSRLS"),
        format!("CREATE ROLE \"{across}\" NOBYPASSRLS"),
        format!("GRANT \"{ACROSS_PROJECTS_ROLE}\" TO \"{across}\""),
        // Switchable but not inherited, so an unswitched transaction is
        // fail-closed and pins the policies alone. Provisioning grants the
        // owner inheriting memberships instead (upgrades need them); the
        // owner half is covered in `delta_data`.
        format!("GRANT \"{tenant}\" TO \"{app_user}\" WITH INHERIT FALSE, SET TRUE"),
        format!("GRANT \"{across}\" TO \"{app_user}\" WITH INHERIT FALSE, SET TRUE"),
        format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{tenant}\", \"{across}\", \"{app_user}\""),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{schema}\" \
             TO \"{tenant}\", \"{across}\", \"{app_user}\""
        ),
        format!("ALTER ROLE \"{app_user}\" SET search_path TO \"{schema}\""),
        // Collections FK groups(id); the schema seeds no project on Postgres,
        // so seed a group per test project. `su` bypasses RLS, so set
        // project_id explicitly.
        format!(
            "INSERT INTO \"{schema}\".groups (id, project_id, name, description, created_on, created_by_id, modified_on, modified_by_id, system, kind) VALUES ('g_a', 'A', 'g', 'd', now(), 'u', now(), 'u', false, 'custom')"
        ),
        format!(
            "INSERT INTO \"{schema}\".groups (id, project_id, name, description, created_on, created_by_id, modified_on, modified_by_id, system, kind) VALUES ('g_b', 'B', 'g', 'd', now(), 'u', now(), 'u', false, 'custom')"
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }

    let app_connect = PostgresConnect {
        options: domain::admin_base().options,
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
    scope(&mut t, &tenant, "A").await;
    sqlx::query(INSERT)
        .bind("c_a")
        .bind("ca")
        .bind("g_a")
        .execute(&mut *t)
        .await
        .unwrap();
    let pid: String = sqlx::query_scalar("SELECT project_id FROM collections WHERE id = 'c_a'")
        .fetch_one(&mut *t)
        .await
        .unwrap();
    assert_eq!(pid, "A", "omitted project_id should default to the GUC");
    t.commit().await.unwrap();

    let mut t = app.begin().await.unwrap();
    scope(&mut t, &tenant, "B").await;
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(n, 0, "project B must not see project A rows");
    t.rollback().await.unwrap();

    let mut t = app.begin().await.unwrap();
    scope(&mut t, &tenant, "B").await;
    let bad = sqlx::query(
        "INSERT INTO collections \
         (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type, project_id) \
         VALUES ('x', 'x', 'd', now(), 'u', now(), 'u', 'g_a', '/', 'F', 'A')",
    )
    .execute(&mut *t)
    .await;
    assert!(
        bad.is_err(),
        "WITH CHECK must reject a cross-project insert"
    );
    t.rollback().await.unwrap();

    let mut t = app.begin().await.unwrap();
    scope(&mut t, &tenant, "B").await;
    sqlx::query(INSERT)
        .bind("c_b")
        .bind("cb")
        .bind("g_b")
        .execute(&mut *t)
        .await
        .unwrap();
    t.commit().await.unwrap();

    let mut t = app.begin().await.unwrap();
    scope(&mut t, &tenant, "A").await;
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(n, 1, "project A should see exactly its own collection");
    t.rollback().await.unwrap();

    let mut t = app.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("SET LOCAL ROLE \"{tenant}\"")))
        .execute(&mut *t)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(
        n, 0,
        "an unset app.project_id must see no rows (fail-closed)"
    );
    t.rollback().await.unwrap();

    let mut t = app.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("SET LOCAL ROLE \"{across}\"")))
        .execute(&mut *t)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&mut *t).await.unwrap();
    assert_eq!(n, 2, "the across-projects role sees every project's rows");
    t.rollback().await.unwrap();

    // The scope switch is transaction-local: on an autocommit connection it
    // does not outlive its own statement, and the next read runs fail-closed.
    sqlx::query(sqlx::AssertSqlSafe(format!("SET LOCAL ROLE \"{across}\"")))
        .execute(&app)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT).fetch_one(&app).await.unwrap();
    assert_eq!(
        n, 0,
        "an autocommit-set role switch must not outlive its statement (fail-closed)"
    );

    // Drop the per-test login role. Its privileges depend on the schema's tables,
    // so `DROP OWNED BY` clears those first; the schema + per-test app role are
    // dropped by `TestDb::drop`.
    app.close().await;
    for stmt in [
        format!("DROP OWNED BY \"{app_user}\""),
        format!("DROP ROLE \"{app_user}\""),
        format!("DROP OWNED BY \"{across}\""),
        format!("DROP ROLE \"{across}\""),
        format!("DROP OWNED BY \"{tenant}\""),
        format!("DROP ROLE \"{tenant}\""),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
            .execute(&su)
            .await
            .unwrap();
    }
}

/// `check()` refuses to start if the **serving** role bypasses RLS. A
/// misconfigured tenant role with `BYPASSRLS` would silently collapse project
/// isolation; `check_rls_posture` on the serving pool must catch it.
#[tokio::test]
async fn test_check_rejects_bypassing_serving_role() {
    use crate::config::DatabaseConfig;

    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    ApiserverComponent
        .check(&db.serving)
        .await
        .expect("NOBYPASSRLS serving role should pass");

    // Create a **own** BYPASSRLS role (not the shared `td_app`) and build a serving
    // pool that `SET ROLE`s it, so the misconfiguration stays isolated to this test.
    // A unique name — roles are cluster-global on the shared server.
    let su = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(&domain::admin_base()))
        .await
        .unwrap();
    let bypassing = format!("b_{}", td_common::id::id());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE \"{bypassing}\" BYPASSRLS"
    )))
    .execute(&su)
    .await
    .unwrap();

    // Pin the bypassing role so the pool's connections `SET ROLE` it (the
    // superuser may switch to any role).
    let cfg = DatabaseConfig {
        conn: (domain::admin_base()),
        schema: Some(db.schema().clone()),
        roles: Some(RlsRoles {
            in_project: bypassing.clone(),
            across_projects: bypassing.clone(),
        }),
        pool: crate::test_utils::test_pool_config(),
    };
    let other = TdPool::connect(&cfg).await.unwrap();
    let res = other.check_rls_posture().await;
    assert!(
        matches!(res, Err(DbError::ConnectedRoleBypassesRls)),
        "a BYPASSRLS serving role must be rejected, got {res:?}"
    );

    // Drop the per-test role (close the probe pool first so it isn't in use).
    other.shutdown().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP ROLE IF EXISTS \"{bypassing}\""
    )))
    .execute(&su)
    .await
    .unwrap();
}

/// The SCD2 close-out trigger's predecessor UPDATE succeeds under RLS (the
/// scoped role sees the row it must close), and FK validation across RLS
/// tables works; project B then sees no rows.
#[tokio::test]
async fn test_rls_scd2_close_out_trigger_under_role() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();

    let mut c = db.begin().await.unwrap();
    {
        c.scope_in_project("A").await.unwrap();
        // Collections FK groups(id); project_id defaults to the scope GUC
        // ('A'), satisfying RLS WITH CHECK and the collection's FK.
        sqlx::query(
            "INSERT INTO groups (id, name, description, created_on, created_by_id, modified_on, modified_by_id, system, kind) \
             VALUES ('g', 'g', 'd', now(), 'u', now(), 'u', false, 'custom')",
        )
        .execute(&mut *c)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO collections (id, name, description, created_on, created_by_id, modified_on, modified_by_id, group_id, base_path, collection_type) \
             VALUES ('col', 'c', 'd', now(), 'u', now(), 'u', 'g', '/', 'F')",
        )
        .execute(&mut *c)
        .await
        .unwrap();
        for (id, on) in [
            (202601010000000001i64, "2026-01-01T00:00:00Z"),
            (202601010000000002i64, "2026-01-02T00:00:00Z"),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO functions \
                 (id, collection_id, name, description, decorator, function_type, runtime_values, function_id, \
                  stage_id, stage_base_path, storage_version, bundle_id, snippet, valid_from, defined_by_id, status) \
                 VALUES ({id}, 'col', 'f', 'd', 'dec', 'R', '{{}}', 'fid', 'stg', '/', '1', 'bun', 'snip', '{on}', 'u', 'active')",
            )))
            .execute(&mut *c)
            .await
            .unwrap();
        }
        let open: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM functions WHERE valid_to = 'infinity'")
                .fetch_all(&mut *c)
                .await
                .unwrap();
        assert_eq!(
            open,
            [202601010000000002i64],
            "the close-out trigger's UPDATE closed the predecessor under RLS"
        );
        let pid: String =
            sqlx::query_scalar("SELECT project_id FROM functions WHERE id = 202601010000000002")
                .fetch_one(&mut *c)
                .await
                .unwrap();
        assert_eq!(pid, "A", "the scope GUC stamped the source project_id");
    }
    c.commit().await.unwrap();

    let mut c = db.begin().await.unwrap();
    {
        c.scope_in_project("B").await.unwrap();
        let f: i64 = sqlx::query_scalar("SELECT count(*) FROM functions")
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(f, 0, "project B sees no function rows");
    }
    c.rollback().await.unwrap();
}

/// The scope primitives switch the transaction's role, `scope_in_project`
/// switches back from a prior cross-project scope, and `scope_global` is the
/// tenant role with no project.
#[tokio::test]
async fn test_set_rls_scope_primitive() {
    let db = crate::test_utils::postgres::pool().await;
    db.upgrade().await.unwrap();
    let roles = crate::test_utils::postgres::test_rls_roles();

    {
        let mut conn = db.begin().await.unwrap();
        conn.scope_across_projects().await.unwrap();
        conn.scope_in_project("PROJ_X").await.unwrap();
        let (role, project): (String, String) =
            sqlx::query_as("SELECT current_user::text, current_setting('app.project_id', true)")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(project, "PROJ_X");
        assert_eq!(
            role, roles.in_project,
            "scope_in_project must switch back from the bypass role"
        );
    }

    {
        let mut conn = db.begin().await.unwrap();
        conn.scope_across_projects().await.unwrap();
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(role, roles.across_projects);
    }

    {
        let mut conn = db.begin().await.unwrap();
        conn.scope_across_projects().await.unwrap();
        conn.scope_global().await.unwrap();
        let (role, project): (String, Option<String>) =
            sqlx::query_as("SELECT current_user::text, current_setting('app.project_id', true)")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert_eq!(
            role, roles.in_project,
            "scope_global runs as the tenant role"
        );
        assert!(
            project.as_deref().unwrap_or("").is_empty(),
            "scope_global leaves no project set"
        );
    }
}
