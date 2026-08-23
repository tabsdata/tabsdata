//
// Copyright 2026 Tabsdata Inc.
//

//! Postgres test database factories.
//!
//! Every test isolates on its **own uniquely-named schema of the shared server**
//! ([`super::server`]). The schema is **superuser-owned** and the only role is the
//! cluster-global `td_app` (created once); the per-test path therefore touches no
//! cluster-global catalog (`pg_database`/`pg_authid`) — it is all schema-local DDL,
//! which PostgreSQL runs concurrently — so tests provision in parallel. Serving
//! pools connect as the superuser and `SET ROLE td_app`. Each test's schema is
//! dropped on teardown ([`TestDbCleanup`]). A test that needs an extra role creates
//! (and drops) its own.
//!
//! - [`pools`] — the `#[td_test::test(db)]` macro path: one schema + serving pool
//!   per requested component, brought to its current version.
//! - [`pool`] — td-database's own `components`/RLS tests: one schema (left un-upgraded;
//!   the caller applies it via [`TestDb::upgrade`]) plus the privileged handle and
//!   superuser connect target those white-box tests need.
//!
//! # Project scope in tests
//!
//! In test builds the `td-test` feature is on, so the serving pool's
//! `after_connect` runs `SET app.bypass_rls = on`: RLS is **disabled by default**
//! in tests. Use `db.scoped(p)` (or a service) to re-enforce RLS for a query.

use crate::components::{AdminPool, SchemaComponent};
use crate::config::{ConnectConfig, DatabaseConfig, Schema};
use crate::pool::TdPool;
#[cfg(test)]
use crate::test_utils::TestDb;
use crate::test_utils::{MultiTestDb, TestDbCleanup};
use crate::test_utils::{server, test_pool_config};

/// A schema name unique across all concurrent tests, processes and runs: a fresh
/// UUIDv7 ([`td_common::id::id`]) per provisioned component. Globally unique ⇒ no
/// collision even with leftovers from a crashed test. Well under PostgreSQL's
/// 63-char identifier cap.
fn unique_schema(index: usize) -> String {
    format!("tdt_{}_{index}", td_common::id::id())
}

/// A superuser connectable to the shared server, pinned to `schema`. Used for both
/// the privileged ([`AdminPool`]) and serving ([`TdPool`]) connects — the schema is
/// superuser-owned, so there is no per-test owner role; serving `SET ROLE`s the
/// shared `td_app`.
fn schema_connectable(schema: &str) -> DatabaseConfig {
    DatabaseConfig {
        conn: ConnectConfig::Postgres(server::admin_base()),
        schema: Some(Schema::new(schema)),
        app_role: None,
        pool: test_pool_config(),
    }
}

/// A superuser connectable with no schema pin — for dropping a test's schemas on
/// teardown.
fn admin_connectable() -> DatabaseConfig {
    DatabaseConfig {
        conn: ConnectConfig::Postgres(server::admin_base()),
        schema: None,
        app_role: None,
        pool: test_pool_config(),
    }
}

/// Create this test's own unique, superuser-owned schema on the shared server and
/// open the privileged [`AdminPool`] for it. Explicitly ensures the cluster-global
/// `td_app` (connects never provision roles). Shared by [`pool`] and [`pools`];
/// both then connect their serving pool(s) to the same `config` (returned to
/// avoid rebuilding it).
async fn provision_schema(index: usize) -> (String, DatabaseConfig, AdminPool) {
    let schema = unique_schema(index);
    let config = schema_connectable(&schema);
    let admin = AdminPool::connect(&config).await.expect("privileged pool");
    admin.create_app_role().await.expect("create app role");
    admin
        .create_schema(&schema)
        .await
        .expect("create test schema");
    (schema, config, admin)
}

/// A [`TestDb`] on its **own unique schema** of the shared server, for td-database's
/// own `components`/RLS tests — which connect their own pools as arbitrary roles via
/// [`server::admin_base`] and create their own roles when they need
/// to mutate role-level state. The schema is superuser-owned and left un-upgraded;
/// the caller applies it via [`TestDb::upgrade`]. Serving `SET ROLE`s the shared
/// `td_app`. [`TestDb::drop`] drops the schema.
#[cfg(test)]
pub(crate) async fn pool() -> TestDb {
    server::ensure_started().await;
    // Keep the privileged pool open in the `TestDb` — the caller drives `upgrade`.
    let (schema, config, admin) = provision_schema(0).await;
    let serving = TdPool::connect(&config).await.expect("postgres test pool");

    TestDb {
        serving,
        admin,
        schema: Schema::new(&schema),
        cleanup: TestDbCleanup::PostgresSchemas {
            admin: Box::new(admin_connectable()),
            schemas: vec![schema],
        },
    }
}

/// Provision a test on its **own unique schema(s)** of the shared server and return
/// a serving pool per component (in request order), each brought to its current
/// version. The schema is created superuser-owned and the build runs as the
/// superuser; serving pools `SET ROLE td_app`. Everything is schema-local, so
/// concurrent tests provision in parallel. [`TestDbCleanup`] drops the schemas.
pub async fn pools(components: &[&'static dyn SchemaComponent]) -> MultiTestDb {
    server::ensure_started().await;

    let mut serving = Vec::with_capacity(components.len());
    let mut schemas = Vec::with_capacity(components.len());
    for (index, component) in components.iter().enumerate() {
        // Build the schema to its current version (tables + grant `td_app`) on the
        // privileged pool, then drop it and connect the serving pool. All
        // schema-local ⇒ runs concurrently with other tests.
        let (schema, config, admin) = provision_schema(index).await;
        component.upgrade(&admin).await.expect("upgrade component");
        admin.close().await;

        serving.push(
            TdPool::connect(&config)
                .await
                .expect("postgres serving pool"),
        );
        schemas.push(schema);
    }

    MultiTestDb {
        serving,
        cleanup: TestDbCleanup::PostgresSchemas {
            admin: Box::new(admin_connectable()),
            schemas,
        },
    }
}

#[cfg(all(test, postgres))]
mod tests {
    use super::*;
    use crate::components::{ApiserverComponent, SchemaSource};
    use crate::dynamic::DynamicConnection;
    use td_schema::schema::backend::Backend;

    struct DummySource;
    impl SchemaSource for DummySource {
        fn current_version(&self) -> usize {
            1
        }
        fn tables(&self, _backend: Backend) -> Vec<String> {
            vec!["CREATE TABLE IF NOT EXISTS dummy_kv (k TEXT PRIMARY KEY)".to_string()]
        }
    }

    struct DummyComponent;
    impl SchemaComponent for DummyComponent {
        fn key(&self) -> &'static str {
            "dummy"
        }
        fn default_schema(&self) -> &'static str {
            "td_dummy"
        }
        fn schema_source(&self) -> &'static dyn SchemaSource {
            &DummySource
        }
    }

    async fn count(pool: &TdPool, sql: &str) -> i64 {
        let mut conn = pool.acquire().await.unwrap();
        #[allow(unreachable_patterns)]
        match &mut conn {
            DynamicConnection::Postgres(h) => {
                sqlx::query_scalar(sql).fetch_one(&mut **h).await.unwrap()
            }
            _ => unreachable!("postgres-only test"),
        }
    }

    #[tokio::test]
    async fn pools_provisions_each_component_on_its_own_schema() {
        let db = pools(&[&ApiserverComponent, &DummyComponent]).await;
        assert_eq!(db.serving.len(), 2);

        // apiserver pool: its schema is fully provisioned (seeded admin present).
        let admins = count(
            &db.serving[0],
            "SELECT count(*) FROM users WHERE name = 'admin'",
        )
        .await;
        assert_eq!(admins, 1, "apiserver schema provisioned + seeded");

        // dummy pool: its placeholder table exists in its own schema and the app
        // role (the serving pool's SET ROLE target) can read it.
        let rows = count(&db.serving[1], "SELECT count(*) FROM dummy_kv").await;
        assert_eq!(rows, 0, "dummy schema provisioned with its table");

        // Drop this test's schemas, as the harness does.
        let MultiTestDb { serving, cleanup } = db;
        drop(serving);
        cleanup.run().await;
    }
}
