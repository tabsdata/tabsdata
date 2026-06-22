//
// Copyright 2026 Tabs Data Inc.
//

//! Integration tests for applying and checking the apiserver schema through the
//! real [`ApiserverComponent`](crate::system::ApiserverComponent) +
//! [`TestDb`](crate::test_utils::TestDb), grouped by concern:
//!
//! * [`seeded`] — the bootstrap-seeded data (users, roles, permissions).
//! * [`structure`] — the live schema's table/view/index inventory + version
//!   and edition gates.
//! * [`rls`] — td-database's row-level-security machinery once the schema is
//!   applied (postgres only).
//!
//! This module holds the shared query helpers; the submodules reach them via
//! `super::`.

mod seeded;
#[cfg(sqlite)]
mod shared_schema;
mod structure;
#[cfg(postgres)]
mod rls;

use crate::dynamic::DynamicConnection;
use crate::pool::TdPool;
use crate::raw_query;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use crate::test_utils::TestDb;

/// A single `id` column, for the seeded-data lookups.
#[derive(sqlx::FromRow)]
struct Value {
    id: String,
}

/// Run a backend catalog query returning a single text column. Branches on the
/// live connection variant to reach the underlying sqlx connection.
async fn scalars(db: &TdPool, sql: &str) -> Vec<String> {
    let mut conn = db.acquire().await.unwrap();
    match &mut conn {
        #[cfg(sqlite)]
        DynamicConnection::Sqlite(h) => sqlx::query_scalar(sql).fetch_all(&mut **h).await.unwrap(),
        #[cfg(postgres)]
        DynamicConnection::Postgres(h) => sqlx::query_scalar(sql).fetch_all(&mut **h).await.unwrap(),
        #[cfg(mysql)]
        DynamicConnection::MySql(h) => sqlx::query_scalar(sql).fetch_all(&mut **h).await.unwrap(),
    }
}

/// Fetch a single text column for a runtime-built query (one whose values are
/// locals, which `raw_query!` cannot interpolate).
async fn fetch_id(db: &TdPool, sql: &str) -> String {
    let mut conn = db.acquire().await.unwrap();
    match &mut conn {
        #[cfg(sqlite)]
        DynamicConnection::Sqlite(h) => sqlx::query_scalar(sql).fetch_one(&mut **h).await.unwrap(),
        #[cfg(postgres)]
        DynamicConnection::Postgres(h) => sqlx::query_scalar(sql).fetch_one(&mut **h).await.unwrap(),
        #[cfg(mysql)]
        DynamicConnection::MySql(h) => sqlx::query_scalar(sql).fetch_one(&mut **h).await.unwrap(),
    }
}

/// Lists user-defined tables across backends. Each backend has its own catalog
/// (`sqlite_master` on sqlite, `information_schema` on postgres); branching on
/// the backend kind keeps the assertion list identical regardless of backend.
async fn schema_table_names(db: &TdPool) -> Vec<String> {
    let sql = match db.kind() {
        #[cfg(sqlite)]
        DatabaseBackend::Sqlite => "SELECT name FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name"
            .to_string(),
        #[cfg(postgres)]
        DatabaseBackend::Postgres => "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' \
             ORDER BY table_name"
            .to_string(),
        #[cfg(mysql)]
        DatabaseBackend::MySql => "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' \
             ORDER BY table_name"
            .to_string(),
    };
    let mut names = scalars(db, &sql).await;
    names.sort();
    names
}

async fn schema_view_names(db: &TdPool) -> Vec<String> {
    let sql = match db.kind() {
        #[cfg(sqlite)]
        DatabaseBackend::Sqlite => {
            "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name".to_string()
        }
        #[cfg(postgres)]
        DatabaseBackend::Postgres => "SELECT table_name FROM information_schema.views \
             WHERE table_schema = current_schema() ORDER BY table_name"
            .to_string(),
        #[cfg(mysql)]
        DatabaseBackend::MySql => "SELECT table_name FROM information_schema.views \
             WHERE table_schema = DATABASE() ORDER BY table_name"
            .to_string(),
    };
    let mut names = scalars(db, &sql).await;
    names.sort();
    names
}

/// User-defined indexes only. Filters out backend-managed artifacts so the
/// assertion list is identical across sqlite and postgres.
async fn schema_index_names(db: &TdPool) -> Vec<String> {
    let sql = match db.kind() {
        #[cfg(sqlite)]
        DatabaseBackend::Sqlite => "SELECT name FROM sqlite_master \
             WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%' \
             ORDER BY name"
            .to_string(),
        #[cfg(postgres)]
        DatabaseBackend::Postgres => "SELECT indexname FROM pg_indexes \
             WHERE schemaname = current_schema() \
               AND indexname NOT LIKE '%_pkey' \
               AND indexname NOT LIKE '%_key' \
             ORDER BY indexname"
            .to_string(),
        #[cfg(mysql)]
        DatabaseBackend::MySql => "SELECT DISTINCT index_name FROM information_schema.statistics \
             WHERE table_schema = DATABASE() AND index_name <> 'PRIMARY' \
             ORDER BY index_name"
            .to_string(),
    };
    let mut names = scalars(db, &sql).await;
    names.sort();
    names
}

async fn schema_db_version(db: &TestDb) -> String {
    raw_query!(
        db,
        "SELECT value FROM tabsdata_system WHERE name = 'db_version'"
    )
    .build_query_scalar()
    .fetch_one(&db.serving)
    .await
    .unwrap()
}

/// The schema a component is mounted on decouples from both its registry key and
/// its default name: with no override it is the component's default, and an
/// override mounts the same content on any schema.
#[test]
fn component_schema_override_resolves() {
    use crate::config::{ComponentOverride, Schema};
    use crate::system::ApiserverComponent;

    assert_eq!(
        super::resolved_schema(None, &ApiserverComponent),
        "td_apiserver"
    );

    let ovr = ComponentOverride {
        schema: Some(Schema::new("tenant_a_apiserver")),
        password: None,
        pool: Default::default(),
    };
    assert_eq!(
        super::resolved_schema(Some(&ovr), &ApiserverComponent),
        "tenant_a_apiserver"
    );
}
