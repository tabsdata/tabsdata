//
// Copyright 2026 Tabsdata Inc.
//

//! Integration tests for applying and checking the apiserver schema through the
//! real [`ApiserverComponent`](crate::components::ApiserverComponent) +
//! [`TestDb`](crate::test_utils::TestDb), grouped by concern:
//!
//! * [`seeded`] — the bootstrap-seeded data (users, roles, permissions).
//! * [`structure`] — the live schema's table/view/index inventory + version
//!   and edition gates.
//! * [`rls`] — td-database's row-level-security machinery once the schema is
//!   applied (postgres only).
//! * [`provisioning`] — [`AdminPool::provision_schema`](crate::components::AdminPool)
//!   as a non-superuser `CREATEROLE` admin (postgres only).
//!
//! This module holds the shared query helpers; the submodules reach them via
//! `super::`.

#[cfg(postgres)]
mod provisioning;
#[cfg(postgres)]
mod rls;
mod seeded;
#[cfg(sqlite)]
mod shared_schema;
mod structure;

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
        DynamicConnection::Postgres(h) => {
            sqlx::query_scalar(sql).fetch_all(&mut **h).await.unwrap()
        }
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
        DynamicConnection::Postgres(h) => {
            sqlx::query_scalar(sql).fetch_one(&mut **h).await.unwrap()
        }
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
/// its default name: with no `schema:` on its entry it is the component's
/// default, and an explicit `schema:` mounts the same content on any schema.
#[cfg(postgres)]
#[test]
fn component_schema_override_resolves() {
    use crate::components::{ApiserverComponent, resolved_schema};
    use crate::config::DatabaseComponents;

    let yaml = "
apiserver:
  postgres:
    host: localhost
    port: 7432
";
    let components: DatabaseComponents = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(
        resolved_schema(&components, &ApiserverComponent).unwrap(),
        "td_apiserver"
    );

    let yaml = "
apiserver:
  postgres:
    host: localhost
    port: 7432
  schema: tenant_a_apiserver
";
    let components: DatabaseComponents = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(
        resolved_schema(&components, &ApiserverComponent).unwrap(),
        "tenant_a_apiserver"
    );
}

/// Credential semantics of the serving config: explicit values pass through,
/// `${embedded}` on the username is rejected, an absent password is a
/// resolution error (the resolved config requires credentials), and
/// `${embedded}` on the password reads the persisted owner file (hard error
/// when it cannot resolve).
#[cfg(all(postgres, feature = "embedded-postgres"))]
#[test]
fn serving_config_credential_semantics() {
    use crate::components::{ApiserverComponent, serving_config};
    use crate::config::{ConnectConfig, DatabaseComponents, DatabaseConfig};
    use crate::error::DbError;

    let parse = |extra: &str| -> DatabaseComponents {
        serde_yaml::from_str(&format!(
            "
apiserver:
  postgres:
    host: localhost
    port: 7432
{extra}"
        ))
        .unwrap()
    };
    let postgres_connect = |config: &DatabaseConfig| {
        #[allow(irrefutable_let_patterns)]
        let ConnectConfig::Postgres(pg) = &config.conn else {
            panic!("expected postgres connect");
        };
        (pg.username.clone(), pg.password.clone())
    };

    // Explicit credentials pass through; username defaults to the owner role.
    let config = serving_config(&parse("    password: owner_secret"), &ApiserverComponent).unwrap();
    let (username, password) = postgres_connect(&config);
    assert_eq!(username, "td_apiserver");
    assert_eq!(password, "owner_secret");

    // An absent password fails at resolution — the resolved config requires
    // credentials, so a passwordless connect can never be attempted.
    assert!(matches!(
        serving_config(&parse(""), &ApiserverComponent),
        Err(DbError::MissingCredentials)
    ));

    // `${embedded}` is not a valid username source for a component.
    assert!(matches!(
        serving_config(
            &parse("    username: ${embedded}\n    password: x"),
            &ApiserverComponent
        ),
        Err(DbError::EmbeddedUsernameNotAllowed)
    ));

    // `${embedded}` password reads the persisted owner file...
    let base = std::env::temp_dir().join(format!("td-embedded-cred-{}", std::process::id()));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        base.join(".td_embedded_owner_password.td_apiserver"),
        "from_file",
    )
    .unwrap();
    unsafe { std::env::set_var("TD_DATABASE_DIR", &data_dir) };
    let config = serving_config(&parse("    password: ${embedded}"), &ApiserverComponent).unwrap();
    let (_, password) = postgres_connect(&config);
    assert_eq!(password, "from_file");

    // ...and an unresolvable placeholder is a hard error, like absence.
    unsafe { std::env::remove_var("TD_DATABASE_DIR") };
    unsafe { std::env::remove_var("TD_URI_REPOSITORY") };
    assert!(matches!(
        serving_config(&parse("    password: ${embedded}"), &ApiserverComponent),
        Err(DbError::MissingDatabaseDir)
    ));

    std::fs::remove_dir_all(&base).ok();
}
