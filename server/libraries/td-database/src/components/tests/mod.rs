//
// Copyright 2026 Tabsdata Inc.
//

//! Integration tests for applying and checking the apiserver schema through the
//! real [`ApiserverComponent`](crate::components::ApiserverComponent) +
//! [`TestDb`](crate::test_utils::TestDb), grouped by concern:
//!
//! * [`delta_data`] — deltas that migrate data, over real rows.
//! * [`seeded`] — the bootstrap-seeded data (users, roles, permissions).
//! * [`structure`] — the live schema's table/view/index inventory + version
//!   and edition gates.
//! * [`scd2`] — the SCD2 close-out trigger's runtime behavior (postgres
//!   only).
//! * [`rls`] — td-database's row-level-security machinery once the schema is
//!   applied (postgres only).
//! * [`provisioning`] — [`AdminPool::provision_schema`](crate::components::AdminPool)
//!   as a non-superuser `CREATEROLE` admin (postgres only).
//! * [`create_preconditions`] — `create` refusing an already-provisioned
//!   database (postgres only).
//!
//! This module holds the shared query helpers; the submodules reach them via
//! `super::`.

mod create_preconditions;
mod delta_data;
mod migration;
mod provisioning;
mod rls;
mod scd2;
mod seeded;
mod shared_schema;
mod structure;
use crate::pool::TdPool;
use crate::test_utils::TestDb;

/// Run a catalog query returning a single text column. Straight on the pool:
/// a transaction dropped without being ended keeps its locks until the pool
/// closes, which is after the teardown that waits for them.
async fn scalars(db: &TdPool, sql: impl Into<String>) -> Vec<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql.into()))
        .fetch_all(db.as_ref())
        .await
        .unwrap()
}

/// Fetch one text column, binding `args` as `$1`, `$2`, … .
async fn fetch_text(db: &TdPool, sql: impl Into<String>, args: &[&str]) -> String {
    let mut query = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.into()));
    for arg in args {
        query = query.bind(*arg);
    }
    query.fetch_one(db.as_ref()).await.unwrap()
}

/// `SELECT count(*)` off `table`.
async fn count_rows(db: &TdPool, table: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
        .fetch_one(db.as_ref())
        .await
        .unwrap()
}

/// A logical id as the schema stores it in text form: the padded
/// `<KEY><YYYYMMDD>-<COUNTER>` an archived version wrote.
fn logical(key: &str, counter: usize) -> String {
    format!("{key}20260101-{counter:010}")
}

/// The same id as the current schema holds it: the `<YYYYMMDD><COUNTER>` `i64`.
fn at_rest(counter: usize) -> String {
    format!("20260101{counter:010}")
}

/// Run a statement for its effect, committing it.
async fn execute(db: &TdPool, sql: impl Into<String>) {
    let mut conn = db.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(sql.into()))
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.commit().await.unwrap();
}

/// Lists this schema's user-defined tables.
async fn schema_table_names(db: &TdPool) -> Vec<String> {
    let sql = "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' \
         ORDER BY table_name";
    let mut names = scalars(db, sql).await;
    names.sort();
    names
}

async fn schema_view_names(db: &TdPool) -> Vec<String> {
    let sql = "SELECT table_name FROM information_schema.views \
         WHERE table_schema = current_schema() ORDER BY table_name";
    let mut names = scalars(db, sql).await;
    names.sort();
    names
}

/// User-defined indexes only, filtering out the constraint-backing ones.
async fn schema_index_names(db: &TdPool) -> Vec<String> {
    let sql = "SELECT indexname FROM pg_indexes \
         WHERE schemaname = current_schema() \
           AND indexname NOT LIKE '%_pkey' \
           AND indexname NOT LIKE '%_key' \
         ORDER BY indexname";
    let mut names = scalars(db, sql).await;
    names.sort();
    names
}

async fn schema_db_version(db: &TestDb) -> String {
    fetch_text(
        &db.serving,
        "SELECT value FROM tabsdata_system WHERE name = $1",
        &["db_version"],
    )
    .await
}

/// A declared pool without its `pool:` block is a config error; the full set
/// validates; and a block for a pool no component has no longer parses.
#[test]
fn test_validate_component_pools_requires_declared_blocks() {
    use crate::components::validate_component_pools;
    use crate::config::DatabaseComponents;
    use crate::error::DbError;

    let entry = |pool: &str| -> DatabaseComponents {
        serde_yaml::from_str(&format!(
            "
apiserver:
  postgres:
    host: localhost
{pool}"
        ))
        .unwrap()
    };

    // The apiserver is the component with an exclusive pool, so its entry
    // carries both blocks.
    let full = entry(
        "  pool:
    regular: {min_connections: 1, max_connections: 2}
    exclusive: {min_connections: 1, max_connections: 2}
",
    );
    validate_component_pools(&full).unwrap();

    let missing_exclusive = entry(
        "  pool:
    regular: {min_connections: 1, max_connections: 2}
",
    );
    assert!(matches!(
        validate_component_pools(&missing_exclusive),
        Err(DbError::MissingComponentPoolConfig(_, "exclusive"))
    ));

    let missing_regular = entry(
        "  pool:
    exclusive: {min_connections: 1, max_connections: 2}
",
    );
    assert!(matches!(
        validate_component_pools(&missing_regular),
        Err(DbError::MissingComponentPoolConfig(_, "regular"))
    ));

    // The pre-split spelling fails where it is written, not silently.
    let err = serde_yaml::from_str::<DatabaseComponents>(
        "
apiserver:
  postgres:
    host: localhost
  pool:
    ro: {min_connections: 1, max_connections: 1}
",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"), "{err}");
}

/// The schema a component is mounted on decouples from both its registry key and
/// its default name: with no `schema:` on its entry it is the component's
/// default, and an explicit `schema:` mounts the same content on any schema.
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
#[cfg(feature = "embedded-postgres")]
#[test]
fn serving_config_credential_semantics() {
    use crate::components::{ApiserverComponent, serving_config};
    use crate::config::{DatabaseComponents, DatabaseConfig};
    use crate::error::DbError;

    let parse = |extra: &str| -> DatabaseComponents {
        serde_yaml::from_str(&format!(
            "
apiserver:
  postgres:
    host: localhost
    port: 7432
    database: postgres
{extra}"
        ))
        .unwrap()
    };
    let postgres_connect =
        |config: &DatabaseConfig| (config.conn.username.clone(), config.conn.password.clone());

    let config = serving_config(&parse("    password: owner_secret"), &ApiserverComponent).unwrap();
    let (username, password) = postgres_connect(&config);
    assert_eq!(username, "postgres_apiserver");
    assert_eq!(password, "owner_secret");

    assert!(matches!(
        serving_config(&parse(""), &ApiserverComponent),
        Err(DbError::MissingCredentials)
    ));

    assert!(matches!(
        serving_config(
            &parse("    username: ${embedded}\n    password: x"),
            &ApiserverComponent
        ),
        Err(DbError::EmbeddedUsernameNotAllowed)
    ));

    let base = std::env::temp_dir().join(format!("td-embedded-cred-{}", std::process::id()));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        base.join(".td_embedded_owner_password.postgres_apiserver"),
        "from_file",
    )
    .unwrap();
    unsafe { std::env::set_var("TD_DATABASE_DIR", &data_dir) };
    let config = serving_config(&parse("    password: ${embedded}"), &ApiserverComponent).unwrap();
    let (_, password) = postgres_connect(&config);
    assert_eq!(password, "from_file");

    unsafe { std::env::remove_var("TD_DATABASE_DIR") };
    unsafe { std::env::remove_var("TD_URI_REPOSITORY") };
    assert!(matches!(
        serving_config(&parse("    password: ${embedded}"), &ApiserverComponent),
        Err(DbError::MissingDatabaseDir)
    ));

    std::fs::remove_dir_all(&base).ok();
}
