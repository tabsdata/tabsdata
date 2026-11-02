//
// Copyright 2026 Tabsdata Inc.
//

//! The optional-extension steps on a real server: `pg_trgm` (in PostgreSQL's
//! contrib, trusted) stands in for an extension a component declares, feeding
//! one trigram index on a table of the test's own schema; no registered
//! component declares it, so no other test builds that index. Extensions are
//! database-wide, so the lifecycle is one sequential test that leaves the
//! database as it found it. PostgreSQL only.

use crate::components::{AdminPool, ExtensionPools, ExtensionState, OptionalExtension};
use crate::config::{DatabaseConfig, PoolConfig, PoolsConfig, PostgresConnect};
use crate::error::DbError;
use crate::generated_sql::generated;
use crate::test_utils::domain::PostgresDomain;
use crate::test_utils::{TestDb, domain};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

const TRGM_TABLE: &str = "trgm_owned";
const TRGM_INDEX: &str = "trgm_owned_idx";

const TRGM: OptionalExtension = OptionalExtension {
    name: "pg_trgm",
    purpose: "test stand-in",
    dependents: || vec![TRGM_INDEX.to_owned()],
    build: |namespace| {
        let namespace = namespace.replace('"', "\"\"");
        vec![format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS {TRGM_INDEX} ON {TRGM_TABLE} \
             USING gin (t \"{namespace}\".gin_trgm_ops)"
        )]
    },
};

fn trgm() -> &'static OptionalExtension {
    &TRGM
}

fn server_config(conn: PostgresConnect) -> DatabaseConfig {
    DatabaseConfig {
        conn,
        schema: None,
        roles: None,
        pool: PoolsConfig {
            admin: Some(PoolConfig {
                min_connections: 1,
                max_connections: 2,
                acquire_timeout: Duration::from_secs(10),
                ..Default::default()
            }),
            ..Default::default()
        },
    }
}

async fn server(internals: &PostgresDomain) -> AdminPool {
    AdminPool::connect(&server_config(domain::admin_base(internals)))
        .await
        .unwrap()
}

async fn su(internals: &PostgresDomain) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(crate::pool::options::pg_connect_options(
            &domain::admin_base(internals),
        ))
        .await
        .unwrap()
}

async fn run(su: &PgPool, sql: impl Into<String>) {
    sqlx::raw_sql(generated(sql.into()))
        .execute(su)
        .await
        .unwrap();
}

async fn installed(su: &PgPool, name: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT FROM pg_extension WHERE extname = $1)")
        .bind(name)
        .fetch_one(su)
        .await
        .unwrap()
}

/// A role without `CREATE` on the database may not create even a trusted
/// extension: it is told the SQL a DBA runs, and nothing is created.
async fn refused_to_a_plain_role(internals: &PostgresDomain, su: &PgPool, db: &TestDb) {
    let role = format!("ext_{}", td_common::id::id());
    run(
        su,
        format!("CREATE ROLE \"{role}\" LOGIN PASSWORD 'ext_pw'"),
    )
    .await;
    let plain = AdminPool::connect(&server_config(PostgresConnect {
        options: domain::admin_base(internals).options,
        username: role.clone(),
        password: "ext_pw".to_owned(),
    }))
    .await
    .unwrap();
    let pools = ExtensionPools {
        server: &plain,
        owner: &db.admin,
    };
    let report = pools.inspect(trgm()).await.unwrap();
    assert_eq!(report.state, ExtensionState::Refused);
    assert_eq!(
        report.dba_sql,
        Some(format!(
            "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA \"{}\"",
            db.schema()
        ))
    );
    let refused = pools.add(trgm()).await.unwrap_err();
    assert!(
        matches!(&refused, DbError::ExtensionRefused(name, sql, _) if name == "pg_trgm" && sql.contains("CREATE EXTENSION")),
        "{refused}"
    );
    assert!(!installed(su, trgm().name).await);
    plain.close().await;
    run(su, format!("DROP ROLE \"{role}\"")).await;
}

async fn upgraded(internals: &PostgresDomain) -> TestDb {
    let db = crate::test_utils::postgres::pool(internals).await;
    db.upgrade().await.unwrap();
    run(
        &db.admin.pool,
        format!("CREATE TABLE {TRGM_TABLE} (t text)"),
    )
    .await;
    db
}

#[td_test::domain("apiserver_postgres_internals")]
#[tokio::test]
async fn an_offered_extension_is_added_marked_and_removed_only_when_ours_postgres(
    domain_apiserver_postgres_internals: PostgresDomain,
) {
    let internals = &domain_apiserver_postgres_internals;
    let su = su(internals).await;
    if installed(&su, trgm().name).await {
        // Somebody else's: never ours to drop.
        let db = upgraded(internals).await;
        let server = server(internals).await;
        let pools = ExtensionPools {
            server: &server,
            owner: &db.admin,
        };
        let report = pools.inspect(trgm()).await.unwrap();
        assert_eq!(report.state, ExtensionState::Enabled);
        assert!(!report.created_by_tabsdata);
        let outcome = pools.remove(trgm()).await.unwrap();
        assert!(!outcome.dropped_extension);
        assert!(installed(&su, trgm().name).await);
        return;
    }
    let db = upgraded(internals).await;
    let server = server(internals).await;
    let pools = ExtensionPools {
        server: &server,
        owner: &db.admin,
    };

    let before = pools.inspect(trgm()).await.unwrap();
    assert_eq!(before.state, ExtensionState::Available, "{before:?}");
    assert!(before.installed_version.is_none() && before.offered_version.is_some());

    refused_to_a_plain_role(internals, &su, &db).await;

    let added = pools.add(trgm()).await.unwrap();
    assert_eq!(added.state, ExtensionState::Enabled);
    assert!(added.created_by_tabsdata);
    assert_eq!(added.dependents.len(), 1);
    assert_eq!(added.dependents[0].name, TRGM_INDEX);
    assert!(added.dependents[0].present, "its index is built");
    assert_eq!(added.installed_version, added.offered_version);
    assert!(!added.update_available);
    assert_eq!(pools.add(trgm()).await.unwrap(), added, "add is idempotent");

    run(
        &db.admin.pool,
        format!("DROP INDEX {TRGM_INDEX}; INSERT INTO {TRGM_TABLE} VALUES ('a'), ('a')"),
    )
    .await;
    let failed = sqlx::raw_sql(generated(format!(
        "CREATE UNIQUE INDEX CONCURRENTLY {TRGM_INDEX} ON {TRGM_TABLE} (t)"
    )))
    .execute(&db.admin.pool)
    .await;
    assert!(failed.is_err(), "the duplicate fails the concurrent build");
    let broken = pools.inspect(trgm()).await.unwrap();
    assert!(
        !broken.dependents[0].present,
        "the invalid index it left is not present"
    );
    assert_eq!(
        pools.add(trgm()).await.unwrap(),
        added,
        "add drops the invalid index and builds it again"
    );

    let schema = db.schema().to_string();
    run(
        &su,
        format!(
            "CREATE TABLE \"{schema}\".trgm_user (t text); \
             CREATE INDEX trgm_user_idx ON \"{schema}\".trgm_user USING gin (t \"{schema}\".gin_trgm_ops)"
        ),
    )
    .await;
    let refused = pools.remove(trgm()).await.unwrap_err();
    assert!(
        matches!(&refused, DbError::ExtensionInUse(name, detail) if name == "pg_trgm" && detail.contains("trgm_user_idx")),
        "{refused}"
    );
    assert!(installed(&su, trgm().name).await, "never CASCADE");
    run(&su, format!("DROP TABLE \"{schema}\".trgm_user")).await;

    let removed = pools.remove(trgm()).await.unwrap();
    assert!(removed.dropped_extension);
    assert!(!installed(&su, trgm().name).await);
    let after = pools.inspect(trgm()).await.unwrap();
    assert_eq!(after.state, ExtensionState::Available);
    assert!(!after.created_by_tabsdata, "the marker goes with it");
}
