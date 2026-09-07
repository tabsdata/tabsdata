//
// Copyright 2026 Tabsdata Inc.
//

//! [`AdminPool::provision_schema`] as a **non-superuser** `CREATEROLE` admin —
//! the external-database identity (e.g. an RDS domain user). The failures this
//! guards against (PostgreSQL 16 splitting `SET` out of role `ADMIN OPTION`,
//! membership gating convergence) are invisible to a superuser. PostgreSQL
//! only.

use crate::components::{AdminPool, ApiserverComponent, ProvisionSpec, UpgradeAction};
use crate::config::{DatabaseConfig, PoolConfig, PoolsConfig, PostgresConnect, RlsRoles, Schema};
use crate::error::DbError;
use crate::pool::TdPool;
use crate::pool::options::pg_connect_options;
use crate::test_utils::server;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

async fn su_pool() -> PgPool {
    server::ensure_started().await;
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pg_connect_options(&server::admin_base()))
        .await
        .unwrap()
}

fn login(username: &str, password: &str) -> PostgresConnect {
    PostgresConnect {
        options: server::admin_base().options,
        username: username.to_owned(),
        password: password.to_owned(),
    }
}

fn config(conn: PostgresConnect, schema: &str, roles: &RlsRoles) -> DatabaseConfig {
    DatabaseConfig {
        conn,
        schema: Some(Schema::new(schema)),
        roles: Some(roles.clone()),
        pool: {
            let pool = PoolConfig {
                min_connections: 1,
                max_connections: 2,
                acquire_timeout: Duration::from_secs(10),
                ..Default::default()
            };
            PoolsConfig {
                regular: Some(pool.clone()),
                exclusive: Some(pool.clone()),
                admin: Some(pool),
            }
        },
    }
}

async fn create_admin(su: &PgPool, admin_role: &str) {
    let database = scalar(su, "SELECT current_database()").await;
    for stmt in [
        format!("CREATE ROLE \"{admin_role}\" LOGIN CREATEROLE PASSWORD 'adm_pw'"),
        format!("GRANT CREATE ON DATABASE \"{database}\" TO \"{admin_role}\""),
    ] {
        execute_retrying(su, &stmt).await;
    }
}

async fn scalar(su: &PgPool, sql: &str) -> String {
    let (value,): (String,) = sqlx::query_as(sql).fetch_one(su).await.unwrap();
    value
}

/// Concurrent `GRANT ... ON DATABASE` from parallel tests race on the one
/// `pg_database` ACL tuple (`XX000` "tuple concurrently updated"); retry
/// instead of serializing the suite.
async fn execute_retrying(su: &PgPool, sql: &str) {
    for _ in 0..10 {
        match sqlx::query(sql).execute(su).await {
            Ok(_) => return,
            Err(sqlx::Error::Database(e)) if e.message().contains("tuple concurrently") => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("{sql}: {e}"),
        }
    }
    panic!("{sql}: still racing after retries");
}

async fn owner_password_hash(su: &PgPool, owner: &str) -> String {
    scalar(
        su,
        &format!("SELECT coalesce(rolpassword, '') FROM pg_authid WHERE rolname = '{owner}'"),
    )
    .await
}

/// The serving path the apiserver takes: log in as the owner and `SET ROLE`
/// to the app role.
async fn owner_login(owner: &str, password: &str, roles: &RlsRoles) -> Result<(), DbError> {
    let mut config = config(login(owner, password), owner, roles);
    config.pool.regular.as_mut().unwrap().acquire_timeout = Duration::from_secs(3);
    TdPool::connect(&config).await?.close().await;
    Ok(())
}

/// Provision, converge, and serve through a plain `CREATEROLE` admin.
#[tokio::test]
async fn test_provision_as_non_superuser_admin() {
    let su = su_pool().await;
    let uid = td_common::id::id();
    let admin_role = format!("adm_{uid}");
    let owner = format!("own_{uid}");
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    create_admin(&su, &admin_role).await;

    let admin = AdminPool::connect(&config(login(&admin_role, "adm_pw"), &owner, &roles))
        .await
        .unwrap();
    let spec = ProvisionSpec {
        schema: &owner,
        owner: &owner,
        password: "owner_pw1",
        roles: &roles,
    };
    admin.provision_schema(&spec).await.unwrap();

    let schema_owner = scalar(
        &su,
        &format!("SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = '{owner}'"),
    )
    .await;
    assert_eq!(schema_owner, owner);
    let bypass = scalar(
        &su,
        &format!(
            "SELECT rolbypassrls::text FROM pg_roles WHERE rolname = '{}'",
            roles.in_project
        ),
    )
    .await;
    assert_eq!(bypass, "false");
    let member = scalar(
        &su,
        &format!(
            "SELECT pg_has_role('{owner}', '{}', 'MEMBER')::text",
            roles.in_project
        ),
    )
    .await;
    assert_eq!(member, "true");

    // Auth-level idempotence: `ALTER ROLE ... PASSWORD` re-hashes with a
    // fresh salt even for an identical password, so hashes can't express
    // "unchanged".
    admin.provision_schema(&spec).await.unwrap();
    owner_login(&owner, "owner_pw1", &roles).await.unwrap();

    // Quote and backslash exercise the `format(%L)` quoting of the staged
    // password GUC.
    let hostile_pw = r#"own'er\pw2"#;
    let drifted = ProvisionSpec {
        password: hostile_pw,
        ..spec
    };
    admin.provision_schema(&drifted).await.unwrap();
    owner_login(&owner, hostile_pw, &roles).await.unwrap();
    assert!(
        owner_login(&owner, "owner_pw1", &roles).await.is_err(),
        "stale password must stop authenticating after convergence"
    );

    admin.drop_schema(&owner, &owner).await.unwrap();
    for stmt in [
        format!("DROP ROLE IF EXISTS \"{}\"", roles.in_project),
        format!("DROP ROLE IF EXISTS \"{}\"", roles.across_projects),
        format!("DROP OWNED BY \"{admin_role}\""),
        format!("DROP ROLE \"{admin_role}\""),
    ] {
        execute_retrying(&su, &stmt).await;
    }
    su.close().await;
}

/// A deployment provisioned before the `across_projects` role existed: owner
/// and schema only, no RLS roles, no marker membership. `db upgrade` runs the
/// role phase on the privileged entry, after which the owner — who holds no
/// `CREATEROLE` — installs a schema whose policies name those roles. Without
/// the phase the owner's preflight refuses and names every missing object.
#[tokio::test]
async fn test_upgrade_provisions_roles_on_the_privileged_entry() {
    let su = su_pool().await;
    let uid = td_common::id::id();
    let owner = format!("own_{uid}");
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    for stmt in [
        format!("CREATE ROLE \"{owner}\" LOGIN PASSWORD 'own_pw'"),
        format!("CREATE SCHEMA \"{owner}\" AUTHORIZATION \"{owner}\""),
    ] {
        sqlx::query(&stmt).execute(&su).await.unwrap();
    }
    let as_owner = AdminPool::connect(&config(login(&owner, "own_pw"), &owner, &roles))
        .await
        .unwrap();

    let refused = as_owner.upgrade(&ApiserverComponent).await.unwrap_err();
    let DbError::MissingAcrossProjectsMembership(sql) = &refused else {
        panic!("expected MissingAcrossProjectsMembership, got {refused:?}");
    };
    for role in [&roles.in_project, &roles.across_projects] {
        assert!(
            sql.contains(&format!("CREATE ROLE \"{role}\"")),
            "the refusal must name the missing role {role}: {sql}"
        );
    }

    let privileged = AdminPool::connect(&DatabaseConfig {
        conn: server::admin_base(),
        schema: None,
        roles: None,
        pool: crate::test_utils::test_pool_config(),
    })
    .await
    .unwrap();
    privileged
        .ensure_rls_roles_for(&owner, &roles)
        .await
        .unwrap();
    privileged.close().await;

    let action = as_owner.upgrade(&ApiserverComponent).await.unwrap();
    assert!(
        matches!(action, Some(UpgradeAction::Install { .. })),
        "the owner installs once the roles exist, got {action:?}"
    );
    for role in [&roles.in_project, &roles.across_projects] {
        let member = scalar(
            &su,
            &format!("SELECT pg_has_role('{owner}', '{role}', 'MEMBER')::text"),
        )
        .await;
        assert_eq!(member, "true", "the owner must hold {role}");
    }

    as_owner.close().await;
    for stmt in [
        format!("DROP SCHEMA \"{owner}\" CASCADE"),
        format!("DROP OWNED BY \"{}\"", roles.in_project),
        format!("DROP OWNED BY \"{}\"", roles.across_projects),
        format!("DROP OWNED BY \"{owner}\""),
        format!("DROP ROLE \"{}\"", roles.in_project),
        format!("DROP ROLE \"{}\"", roles.across_projects),
        format!("DROP ROLE \"{owner}\""),
    ] {
        execute_retrying(&su, &stmt).await;
    }
    su.close().await;
}

/// A role that exists but is not the admin's is never clobbered, however
/// often the misconfigured provision is re-run: the membership marker only
/// appears in the transaction that creates a role, so run 1 cannot mint the
/// credential that would let run 2 converge (pre-16 `CREATEROLE` may
/// grant/alter any non-superuser role). The batch fails loudly at
/// `CREATE SCHEMA ... AUTHORIZATION` instead.
#[tokio::test]
async fn test_provision_leaves_foreign_owner_role_untouched() {
    let su = su_pool().await;
    let uid = td_common::id::id();
    let admin_role = format!("adm_{uid}");
    let foreign = format!("own_{uid}");
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    create_admin(&su, &admin_role).await;
    execute_retrying(
        &su,
        &format!("CREATE ROLE \"{foreign}\" LOGIN PASSWORD 'theirs'"),
    )
    .await;
    let hash = owner_password_hash(&su, &foreign).await;

    let admin = AdminPool::connect(&config(login(&admin_role, "adm_pw"), &foreign, &roles))
        .await
        .unwrap();
    let spec = ProvisionSpec {
        schema: &foreign,
        owner: &foreign,
        password: "mine",
        roles: &roles,
    };
    for _ in 0..2 {
        admin.provision_schema(&spec).await.unwrap_err();
        assert_eq!(owner_password_hash(&su, &foreign).await, hash);
        let member = scalar(
            &su,
            &format!("SELECT pg_has_role('{admin_role}', '{foreign}', 'MEMBER')::text"),
        )
        .await;
        assert_eq!(
            member, "false",
            "no self-grant may leak onto a foreign role"
        );
    }

    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{foreign}\" CASCADE"))
        .execute(&su)
        .await
        .unwrap();
    for stmt in [
        format!("DROP ROLE IF EXISTS \"{}\"", roles.in_project),
        format!("DROP ROLE IF EXISTS \"{}\"", roles.across_projects),
        format!("DROP ROLE \"{foreign}\""),
        format!("DROP OWNED BY \"{admin_role}\""),
        format!("DROP ROLE \"{admin_role}\""),
    ] {
        execute_retrying(&su, &stmt).await;
    }
    su.close().await;
}

/// A superuser admin colliding with an existing foreign role leaves it
/// byte-untouched (`pg_has_role` is always true for superusers, so the gate
/// deliberately does not consult it).
#[tokio::test]
async fn test_superuser_provision_leaves_colliding_role_untouched() {
    let su = su_pool().await;
    let uid = td_common::id::id();
    let foreign = format!("own_{uid}");
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    execute_retrying(
        &su,
        &format!("CREATE ROLE \"{foreign}\" NOLOGIN PASSWORD 'theirs'"),
    )
    .await;
    let hash = owner_password_hash(&su, &foreign).await;

    let admin = AdminPool::connect(&config(server::admin_base(), &foreign, &roles))
        .await
        .unwrap();
    let spec = ProvisionSpec {
        schema: &foreign,
        owner: &foreign,
        password: "mine",
        roles: &roles,
    };
    admin.provision_schema(&spec).await.unwrap();

    assert_eq!(owner_password_hash(&su, &foreign).await, hash);
    let can_login = scalar(
        &su,
        &format!("SELECT rolcanlogin::text FROM pg_roles WHERE rolname = '{foreign}'"),
    )
    .await;
    assert_eq!(
        can_login, "false",
        "NOLOGIN must survive a colliding provision"
    );

    admin.drop_schema(&foreign, &foreign).await.unwrap();
    execute_retrying(
        &su,
        &format!("DROP ROLE IF EXISTS \"{}\"", roles.in_project),
    )
    .await;
    execute_retrying(
        &su,
        &format!("DROP ROLE IF EXISTS \"{}\"", roles.across_projects),
    )
    .await;
    su.close().await;
}

/// A batch that genuinely cannot proceed keeps its provisioning error type,
/// so `db create` reports the real SQLSTATE.
#[tokio::test]
async fn test_provision_without_create_privilege_fails() {
    let su = su_pool().await;
    let uid = td_common::id::id();
    let admin_role = format!("adm_{uid}");
    execute_retrying(
        &su,
        &format!("CREATE ROLE \"{admin_role}\" LOGIN PASSWORD 'adm_pw'"),
    )
    .await;

    let owner = format!("own_{uid}");
    let roles = RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    };
    let admin = AdminPool::connect(&config(login(&admin_role, "adm_pw"), &owner, &roles))
        .await
        .unwrap();
    let spec = ProvisionSpec {
        schema: &owner,
        owner: &owner,
        password: "owner_pw",
        roles: &roles,
    };
    let error = admin.provision_schema(&spec).await.unwrap_err();
    assert!(
        matches!(error, DbError::FailedToCreateOrUpgradeDatabaseSchema(_)),
        "expected FailedToCreateOrUpgradeDatabaseSchema, got: {error:?}"
    );

    execute_retrying(&su, &format!("DROP ROLE \"{admin_role}\"")).await;
    su.close().await;
}
