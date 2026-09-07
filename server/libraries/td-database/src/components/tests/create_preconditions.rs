//
// Copyright 2026 Tabsdata Inc.
//

//! The create precondition: provisioning refuses a database whose objects are
//! already there. The provisioning DDL is idempotent on purpose, so this is the
//! only thing standing between a stray `db create` and a live database.

use crate::components::{APISERVER_COMPONENT_KEY, AdminPool, provision_all};
use crate::config::{
    ComponentConfig, DatabaseComponents, DatabaseConfig, PoolConfig, PoolsConfig, PostgresConfig,
    RlsRoles, Schema,
};
use crate::error::DbError;
use crate::test_utils::server;
use std::time::Duration;

fn roles(uid: &impl std::fmt::Display) -> RlsRoles {
    RlsRoles {
        in_project: format!("ip_{uid}"),
        across_projects: format!("ap_{uid}"),
    }
}

/// The privileged pool `db create` provisions with.
fn admin_config(roles: &RlsRoles) -> DatabaseConfig {
    DatabaseConfig {
        conn: server::admin_base(),
        schema: None,
        roles: Some(roles.clone()),
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

/// A `database:` map holding the one component this crate's registry carries.
fn components(schema: &str, roles: &RlsRoles) -> DatabaseComponents {
    DatabaseComponents::from([(
        APISERVER_COMPONENT_KEY.to_owned(),
        ComponentConfig {
            postgres: PostgresConfig {
                password: Some("owner_pw".to_owned()),
                ..server::admin_base().options
            },
            schema: Some(Schema::new(schema)),
            in_project_role: Some(roles.in_project.clone()),
            across_projects_role: Some(roles.across_projects.clone()),
            pool: PoolsConfig::default(),
        },
    )])
}

#[tokio::test]
async fn test_provision_all_refuses_an_already_provisioned_component() {
    server::ensure_started().await;
    let uid = td_common::id::id();
    let schema = format!("own_{uid}");
    let roles = roles(&uid);
    let components = components(&schema, &roles);
    let admin = AdminPool::connect(&admin_config(&roles)).await.unwrap();

    provision_all(&components, &admin).await.unwrap();
    assert!(admin.schema_exists(&schema).await.unwrap());
    assert!(admin.role_exists(&schema).await.unwrap());

    let error = provision_all(&components, &admin).await.unwrap_err();
    let DbError::CreatePreconditionFailed(present) = &error else {
        panic!("expected CreatePreconditionFailed, got {error:?}");
    };
    assert!(
        present.contains(APISERVER_COMPONENT_KEY) && present.contains(&schema),
        "the error must name the colliding component and its schema: {present}"
    );

    admin.drop_schema(&schema, &schema).await.unwrap();
    admin.drop_rls_role(&roles.in_project).await.unwrap();
    admin.drop_rls_role(&roles.across_projects).await.unwrap();
    admin.close().await;
}
