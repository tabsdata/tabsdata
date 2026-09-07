//
// Copyright 2026 Tabsdata Inc.
//

//! The create precondition: provisioning refuses a database whose objects are
//! already there. The provisioning DDL is idempotent on purpose, so this is the
//! only thing standing between a stray `db create` and a live database.

use crate::components::{APISERVER_COMPONENT_KEY, AdminPool, provision_all};
use crate::config::{
    ComponentConfig, DatabaseComponents, DatabaseConfig, PoolConfig, PoolsConfig, PostgresConfig,
    Schema,
};
use crate::error::DbError;
use crate::test_utils::server;
use std::time::Duration;

/// The privileged pool `db create` provisions with.
fn admin_config(app_role: &str) -> DatabaseConfig {
    DatabaseConfig {
        conn: server::admin_base(),
        schema: None,
        app_role: Some(app_role.to_owned()),
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
fn components(schema: &str, app_role: &str) -> DatabaseComponents {
    DatabaseComponents::from([(
        APISERVER_COMPONENT_KEY.to_owned(),
        ComponentConfig {
            postgres: PostgresConfig {
                password: Some("owner_pw".to_owned()),
                ..server::admin_base().options
            },
            schema: Some(Schema::new(schema)),
            app_role: Some(app_role.to_owned()),
            pool: PoolsConfig::default(),
        },
    )])
}

#[tokio::test]
async fn test_provision_all_refuses_an_already_provisioned_component() {
    server::ensure_started().await;
    let uid = td_common::id::id();
    let schema = format!("own_{uid}");
    let app_role = format!("app_{uid}");
    let components = components(&schema, &app_role);
    let admin = AdminPool::connect(&admin_config(&app_role)).await.unwrap();

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
    admin.drop_app_role(&app_role).await.unwrap();
    admin.close().await;
}
