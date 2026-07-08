//
// Copyright 2026 Tabs Data Inc.
//

//! SQLite test database factory. Builds a fresh per-test database file under
//! `tdtestdir!()` with relaxed pool settings.

use crate::components::{AdminPool, ProvisionSpec, SchemaComponent};
use crate::config::schema::TEST_SCHEMA;
use crate::config::{DatabaseConfig, Schema};
use crate::pool::TdPool;
use crate::test_utils::{MultiTestDb, TestDb, TestDbCleanup};
use std::time::Duration;
use url::Url;

/// Fresh sqlite [`TestDb`] backed by a per-test temp file. SQLite has no RLS, so
/// the admin and serving connections are two ordinary connections to the same
/// file. Caller is responsible for `.upgrade()` if they need the schema applied.
pub async fn pool() -> TestDb {
    let db_file = td_common::tdtestdir!().join(format!("{}.db", td_common::id::id()));
    let db_url = Url::from_file_path(db_file).unwrap();
    let mut config = DatabaseConfig::new(db_url.clone()).unwrap();
    config.pool.min_connections = 0;
    config.pool.idle_timeout = std::time::Duration::from_millis(500);
    let serving = TdPool::connect(&config).await.expect("sqlite test pool");
    let admin = AdminPool::connect(&config)
        .await
        .expect("sqlite admin connection");
    TestDb {
        serving,
        admin,
        schema: Schema::new(TEST_SCHEMA),
        url: db_url,
        // sqlite: per-test temp file, wiped on the next run — nothing to drop.
        cleanup: TestDbCleanup::Noop,
    }
}

/// Provision each component into one fresh sqlite file (SQLite has no schemas, so
/// all components share the database) and return a serving pool per component in
/// request order. `provision` is a no-op on sqlite; each component's `upgrade`
/// creates its tables.
pub async fn pools(components: &[&'static dyn SchemaComponent]) -> MultiTestDb {
    let db_file = td_common::tdtestdir!().join(format!("{}.db", td_common::id::id()));
    let db_url = Url::from_file_path(db_file).unwrap();
    let mut config = DatabaseConfig::new(db_url).unwrap();
    config.pool.min_connections = 0;
    config.pool.idle_timeout = Duration::from_millis(500);

    let admin = AdminPool::connect(&config)
        .await
        .expect("sqlite admin connection");
    for component in components {
        let schema = component.default_schema();
        component
            .provision(
                &admin,
                &ProvisionSpec {
                    schema,
                    owner: schema,
                    password: "",
                },
            )
            .await
            .expect("provision component");
        component.upgrade(&admin).await.expect("upgrade component");
    }

    let mut serving = Vec::with_capacity(components.len());
    for _ in components {
        serving.push(TdPool::connect(&config).await.expect("sqlite serving pool"));
    }
    // Nothing to tear down: the temp file lives under `tdtestdir!()` and is wiped
    // on the next run, and the serving pools own their own connections.
    MultiTestDb {
        serving,
        cleanup: TestDbCleanup::Noop,
    }
}
