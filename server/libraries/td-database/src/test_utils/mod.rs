//
// Copyright 2026 Tabsdata Inc.
//

//! Per-backend test database factories. The canonical "give me a fresh database
//! for one test" — used by td-test's `DbBackendProvider` impls and, directly, by
//! td-database's own tests.
//!
//! A [`TestDb`] pairs a privileged [`AdminPool`] with the serving [`TdPool`] a
//! test queries (and, for the embedded backend, owns the private embedded server
//! so it stops when the test ends). Returned databases are freshly connected but
//! **un-upgraded**; callers that need the schema applied call [`TestDb::upgrade`].

use crate::components::{AdminPool, ApiserverComponent, SchemaComponent};
use crate::config::{DatabaseConfig, PoolConfig, Schema};
use crate::dynamic::DynamicPool;
use crate::error::DbError;
use crate::pool::TdPool;
use std::ops::Deref;
use std::time::Duration;
use tracing::warn;

pub mod postgres;
pub mod server;
#[cfg(sqlite)]
pub mod sqlite;

/// Pool settings for the shared-server test pools. One server backs the whole
/// suite, so each pool stays lean and holds **no resident connections**
/// (`min_connections = 0` + short idle timeout: connections open on demand and
/// release promptly, so the many concurrent test processes don't pile up on the one
/// server). A generous acquire timeout lets a test wait out a brief connection-latency
/// spike under load instead of failing.
pub(crate) fn test_pool_config() -> PoolConfig {
    PoolConfig {
        min_connections: 0,
        // Lean: tests issue mostly-sequential queries, so a tiny cap keeps the total
        // connection demand far below the server limit even at high concurrency
        // (many test processes × admin + serving + teardown pools at once).
        max_connections: 2,
        idle_timeout: Some(Duration::from_secs(1)),
        acquire_timeout: Duration::from_secs(120),
        ..Default::default()
    }
}

/// How a [`MultiTestDb`]/[`TestDb`] is torn down once the test returns. Postgres
/// tests isolate on their own per-test schema(s) of the shared server, so cleanup
/// drops those schemas; sqlite tests own a temp file the next run wipes, so there is
/// nothing to tear down. Run asynchronously after the test body.
pub enum TestDbCleanup {
    /// Drop each per-test schema (`CASCADE`) via this superuser connectable. The
    /// shared `td_app` and the superuser owner are left untouched. Best-effort: a
    /// failure is logged, not fatal (the next `cargo make test` setup starts clean).
    PostgresSchemas {
        // Boxed: `DatabaseConfig` is large and this keeps the common `Noop`
        // variant (and so every `TestDbCleanup`) small.
        admin: Box<DatabaseConfig>,
        schemas: Vec<String>,
    },
    /// Nothing to release (sqlite: the temp file is wiped on the next run).
    Noop,
}

impl TestDbCleanup {
    pub async fn run(self) {
        let TestDbCleanup::PostgresSchemas { admin, schemas } = self else {
            return;
        };
        match AdminPool::connect(&admin).await {
            Ok(pool) => {
                for schema in &schemas {
                    if let Err(error) = pool.drop_schema_cascade(schema).await {
                        warn!(%schema, %error, "failed to drop test schema");
                    }
                }
                pool.close().await;
            }
            Err(error) => warn!(%error, "failed to connect to drop test schemas"),
        }
    }
}

/// A test database: a privileged [`AdminPool`] and the serving pool, both bound to
/// this test's **own unique schema** of the shared server. On postgres [`Drop`]
/// drops the schema; sqlite has a temp file the next run wipes. Derefs to the
/// serving [`TdPool`] so queries read like production.
pub struct TestDb {
    /// Serving pool a test queries.
    pub serving: TdPool,
    /// Privileged (superuser) pool on the same schema — runs upgrades/checks.
    pub admin: AdminPool,
    /// The schema the serving pool is scoped to.
    schema: Schema,
    /// How to drop this test's schema, run on [`Drop`]. [`TestDbCleanup::Noop`] on
    /// sqlite (the temp file is wiped on the next run).
    cleanup: TestDbCleanup,
}

impl Drop for TestDb {
    /// Best-effort teardown of this test's schema. `#[tokio::test]` runs on a
    /// current-thread runtime (so no `block_in_place`) and `Drop` is sync, so run the
    /// async cleanup on a short-lived thread with its own runtime. Failures are logged
    /// by [`TestDbCleanup::run`], never fatal — the next cold start wipes any leftover.
    fn drop(&mut self) {
        let cleanup = std::mem::replace(&mut self.cleanup, TestDbCleanup::Noop);
        if matches!(cleanup, TestDbCleanup::Noop) {
            return;
        }
        let _ = std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                rt.block_on(cleanup.run());
            }
        })
        .join();
    }
}

impl Deref for TestDb {
    type Target = TdPool;

    fn deref(&self) -> &Self::Target {
        &self.serving
    }
}

impl AsRef<DynamicPool> for TestDb {
    fn as_ref(&self) -> &DynamicPool {
        self.serving.as_ref()
    }
}

/// Several components provisioned for one test — one serving pool each (in request
/// order) — plus how to tear them down. On postgres each lives on its own unique
/// schema of the shared server; on sqlite, one shared temp-file database.
pub struct MultiTestDb {
    /// One serving pool per requested component, in request order.
    pub serving: Vec<TdPool>,
    /// How to release this test's database resources once it returns.
    pub cleanup: TestDbCleanup,
}

impl TestDb {
    /// The schema the serving pool is scoped to.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Apply the apiserver schema to its current version, via the real
    /// [`ApiserverComponent`] on the privileged system pool.
    pub async fn upgrade(&self) -> Result<(), DbError> {
        ApiserverComponent.upgrade(&self.admin).await
    }
}
