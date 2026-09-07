//
// Copyright 2026 Tabsdata Inc.
//

//! Test database factories. The canonical "give me a fresh database for one
//! test" — used by td-test's db provisioning and, directly, by td-database's
//! own tests.
//!
//! A [`TestDb`] pairs a privileged [`AdminPool`] with the serving [`TdPool`] a
//! test queries. Returned databases are freshly connected but **un-upgraded**;
//! callers that need the schema applied call [`TestDb::upgrade`].
//!
//! Only this crate's own internal factories boot the shared embedded server
//! ([`server`]); the [`postgres::pools`] path the `#[td_test::test(db)]` macro
//! uses takes its connect from the caller, which points it at a tdx-provisioned
//! domain's database.

use crate::components::{AdminPool, ApiserverComponent};
use crate::config::{DatabaseConfig, PoolConfig, PoolsConfig, Schema};
use crate::error::DbError;
use crate::pool::TdPool;
use sqlx::PgPool;
use std::ops::Deref;
use std::time::Duration;
use tracing::warn;

pub mod postgres;
pub mod server;
/// Pool settings for the test pools. One server backs the whole suite, so each
/// pool stays lean and holds **no resident connections**
/// (`min_connections = 0` + short idle timeout: connections open on demand and
/// release promptly, so the many concurrent test processes don't pile up on the one
/// server). A generous acquire timeout lets a test wait out a brief connection-latency
/// spike under load instead of failing.
pub fn test_pool_config() -> PoolsConfig {
    let pool = PoolConfig {
        min_connections: 0,
        // Lean: tests issue mostly-sequential queries, so a tiny cap keeps the total
        // connection demand far below the server limit even at high concurrency
        // (many test processes × admin + serving + teardown pools at once).
        max_connections: 2,
        idle_timeout: Some(Duration::from_secs(1)),
        acquire_timeout: Duration::from_secs(120),
        ..Default::default()
    };
    PoolsConfig {
        regular: Some(pool.clone()),
        exclusive: Some(pool.clone()),
        admin: Some(pool),
    }
}

/// How a [`MultiTestDb`]/[`TestDb`] is torn down once the test returns. Tests
/// isolate on their own per-test schema(s) of the shared server, so cleanup
/// drops those schemas. Run asynchronously after the test body.
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
    /// Nothing to release.
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
/// this test's **own unique schema** of the shared server. [`Drop`] drops the
/// schema. Derefs to the serving [`TdPool`] so queries read like production.
pub struct TestDb {
    /// Serving pool a test queries.
    pub serving: TdPool,
    /// Privileged (superuser) pool on the same schema — runs upgrades/checks.
    pub(crate) admin: AdminPool,
    /// The schema the serving pool is scoped to.
    schema: Schema,
    /// How to drop this test's schema, run on [`Drop`].
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

impl AsRef<PgPool> for TestDb {
    fn as_ref(&self) -> &PgPool {
        self.serving.as_ref()
    }
}

/// Several components provisioned for one test — one serving pool each (in request
/// order) — plus how to tear them down. Each lives on its own unique schema of
/// the shared server.
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
        self.admin.upgrade(&ApiserverComponent).await?;
        Ok(())
    }
}
