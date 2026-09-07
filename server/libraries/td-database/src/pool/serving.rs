//
// Copyright 2026 Tabsdata Inc.
//

//! [`TdPool`] — the serving application pool handed to the running server.

use crate::config::DatabaseConfig;
use crate::connection::PgConnectionExt;
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::sea_query::builder::SeaQueryExt;
use sea_query::{Alias, Expr, Query};
use sqlx::PgPool;
use sqlx::{Error, Postgres, Transaction};
use std::time::Duration;
use tracing::{debug, info, instrument};

/// Serving pool handed to the running server and its services. On PostgreSQL its
/// connections run as the non-bypass [`APP_ROLE`](crate::APP_ROLE), so every
/// request is enforced by row-level security. It deliberately carries **no**
/// privileged connection: schema creation/upgrade/checks live in the `components`
/// module's [`AdminPool`](crate::components::AdminPool).
#[derive(Debug, Clone)]
pub struct TdPool {
    pub(crate) pool: PgPool,
    /// Warn/lock identity: the component key ("db" outside the serving map).
    pub component: &'static str,
    /// Warn/lock identity: which of the component's pools this is.
    pub kind: PoolKind,
    /// Also bounds how long an exclusive connection queues on its project's
    /// lock.
    pub(crate) acquire_timeout: Duration,
}

impl AsRef<PgPool> for TdPool {
    fn as_ref(&self) -> &PgPool {
        &self.pool
    }
}

impl TdPool {
    /// Connects to an already-running database as the **serving** role
    /// (RLS-enforced `td_app` on PostgreSQL). This is a pure application pool: it
    /// holds no privileged connection and does not own the database server's
    /// lifecycle — the embedded server, when used, is booted out of band
    /// (`td_dbmanager start`), and this pool connects to it like any other
    /// server. The schema is
    /// assumed to be up to date.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        Self::connect_as(config, PoolKind::Regular).await
    }

    /// [`Self::connect`] opening `kind`'s pool — a `TdPool` is always one of
    /// a component's serving pools, never the admin connection.
    #[instrument(name = "db.pool.connect", skip(config), level = "info", err(Debug))]
    pub(crate) async fn connect_as(
        config: &DatabaseConfig,
        kind: PoolKind,
    ) -> Result<Self, DbError> {
        info!("db.pool.connect: opening serving pool");
        let out = config.connect_serving(kind).await?;
        debug!("Connected to database pool: {:#?}", out.pool);
        Ok(Self {
            pool: out.pool,
            component: "db",
            kind,
            acquire_timeout: out.acquire_timeout,
        })
    }

    pub fn of_component(mut self, component: &'static str) -> Self {
        self.component = component;
        self
    }

    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Verify the **serving** pool's connections are correctly scoped for RLS:
    /// `after_connect` switched them to [`APP_ROLE`](crate::APP_ROLE), and that
    /// role does **not** bypass row-level security (no SUPERUSER / BYPASSRLS).
    /// Project isolation rests entirely on RLS — `FORCE ROW LEVEL SECURITY` only
    /// reaches a non-superuser table owner — so a serving role that bypassed it
    /// would let every project read and write every other project's rows,
    /// silently. The query runs on the serving pool, so `current_user` is the
    /// after_connect role. Fail closed at startup
    /// ([`DbError::ConnectedRoleBypassesRls`]). PostgreSQL only (no RLS elsewhere).
    pub(crate) async fn check_rls_posture(&self) -> Result<(), DbError> {
        let bypasses: Option<bool> = Query::select()
            .expr(Expr::cust("rolsuper OR rolbypassrls"))
            .from(Alias::new("pg_roles"))
            .and_where(Expr::cust("rolname = current_user"))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)?;
        if matches!(bypasses, Some(true)) {
            return Err(DbError::ConnectedRoleBypassesRls);
        }
        debug!("db.pool.check: serving RLS posture OK");
        Ok(())
    }

    /// Run `stmts` as one committed transaction on the serving pool — used by a
    /// component's [`seed`](crate::components::SchemaComponent::seed) to apply
    /// its idempotent baseline-content sync.
    pub async fn execute_batch(&self, stmts: &[String]) -> Result<(), DbError> {
        let mut conn = self.begin().await.map_err(DbError::SqlError)?;
        conn.execute_batch(stmts).await.map_err(DbError::SqlError)?;
        conn.commit().await.map_err(DbError::SqlError)?;
        Ok(())
    }

    /// Verify the schema's recorded `db_version` matches `source`'s current
    /// version. A plain read, so the serving pool can run it at startup without
    /// privileged access. See [`crate::pool::state`].
    pub async fn check_db_version(
        &self,
        source: &dyn td_schema::SchemaSource,
    ) -> Result<(), DbError> {
        crate::pool::state::check_db_version(&self.pool, source).await
    }

    /// The recorded `db_version`, or `None` when the schema does not yet exist.
    pub async fn db_version(&self) -> Result<Option<usize>, DbError> {
        crate::pool::state::db_version(&self.pool).await
    }

    /// Verify the recorded major Tabsdata version is the one this binary speaks.
    pub(crate) async fn check_tabsdata_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_tabsdata_version(&self.pool).await
    }

    /// Verify the recorded edition is compatible with and current for this binary.
    pub(crate) async fn check_tabsdata_edition(&self) -> Result<(), DbError> {
        crate::pool::state::check_tabsdata_edition(&self.pool).await
    }

    /// Verify the PostgreSQL server is new enough for the schema's features.
    pub async fn check_pg_min_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_pg_min_version(&self.pool).await
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Postgres>, Error> {
        self.pool.begin().await
    }

    /// `READ ONLY`, which the RLS `SET LOCAL` needs to scope to and in which a
    /// stray write fails loudly.
    pub async fn begin_ro(&self) -> Result<Transaction<'static, Postgres>, Error> {
        self.pool.begin_with("BEGIN READ ONLY").await
    }

    /// Returns if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.pool.is_closed()
    }

    /// Close the pool, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Close the sqlx pool, waiting for connections to drain. The serving pool
    /// owns no server lifecycle, so this is the serving-side of a graceful
    /// shutdown; the embedded server is stopped out of band (`td_dbmanager stop`).
    pub async fn shutdown(&self) {
        self.pool.close().await;
    }
}
