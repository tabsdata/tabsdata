//
// Copyright 2026 Tabsdata Inc.
//

//! [`TdPool`] — the serving application pool handed to the running server.

use crate::config::DatabaseConfig;
use crate::connection::{IN_PROJECT_ROLE_GUC, PgConnectionExt};
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::catalog::{current_setting, current_user, has_role};
use sea_query::{Alias, Expr, ExprTrait, Query};
use sqlx::PgPool;
use sqlx::{Error, Postgres, Transaction};
use std::time::Duration;
use td_schema::ACROSS_PROJECTS_ROLE;
use tracing::{debug, info, instrument};

/// Serving pool handed to the running server and its services. On PostgreSQL its
/// connections run as the non-bypass tenant RLS role, so every
/// request is enforced by row-level security. It deliberately carries **no**
/// privileged connection: schema creation/upgrade/checks live in the `components`
/// module's [`AdminPool`](crate::components::AdminPool).
#[derive(Debug, Clone)]
pub struct TdPool {
    pub(crate) pool: PgPool,
    /// Warn/lock identity: the component key ("db" outside the serving map).
    pub component: &'static str,
    /// Also bounds how long a unit waits on its project's lock.
    pub(crate) acquire_timeout: Duration,
}

impl AsRef<PgPool> for TdPool {
    fn as_ref(&self) -> &PgPool {
        &self.pool
    }
}

impl TdPool {
    /// Connects to an already-running database as the **serving** role
    /// (the RLS-enforced tenant role). This is a pure application pool: it
    /// holds no privileged connection and does not own the database server's
    /// lifecycle — the embedded server, when used, is booted out of band
    /// (`td_dbmanager start`), and this pool connects to it like any other
    /// server. The schema is
    /// assumed to be up to date.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        Self::connect_kind(config, PoolKind::Regular).await
    }

    #[instrument(name = "db.pool.connect", skip(config), level = "info", err(Debug))]
    pub(crate) async fn connect_kind(
        config: &DatabaseConfig,
        kind: PoolKind,
    ) -> Result<Self, DbError> {
        info!(%kind, "db.pool.connect: opening serving pool");
        let out = config.connect_serving(kind).await?;
        debug!("Connected to database pool: {:#?}", out.pool);
        Ok(Self {
            pool: out.pool,
            component: "db",
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
    /// the role `after_connect` switched them to does **not** bypass
    /// row-level security (no SUPERUSER / BYPASSRLS), and the tenant role does
    /// not hold the `across_projects` membership — which would make that
    /// policy apply to tenant reads and collapse isolation.
    /// Project isolation rests entirely on RLS — `FORCE ROW LEVEL SECURITY` only
    /// reaches a non-superuser table owner — so a serving role that bypassed it
    /// would let every project read and write every other project's rows,
    /// silently. The query runs on the serving pool, so `current_user` is the
    /// after_connect role. Fail closed at startup
    /// ([`DbError::ConnectedRoleBypassesRls`]). PostgreSQL only (no RLS elsewhere).
    pub(crate) async fn check_rls_posture(&self) -> Result<(), DbError> {
        let bypasses: Option<bool> = Query::select()
            .expr(Expr::col(Alias::new("rolsuper")).or(Expr::col(Alias::new("rolbypassrls"))))
            .from(Alias::new("pg_roles"))
            .and_where(Expr::col(Alias::new("rolname")).eq(current_user()))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)?;
        if matches!(bypasses, Some(true)) {
            return Err(DbError::ConnectedRoleBypassesRls);
        }
        let tenant_bypasses: Option<bool> = Query::select()
            .expr(has_role(
                current_setting(IN_PROJECT_ROLE_GUC),
                ACROSS_PROJECTS_ROLE,
                "USAGE",
            ))
            .build_query_scalar()
            .fetch_optional(&self.pool)
            .await
            .map_err(DbError::SqlError)?;
        if matches!(tenant_bypasses, Some(true)) {
            return Err(DbError::TenantRoleSpansProjects);
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

    /// The random identity the database was created with.
    pub async fn instance_id(&self) -> Result<String, DbError> {
        crate::pool::state::instance_id(&self.pool).await
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
        let pool = self.pool.clone();
        uncancellable(async move { pool.begin().await }).await
    }

    /// Commits only what a serial order of the concurrent transactions would
    /// have; one that cannot fails with a serialization failure (40001) and is
    /// run again whole.
    pub async fn begin_serializable(&self) -> Result<Transaction<'static, Postgres>, Error> {
        let pool = self.pool.clone();
        uncancellable(async move { pool.begin_with("BEGIN ISOLATION LEVEL SERIALIZABLE").await })
            .await
    }

    /// One snapshot for every statement, and a conflict (40001) on updating a
    /// row another transaction changed since; what it reads to decide is
    /// kept still by a lock its caller holds, not by the database.
    pub async fn begin_repeatable(&self) -> Result<Transaction<'static, Postgres>, Error> {
        let pool = self.pool.clone();
        uncancellable(async move {
            pool.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
                .await
        })
        .await
    }

    /// `READ ONLY`, which the RLS `SET LOCAL` needs to scope to and in which a
    /// stray write fails loudly; `REPEATABLE READ`, so all its statements read
    /// one snapshot and a commit cannot land between two of them. A read-only
    /// transaction never fails to serialize there.
    pub async fn begin_ro(&self) -> Result<Transaction<'static, Postgres>, Error> {
        let pool = self.pool.clone();
        uncancellable(async move {
            pool.begin_with("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")
                .await
        })
        .await
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

/// Runs a transaction's opening to completion even when its caller is dropped
/// (an HTTP handler whose client went away): sqlx 0.9.0 raises the transaction
/// depth only once `BEGIN` has answered, so a drop in between queues no
/// rollback and the pool hands the next caller a connection still inside the
/// orphaned transaction. A completed but unclaimed transaction is dropped with
/// the task, which queues its rollback.
///
/// TODO: remove once the workspace's sqlx carries
/// <https://github.com/launchbadge/sqlx/pull/4394> (merged 2026-09-09 as
/// `f1e94ec`, unreleased as of 0.9.0); tracked by
/// <https://github.com/launchbadge/sqlx/issues/2805> and the release request
/// <https://github.com/launchbadge/sqlx/issues/4495>. Removing it must keep
/// `pg_begin_cancelled_in_flight_leaves_no_transaction` passing.
async fn uncancellable<T: Send + 'static>(
    opening: impl Future<Output = Result<T, Error>> + Send + 'static,
) -> Result<T, Error> {
    tokio::spawn(opening)
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))?
}
