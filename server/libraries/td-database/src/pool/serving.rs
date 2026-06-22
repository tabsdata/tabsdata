//
// Copyright 2026 Tabs Data Inc.
//

//! [`TdPool`] — the serving application pool handed to the running server.

use crate::config::DatabaseConfig;
use crate::dynamic::{DynamicConnection, DynamicPool, DynamicTransaction};
use crate::error::DbError;
use crate::pool::connect::ConnectRole;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::DatabaseKind;
use sea_query::{Alias, Expr, Query};
use sqlx::Error;
use td_schema::schema::backend::Backend;
use tracing::{debug, info, instrument, warn};

/// Serving pool handed to the running server and its services. On PostgreSQL its
/// connections run as the non-bypass [`APP_ROLE`](crate::APP_ROLE), so every
/// request is enforced by row-level security. It deliberately carries **no**
/// privileged connection: schema creation/upgrade/checks live in the `system`
/// module's [`SystemPool`](crate::system::SystemPool), built from
/// [`DatabaseConfig::connect`] with [`ConnectRole::Admin`].
#[derive(Debug, Clone)]
pub struct TdPool {
    pub(crate) pool: DynamicPool,
}

impl AsRef<DynamicPool> for TdPool {
    fn as_ref(&self) -> &DynamicPool {
        &self.pool
    }
}

/// Specialized [`Pool`] that uses two pools, one for read-only operations and one for
/// read-write operations.
impl TdPool {
    /// Connects to an already-running database as the **serving** role
    /// (RLS-enforced `td_app` on PostgreSQL). This is a pure application pool: it
    /// holds no privileged connection and does not own the database server's
    /// lifecycle — the embedded server, when used, is booted out of band
    /// (`td_dbmanager start`), and this pool connects to it by URL. The schema is
    /// assumed to be up to date.
    #[instrument(name = "db.pool.connect", skip_all, level = "info", err(Debug))]
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        info!("db.pool.connect: opening serving pool");
        let out = config.connect(ConnectRole::Serving).await?;
        debug!("Connected to database pool: {:#?}", out.pool);
        Ok(Self { pool: out.pool })
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
    pub async fn check_rls_posture(&self) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
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

    /// Verify the schema's recorded `db_version` matches `source`'s current
    /// version. A plain read, so the serving pool can run it at startup without
    /// privileged access. See [`crate::pool::state`].
    pub async fn check_db_version(&self, source: &dyn td_schema::SchemaSource) -> Result<(), DbError> {
        crate::pool::state::check_db_version(&self.pool, source).await
    }

    /// The recorded `db_version`, or `None` when the schema does not yet exist.
    pub async fn db_version(&self) -> Result<Option<usize>, DbError> {
        crate::pool::state::db_version(&self.pool).await
    }

    /// Verify the recorded major Tabsdata version is the one this binary speaks.
    pub async fn check_tabsdata_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_tabsdata_version(&self.pool).await
    }

    /// Verify the recorded edition is compatible with and current for this binary.
    pub async fn check_tabsdata_edition(&self) -> Result<(), DbError> {
        crate::pool::state::check_tabsdata_edition(&self.pool).await
    }

    /// Verify the PostgreSQL server is new enough for the schema's features.
    pub async fn check_pg_min_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_pg_min_version(&self.pool).await
    }

    /// Delegates to the read-only pool's [`Pool::acquire`] method.
    pub async fn acquire<'c>(&self) -> Result<DynamicConnection<'c>, Error> {
        let start = std::time::Instant::now();
        let result = self.pool.acquire().await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        if elapsed_ms > 100 {
            warn!(elapsed_ms, "db.pool.acquire: slow acquisition");
        }
        result
    }

    /// Delegates to [`DynamicPool::acquire_for_read`].
    pub async fn acquire_for_read<'c>(&self) -> Result<DynamicConnection<'c>, Error> {
        self.pool.acquire_for_read().await
    }

    /// Delegates to the read-write pool's [`Pool::begin`] method.
    pub async fn begin<'c>(&self) -> Result<DynamicTransaction<'c>, Error> {
        let start = std::time::Instant::now();
        let result = self.pool.begin().await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        if elapsed_ms > 100 {
            warn!(elapsed_ms, "db.pool.begin: slow transaction start");
        }
        result
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
