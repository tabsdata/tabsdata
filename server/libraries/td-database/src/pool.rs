//
// Copyright 2026 Tabs Data Inc.
//

use crate::config::DatabaseConfig;
use crate::dynamic::{DynamicConnection, DynamicPool, DynamicTransaction};
#[cfg(feature = "embedded-postgres")]
use crate::embedded::EmbeddedPostgres;
use crate::error::DbError;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use crate::{APP_ROLE, APP_SCHEMA};
use sea_query::{Alias, Expr, ExprTrait, Iden, Query};
use sqlx::Error;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;
use td_schema::schema::SchemaVersion;
use td_schema::{
    TabsdataSystem, CURRENT_TABSDATA_VERSION, CURRENT_VERSION, DB_EDITION_NAME,
    DB_VERSION_NAME, TABSDATA_VERSION_NAME,
};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, instrument, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct TdPool {
    pub(crate) pool: DynamicPool,
    admin: DynamicPool,
    url: Arc<Url>,
    // Owns the embedded postgres server when this pool was booted via the
    // embedded path. Shared across clones via `Arc` so the server stops
    // only when the last `TdPool` clone drops. `pool` is declared first
    // so the sqlx pool drops before the embedded handle within a single
    // struct drop.
    #[cfg(feature = "embedded-postgres")]
    _embedded: Option<Arc<EmbeddedPostgres>>,
}

impl AsRef<DynamicPool> for TdPool {
    fn as_ref(&self) -> &DynamicPool {
        &self.pool
    }
}

/// Specialized [`Pool`] that uses two pools, one for read-only operations and one for
/// read-write operations.
impl TdPool {
    /// Connects to a database using the given configuration.
    ///
    /// When `config` configured the postgres backend with `embedded: Some(_)`,
    /// the embedded server is booted inside [`DatabaseConfig::connect`] and
    /// the live handle parks here (under `Arc`) so the server stops cleanly
    /// when the last clone of this pool drops. The schema is assumed to be
    /// up to date.
    #[instrument(name = "db.pool.connect", skip_all, level = "info", err(Debug))]
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        info!("db.pool.connect: opening pool");
        let out = config.connect().await?;
        debug!("Connected to database pool: {:#?}", out.pool);
        Ok(Self {
            pool: out.pool,
            admin: out.admin,
            url: Arc::new(out.url),
            #[cfg(feature = "embedded-postgres")]
            _embedded: out.embedded.map(Arc::new),
        })
    }

    /// The URL the pool is actually connected to. For external backends this
    /// matches what was configured; for embedded postgres this is the URL
    /// the booted server is listening on.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Creates a database using the given configuration.
    ///
    /// Creates the schema.
    #[instrument(name = "db.pool.create", skip_all, level = "info", err(Debug))]
    pub async fn create(config: &DatabaseConfig) -> Result<Self, DbError> {
        let this = Self::connect(config).await?;
        this.upgrade().await?;
        Ok(this)
    }

    #[instrument(name = "db.pool.check", skip_all, level = "debug", err(Debug))]
    pub async fn check(&self) -> Result<(), DbError> {
        self.check_tabsdata_version().await?;
        debug!("db.pool.check: tabsdata version OK");
        self.check_db_version().await?;
        debug!("db.pool.check: db version OK");
        self.check_tabsdata_edition().await?;
        debug!("db.pool.check: edition OK");
        self.check_pg_min_version().await?;
        self.check_rls_posture().await?;
        self.check_upgrader_permissions().await?;
        Ok(())
    }

    /// PostgreSQL major version required for `CREATE VIEW … WITH
    /// (security_invoker = true)`, which the schema relies on so views over
    /// RLS tables are evaluated as the querying role (project isolation) rather
    /// than the view owner.
    const MIN_PG_VERSION_NUM: i32 = 150_000; // 15.0

    /// Refuse to run on a PostgreSQL server older than 15. The schema build
    /// emits `security_invoker` views (15+); on an older server the upgrade
    /// would fail with an opaque `CREATE VIEW` syntax error, and — worse — a
    /// hand-built older schema without `security_invoker` would silently
    /// evaluate views as their (superuser) owner and bypass project isolation.
    /// Probe the version explicitly and fail with a clear, actionable error.
    /// PostgreSQL only.
    async fn check_pg_min_version(&self) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let version: Option<i32> = Query::select()
            .expr(Expr::cust("current_setting('server_version_num')::int"))
            .build_query_scalar()
            .fetch_optional(&self.admin)
            .await
            .map_err(DbError::SqlError)?;
        let version = version.unwrap_or(0);
        if version < Self::MIN_PG_VERSION_NUM {
            return Err(DbError::UnsupportedPostgresVersion(
                version,
                Self::MIN_PG_VERSION_NUM,
            ));
        }
        debug!(
            server_version_num = version,
            "db.pool.check: postgres version OK"
        );
        Ok(())
    }

    /// Verify the **admin/upgrader** role has the privileges schema
    /// create/upgrade needs: CREATE on schema `public` (to create and alter
    /// tables). Caught at startup as [`DbError::UpgraderLacksPermissions`]
    /// instead of failing mid-upgrade with an opaque permission error. Runs on
    /// the admin pool (raw role). PostgreSQL only.
    async fn check_upgrader_permissions(&self) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let can_create: Option<bool> = Query::select()
            .expr(Expr::cust(format!(
                "has_schema_privilege(current_user, '{APP_SCHEMA}', 'CREATE')"
            )))
            .build_query_scalar()
            .fetch_optional(&self.admin)
            .await
            .map_err(DbError::SqlError)?;
        if !matches!(can_create, Some(true)) {
            return Err(DbError::UpgraderLacksPermissions(format!(
                "CREATE on schema {APP_SCHEMA}"
            )));
        }
        debug!("db.pool.check: upgrader permissions OK");
        Ok(())
    }

    /// Verify the **serving** pool's connections are correctly scoped for RLS:
    /// `after_connect` switched them to [`APP_ROLE`], and that role does **not**
    /// bypass row-level security (no SUPERUSER / BYPASSRLS). Project isolation
    /// rests entirely on RLS — `FORCE ROW LEVEL SECURITY` only reaches a
    /// non-superuser table owner — so a serving role that bypassed it would let
    /// every project read and write every other project's rows, silently. The
    /// query runs on the serving pool, so `current_user` is the after_connect
    /// role. Fail closed at startup ([`DbError::ConnectedRoleBypassesRls`]).
    /// PostgreSQL only (no RLS elsewhere).
    async fn check_rls_posture(&self) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
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

    pub async fn upgrade(&self) -> Result<(), DbError> {
        info!("Starting database upgrade");
        self.upgrade_db_version().await?;
        self.upgrade_tabsdata_edition().await?;
        self.ensure_app_role().await?;
        info!("Database upgrade complete");
        Ok(())
    }

    /// Grant the row-level-security app role ([`APP_ROLE`]) the schema/table
    /// privileges it needs, run on the admin pool during `upgrade()` — i.e.
    /// after the tables exist. The role itself is created earlier, at connect,
    /// by [`provision_app_role`] (so the serving pools' `after_connect` `SET
    /// ROLE` can succeed); the `CREATE ROLE` guard here is a redundant no-op then.
    async fn ensure_app_role(&self) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let stmts = vec![
            format!(
                "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{APP_ROLE}') \
                 THEN CREATE ROLE \"{APP_ROLE}\" NOBYPASSRLS; END IF; END $$"
            ),
            format!("CREATE SCHEMA IF NOT EXISTS \"{APP_SCHEMA}\""),
            format!("GRANT USAGE ON SCHEMA \"{APP_SCHEMA}\" TO \"{APP_ROLE}\""),
            format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{APP_SCHEMA}\" TO \"{APP_ROLE}\""
            ),
            // Future tables created by this (bootstrap) role auto-grant to the
            // app role, so a later migration that adds a table doesn't silently
            // leave td_app without access. `ALTER DEFAULT PRIVILEGES` applies
            // only to tables created after it runs, by the role running it.
            format!(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{APP_SCHEMA}\" \
                 GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{APP_ROLE}\""
            ),
        ];
        let mut tx = self
            .admin
            .begin()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        tx.as_mut_conn()
            .execute_batch(&stmts)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        tx.commit()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        debug!(role = APP_ROLE, "Ensured RLS app role");
        Ok(())
    }

    fn map_system_db_error(err: Error, row: impl Display) -> DbError {
        match &err {
            Error::RowNotFound => DbError::DatabaseCorrupted(
                format!(
                    "Missing '{row}' row in '{}'",
                    TabsdataSystem::Table.to_string()
                )
                .to_string(),
            ),
            Error::Database(database_err) => {
                if is_undefined_table_error(database_err.code().as_deref(), database_err.message())
                {
                    DbError::DatabaseSchemaDoesNotExist
                } else {
                    DbError::SqlError(err)
                }
            }
            _ => DbError::SqlError(err),
        }
    }

    /// Read a single `value` from `tabsdata_system` keyed by `name`. Missing
    /// rows return `Ok(None)`; only the read itself can fail.
    async fn read_system_value(&self, name: &str) -> Result<Option<String>, DbError> {
        Query::select()
            .column(TabsdataSystem::Value)
            .from(TabsdataSystem::Table)
            .and_where(Expr::col(TabsdataSystem::Name).eq(name))
            .build_query_scalar()
            .fetch_optional(&self.admin)
            .await
            .map_err(|e| Self::map_system_db_error(e, name))
    }

    /// Read the current `db_version` from the database.
    async fn try_read_db_version(&self) -> Result<usize, DbError> {
        let res = self
            .read_system_value(DB_VERSION_NAME)
            .await?
            .ok_or_else(|| {
                DbError::DatabaseCorrupted(format!(
                    "Missing '{DB_VERSION_NAME}' row in '{}'",
                    TabsdataSystem::Table.to_string()
                ))
            })?;
        res.parse::<usize>().map_err(|_| {
            DbError::DatabaseCorrupted(format!(
                "'{DB_VERSION_NAME}' value '{res}' must be an integer"
            ))
        })
    }

    async fn check_db_version(&self) -> Result<(), DbError> {
        let version = self.try_read_db_version().await?;
        match version.cmp(&CURRENT_VERSION) {
            Ordering::Equal => Ok(()),
            Ordering::Less => Err(DbError::DatabaseNeedsUpgrade(format!(
                "Tabsdata database version is '{}', binary database version is '{}'",
                version, CURRENT_VERSION
            ))),
            Ordering::Greater => Err(DbError::DatabaseIsNewer(
                version.to_string(),
                CURRENT_VERSION,
            )),
        }
    }

    /// Read the major Tabsdata version recorded in `tabsdata_system`.
    ///
    /// Pre-2.0 databases lack this row; we map a missing row to `1` so the
    /// startup gate can refuse them with a clear cross-major message.
    async fn try_read_tabsdata_version(&self) -> Result<usize, DbError> {
        match self.read_system_value(TABSDATA_VERSION_NAME).await? {
            None => Ok(1),
            Some(raw) => raw.parse::<usize>().map_err(|_| {
                DbError::DatabaseCorrupted(format!(
                    "'{TABSDATA_VERSION_NAME}' value '{raw}' must be an integer"
                ))
            }),
        }
    }

    async fn check_tabsdata_version(&self) -> Result<(), DbError> {
        let version = self.try_read_tabsdata_version().await?;
        if version == CURRENT_TABSDATA_VERSION {
            Ok(())
        } else {
            Err(DbError::IncompatibleMajorVersion(
                version,
                CURRENT_TABSDATA_VERSION,
            ))
        }
    }

    /// Schema-counter check for the current major version.
    ///
    /// Cross-major upgrade is not supported (see [`check_tabsdata_version`]).
    /// In-major upgrades walk the delta ladder
    /// (`td_schema::schema::deltas::get`) from the stored `db_version` up to
    /// `CURRENT_VERSION`; each delta applies in its own transaction. A
    /// missing delta in the range panics with the upstream
    /// `"No delta migration for version N"` error.
    pub async fn upgrade_db_version(&self) -> Result<(), DbError> {
        match self.try_read_db_version().await {
            Ok(version) if version == CURRENT_VERSION => {
                debug!(version, "Schema version is up to date");
            }
            Ok(version) if version < CURRENT_VERSION => {
                info!(from = version, to = CURRENT_VERSION, "Upgrading schema");
                for target in (version + 1)..=CURRENT_VERSION {
                    let stmts = td_schema::schema::deltas::get(target, self.kind());
                    let mut tx = self
                        .admin
                        .begin()
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    tx.as_mut_conn()
                        .execute_batch(&stmts)
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    tx.commit()
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                    info!(target, "Schema delta applied");
                }
            }
            Ok(version) => {
                warn!(
                    database_version = version,
                    binary_version = CURRENT_VERSION,
                    "Database is newer than binary"
                );
                return Err(DbError::DatabaseIsNewer(
                    version.to_string(),
                    CURRENT_VERSION,
                ));
            }
            Err(DbError::DatabaseSchemaDoesNotExist) => {
                info!(
                    version = CURRENT_VERSION,
                    "No existing schema found, creating fresh install"
                );
                let mut tx = self
                    .admin
                    .begin()
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                let stmts = td_schema::schema::current::Schema::build(self.kind());
                tx.as_mut_conn()
                    .execute_batch(&stmts)
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                tx.commit()
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
                info!(version = CURRENT_VERSION, "Fresh schema created");
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    async fn parse_tabsdata_edition(&self) -> Result<Option<String>, DbError> {
        self.read_system_value(DB_EDITION_NAME).await
    }

    async fn check_tabsdata_edition(&self) -> Result<(), DbError> {
        let instance_edition = self.parse_tabsdata_edition().await?;
        let runtime_edition = TabsdataEdition;

        match instance_edition {
            None => Err(DbError::DatabaseNeedsUpgrade(format!(
                "instance edition is undefined, binary edition is '{}'",
                runtime_edition.label(),
            ))),
            Some(instance_edition) => {
                if !runtime_edition.is_compatible(&instance_edition) {
                    Err(DbError::InvalidEdition(
                        instance_edition,
                        runtime_edition.label().to_string(),
                    ))
                } else if runtime_edition.requires_upgrade(&instance_edition) {
                    Err(DbError::DatabaseNeedsUpgrade(format!(
                        "instance edition is '{instance_edition}', binary edition is '{}'",
                        runtime_edition.label()
                    )))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn upgrade_tabsdata_edition(&self) -> Result<(), DbError> {
        let instance_edition = self.parse_tabsdata_edition().await?;
        let runtime_edition = TabsdataEdition;

        match instance_edition {
            None => {
                info!(edition = runtime_edition.label(), "Setting initial edition");
                Query::insert()
                    .into_table(TabsdataSystem::Table)
                    .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                    .values([DB_EDITION_NAME.into(), runtime_edition.label().into()])
                    .map_err(|e| DbError::CannotUpgradeEdition(e.to_string()))?
                    .build_query()
                    .execute(&self.admin)
                    .await
                    .map_err(DbError::FailedToCreateOrUpgradeDatabaseEdition)?;
                Ok(())
            }
            Some(ref instance_edition) => {
                if !runtime_edition.is_compatible(instance_edition) {
                    warn!(
                        instance = %instance_edition,
                        runtime = runtime_edition.label(),
                        "Incompatible edition"
                    );
                    Err(DbError::CannotUpgradeEdition(format!(
                        "instance edition is '{instance_edition}' cannot be upgraded to '{}'",
                        runtime_edition.label()
                    )))
                } else if runtime_edition.requires_upgrade(instance_edition) {
                    info!(
                        from = %instance_edition,
                        to = runtime_edition.label(),
                        "Upgrading edition"
                    );
                    Query::update()
                        .table(TabsdataSystem::Table)
                        .value(TabsdataSystem::Value, runtime_edition.label())
                        .and_where(Expr::col(TabsdataSystem::Name).eq(DB_EDITION_NAME))
                        .build_query()
                        .execute(self)
                        .await
                        .map_err(DbError::FailedToCreateOrUpgradeDatabaseEdition)?;
                    Ok(())
                } else {
                    debug!(
                        edition = %instance_edition,
                        "Edition is up to date"
                    );
                    Ok(())
                }
            }
        }
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

    /// Install fixture/setup DDL on the **admin** pool, then grant the app role
    /// access to whatever it created. Test-only: the serving pools connect as
    /// the non-owner [`APP_ROLE`], which has no `CREATE` on the schema, so
    /// synthetic fixture tables must be created by the admin (bootstrap) role —
    /// and the serving role then needs explicit privileges to read/write them.
    #[cfg(feature = "test-utils")]
    pub async fn install_fixtures(&self, stmts: &[String]) -> Result<(), Error> {
        let mut tx = self.admin.begin().await?;
        tx.as_mut_conn().execute_batch(stmts).await?;
        if matches!(self.kind(), DatabaseBackend::Postgres) {
            let grant = format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{APP_SCHEMA}\" TO \"{APP_ROLE}\""
            );
            tx.as_mut_conn()
                .execute_batch(std::slice::from_ref(&grant))
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Returns if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.pool.is_closed()
    }

    /// Close the pool, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Close the sqlx pool and, if this pool owns an embedded postgres handle,
    /// stop the server explicitly via `pg_ctl stop --wait`. Use this on a
    /// graceful shutdown path so the embedded server lifecycle is not at the
    /// mercy of Drop ordering after the tokio runtime has been torn down.
    /// Safe to call on a non-embedded pool (no-op for the embedded part).
    pub async fn shutdown(&self) {
        self.pool.close().await;
        #[cfg(feature = "embedded-postgres")]
        if let Some(embedded) = self._embedded.as_ref()
            && let Err(error) = embedded.stop().await
        {
            warn!(%error, "embedded postgres shutdown failed");
        }
    }
}

/// Create the row-level-security app role ([`APP_ROLE`]) if it does not exist,
/// `NOBYPASSRLS`. Called on the admin pool during connect — *before* the
/// serving pools' `after_connect` `SET ROLE td_app` runs — so the switch can
/// succeed. Idempotent, and privilege-free when the role already exists (the
/// `IF NOT EXISTS` guard means external deployments, which pre-create the role,
/// need no `CREATEROLE`). Grants are applied later in [`TdPool::ensure_app_role`]
/// (they need the schema tables to exist first).
#[cfg(postgres)]
pub(crate) async fn provision_app_role(pool: &sqlx::PgPool) -> Result<(), DbError> {
    let stmt = format!(
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{APP_ROLE}') \
         THEN CREATE ROLE \"{APP_ROLE}\" NOBYPASSRLS; END IF; END $$"
    );
    sqlx::query(&stmt)
        .execute(pool)
        .await
        .map_err(DbError::SqlError)?;
    debug!(role = APP_ROLE, "Provisioned RLS app role");
    Ok(())
}

/// True if a database error reports a missing `tabsdata_system` table for any
/// backend enabled at compile time. Each arm is gated on its backend feature so
/// the check stays tight to what's actually linked in.
fn is_undefined_table_error(code: Option<&str>, message: &str) -> bool {
    #[cfg(postgres)]
    if matches!(code, Some("42P01")) {
        return true;
    }
    #[cfg(mysql)]
    if matches!(code, Some("42S02")) {
        return true;
    }
    #[cfg(sqlite)]
    if message.contains("no such table") {
        return true;
    }
    let _ = (code, message);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(postgres)]
    #[test]
    fn undefined_table_error_pg_sqlstate() {
        assert!(is_undefined_table_error(Some("42P01"), ""));
        assert!(!is_undefined_table_error(Some("23505"), ""));
    }

    #[cfg(mysql)]
    #[test]
    fn undefined_table_error_mysql_sqlstate() {
        assert!(is_undefined_table_error(Some("42S02"), ""));
        assert!(!is_undefined_table_error(Some("HY000"), ""));
    }

    #[cfg(sqlite)]
    #[test]
    fn undefined_table_error_sqlite_message() {
        assert!(is_undefined_table_error(None, "no such table: foo"));
        assert!(!is_undefined_table_error(None, "syntax error near 'x'"));
    }
}
