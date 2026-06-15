//
// Copyright 2026 Tabs Data Inc.
//

use crate::APP_ROLE;
use crate::config::{DatabaseConfig, Schema};
use crate::dynamic::{DynamicConnection, DynamicPool, DynamicTransaction};
#[cfg(feature = "embedded-postgres")]
use crate::embedded::EmbeddedPostgres;
use crate::error::DbError;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use sea_query::{Alias, Expr, ExprTrait, Iden, Query};
use sqlx::Error;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;
use td_schema::schema::SchemaVersion;
use td_schema::{
    CURRENT_TABSDATA_VERSION, CURRENT_VERSION, DB_EDITION_NAME, DB_VERSION_NAME,
    TABSDATA_VERSION_NAME, TabsdataSystem,
};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, instrument, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct TdPool {
    pub(crate) pool: DynamicPool,
    admin: DynamicPool,
    url: Arc<Url>,
    // The PostgreSQL schema the connections' `search_path` is pinned to, used to
    // scope schema-qualified DDL and privilege checks. `None` on SQLite/MySQL,
    // which have no `search_path`.
    schema: Option<Schema>,
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
            schema: out.schema,
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

    /// The PostgreSQL schema this pool's `search_path` is pinned to. Panics on a
    /// non-PostgreSQL backend (SQLite/MySQL have no `search_path`), so callers
    /// must guard on the backend kind first; on the PostgreSQL path it is
    /// mandatory and always present.
    pub fn schema(&self) -> &Schema {
        self.schema
            .as_ref()
            .expect("No schema configured: only the PostgreSQL backend carries one. Guard on the backend kind before calling schema()")
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
    pub(crate) async fn check_pg_min_version(&self) -> Result<(), DbError> {
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
    /// create/upgrade needs: CREATE on the application schema (to create and
    /// alter tables). Caught at startup as [`DbError::UpgraderLacksPermissions`]
    /// instead of failing mid-upgrade with an opaque permission error. Runs on
    /// the admin pool (raw role). PostgreSQL only.
    pub(crate) async fn check_upgrader_permissions(&self) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let schema = self.schema();
        let can_create: Option<bool> = Query::select()
            .expr(Expr::cust(format!(
                "has_schema_privilege(current_user, '{schema}', 'CREATE')"
            )))
            .build_query_scalar()
            .fetch_optional(&self.admin)
            .await
            .map_err(DbError::SqlError)?;
        if !matches!(can_create, Some(true)) {
            return Err(DbError::UpgraderLacksPermissions(format!(
                "CREATE on schema {schema}"
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
    pub(crate) async fn check_rls_posture(&self) -> Result<(), DbError> {
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

    /// Grant the row-level-security app role ([`APP_ROLE`]) the schema/table
    /// privileges it needs, run on the admin pool during `upgrade()` — i.e.
    /// after the tables exist. The role, schema and owner are created earlier in
    /// the provisioning phase by [`Self::provision_schema`]; the upgrade phase
    /// runs as the schema owner, which can grant on what it owns but cannot
    /// `CREATE ROLE` or `CREATE SCHEMA`.
    pub async fn ensure_app_role(&self) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let schema = self.schema();
        // Future tables created by this (owner) role auto-grant to the app
        // role, so a later migration that adds a table doesn't silently leave
        // td_app without access. `ALTER DEFAULT PRIVILEGES` applies only to
        // tables created after it runs, by the role running it.
        let stmts = grant_app_role_privileges_sql(schema);
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

    /// Provision, on the admin pool, everything an externally-hosted schema needs
    /// before its owner can populate it: the login-capable owner role, the
    /// [`APP_ROLE`] the serving pools switch into, the schema owned by the owner,
    /// and the owner's membership in [`APP_ROLE`] so its connections'
    /// `after_connect` `SET ROLE` succeeds. Idempotent: each object is left in
    /// place when it already exists. The [`APP_ROLE`] table grants are applied
    /// later by [`Self::ensure_app_role`], once the tables exist.
    pub async fn provision_schema(
        &self,
        schema: &str,
        owner: &str,
        password: &str,
    ) -> Result<(), DbError> {
        if !matches!(self.kind(), DatabaseBackend::Postgres) {
            return Ok(());
        }
        let stmts = vec![
            create_owner_role_sql(owner, password),
            create_app_role_sql(),
            create_schema_sql(schema, Some(owner)),
            grant_app_role_membership_sql(owner),
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
        debug!(
            owner,
            schema,
            role = APP_ROLE,
            "Provisioned schema and owner"
        );
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

    pub(crate) async fn check_db_version(&self) -> Result<(), DbError> {
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

    pub(crate) async fn check_tabsdata_version(&self) -> Result<(), DbError> {
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

    pub(crate) async fn check_tabsdata_edition(&self) -> Result<(), DbError> {
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

    pub(crate) async fn upgrade_tabsdata_edition(&self) -> Result<(), DbError> {
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
            let grant = grant_app_role_tables_sql(self.schema());
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

#[cfg(postgres)]
fn create_app_role_sql() -> String {
    format!(
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{APP_ROLE}') \
         THEN CREATE ROLE \"{APP_ROLE}\" NOBYPASSRLS; END IF; END $$"
    )
}

#[cfg(postgres)]
fn create_owner_role_sql(owner: &str, password: &str) -> String {
    format!(
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{owner}') \
         THEN CREATE ROLE \"{owner}\" LOGIN PASSWORD '{password}'; END IF; END $$"
    )
}

#[cfg(postgres)]
fn create_schema_sql(schema: &str, owner: Option<&str>) -> String {
    match owner {
        Some(owner) => {
            format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\" AUTHORIZATION \"{owner}\"")
        }
        None => format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""),
    }
}

#[cfg(postgres)]
fn grant_app_role_membership_sql(owner: &str) -> String {
    format!("GRANT \"{APP_ROLE}\" TO \"{owner}\"")
}

#[cfg(postgres)]
fn grant_app_role_tables_sql(schema: &str) -> String {
    format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{APP_ROLE}\""
    )
}

#[cfg(postgres)]
fn grant_app_role_privileges_sql(schema: &str) -> [String; 3] {
    [
        format!("GRANT USAGE ON SCHEMA \"{schema}\" TO \"{APP_ROLE}\""),
        grant_app_role_tables_sql(schema),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA \"{schema}\" \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{APP_ROLE}\""
        ),
    ]
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
    sqlx::query(&create_app_role_sql())
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
