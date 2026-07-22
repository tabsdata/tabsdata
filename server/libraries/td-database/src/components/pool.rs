//
// Copyright 2026 Tabsdata Inc.
//

//! [`AdminPool`] — the privileged admin/upgrader pool used by the `components` module.

use crate::components::sql::execute_batch_tx;
#[cfg(postgres)]
use crate::components::sql::{
    create_owner_role_sql, create_schema_sql, grant_app_role_membership_sql,
    grant_app_role_privileges_sql,
};
use crate::config::{DatabaseConfig, Schema};
use crate::dynamic::DynamicPool;
use crate::error::DbError;
use crate::pool::connect::ConnectRole;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use sea_query::{Expr, ExprTrait, OnConflict, Query};
use td_schema::SchemaSource;
use td_schema::schema::backend::Backend;
use td_schema::{DB_EDITION_NAME, DB_VERSION_NAME, TabsdataSystem, tabsdata_system_ddl};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, instrument, warn};

/// The privileged pool used to provision, upgrade and check schemas. On
/// PostgreSQL it keeps the DDL rights the serving `TdPool` gives up: the
/// superuser variant runs `CREATE SCHEMA`/`CREATE ROLE`; the per-component
/// variant connects as the schema owner. Short-lived around a management op.
#[derive(Debug, Clone)]
pub struct AdminPool {
    pool: DynamicPool,
    // Stored (not passed) to stay in lock-step with the connection's
    // `search_path`. `None` on SQLite/MySQL.
    schema: Option<Schema>,
    // The RLS app role this pool provisions/grants: `td_app` in production, a
    // per-test role in tests.
    app_role: String,
}

impl DatabaseKind for AdminPool {
    fn kind(&self) -> DatabaseBackend {
        self.pool.kind()
    }
}

impl AdminPool {
    /// Connect as a component's **admin/upgrader** role: a raw-role pool with
    /// DDL rights on its (already existing) schema.
    #[instrument(name = "db.admin_pool.connect", skip_all, level = "info", err(Debug))]
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        info!("db.admin_pool.connect: opening admin pool");
        let app_role = config.app_role().to_owned();
        let out = config.connect(ConnectRole::Admin).await?;
        debug!("Connected to admin pool: {:#?}", out.pool);
        Ok(Self::from_parts(out.pool, out.schema, app_role))
    }

    /// Build from an already-opened admin connection.
    pub(crate) fn from_parts(pool: DynamicPool, schema: Option<Schema>, app_role: String) -> Self {
        Self {
            pool,
            schema,
            app_role,
        }
    }

    /// The PostgreSQL schema this pool's `search_path` is pinned to. Panics on
    /// a non-PostgreSQL backend; guard on the backend kind first.
    pub fn schema(&self) -> &Schema {
        self.schema
            .as_ref()
            .expect("No schema configured: only the PostgreSQL backend carries one. Guard on the backend kind before calling schema()")
    }

    /// Provision a schema: the login-capable owner role, the schema it owns,
    /// and the owner's [`APP_ROLE`] membership. Idempotent. Table grants come
    /// later ([`Self::ensure_app_role`]), once tables exist.
    pub async fn provision_schema(
        &self,
        schema: &str,
        owner: &str,
        password: &str,
    ) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            let stmts = vec![
                create_owner_role_sql(owner, password),
                create_schema_sql(schema, owner),
                grant_app_role_membership_sql(owner, &self.app_role),
            ];
            execute_batch_tx(&self.pool, &stmts).await?;
            debug!(
                owner,
                schema,
                role = self.app_role,
                "Provisioned schema and owner"
            );
        }
        Ok(())
    }

    /// Create a schema owned by the connected (superuser) role — the test
    /// factories' path, free of any cluster-global `CREATE ROLE` so tests
    /// provision in parallel. Idempotent.
    pub async fn create_schema(&self, schema: &str) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        execute_batch_tx(
            &self.pool,
            &[format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"")],
        )
        .await
    }

    /// Drop a schema and everything in it, leaving roles untouched.
    pub async fn drop_schema_cascade(&self, schema: &str) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        execute_batch_tx(
            &self.pool,
            &[format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE")],
        )
        .await
    }

    /// Drop a component's schema and its owner role. Destructive; the caller
    /// guards.
    pub async fn drop_schema(&self, schema: &str, owner: &str) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        let stmts = vec![
            format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"),
            format!("DROP ROLE IF EXISTS \"{owner}\""),
        ];
        execute_batch_tx(&self.pool, &stmts).await?;
        debug!(owner, schema, "Dropped schema and owner");
        Ok(())
    }

    /// Refuse a PostgreSQL server older than 15: the schema emits
    /// `security_invoker` views (15+); without them views would evaluate as
    /// their superuser owner and bypass project isolation.
    pub async fn check_pg_min_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_pg_min_version(&self.pool).await
    }

    /// Verify the upgrader role has CREATE on the application schema — caught
    /// up front instead of failing mid-upgrade with an opaque error.
    pub async fn check_upgrader_permissions(&self) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        let schema = self.schema();
        let can_create: Option<bool> = Query::select()
            .expr(Expr::cust(format!(
                "has_schema_privilege(current_user, '{schema}', 'CREATE')"
            )))
            .build_query_scalar()
            .fetch_optional(&self.pool)
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

    /// Grant the RLS app role its schema/table privileges, run during
    /// `upgrade()` once the tables exist. Runs as the schema owner.
    pub async fn ensure_app_role(&self) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            // `ALTER DEFAULT PRIVILEGES` auto-grants future tables, so a later
            // migration doesn't silently leave the app role without access.
            let stmts = grant_app_role_privileges_sql(self.schema(), &self.app_role);
            execute_batch_tx(&self.pool, &stmts).await?;
            debug!(role = self.app_role, "Ensured RLS app role");
        }
        Ok(())
    }

    /// The recorded `db_version`, or `None` when the schema does not exist.
    pub async fn db_version(&self) -> Result<Option<usize>, DbError> {
        crate::pool::state::db_version(&self.pool).await
    }

    /// Walk the delta ladder from the stored `db_version` up to the source's
    /// current version (or build fresh when absent); each delta in its own
    /// transaction. Refuses a database newer than the binary.
    pub async fn upgrade_db_version(&self, source: &dyn SchemaSource) -> Result<(), DbError> {
        // The pool, not the schema's SQL, owns the `tabsdata_system` ledger
        // and its `db_version` row.
        self.ensure_system_table().await?;

        let current = source.current_version();
        match crate::pool::state::db_version(&self.pool).await? {
            Some(version) if version == current => {
                debug!(version, "Schema version is up to date");
            }
            Some(version) if version < current => {
                info!(from = version, to = current, "Upgrading schema");
                for target in (version + 1)..=current {
                    let stmts = source.delta(target, self.kind().into());
                    execute_batch_tx(&self.pool, &stmts).await?;
                    info!(target, "Schema delta applied");
                }
                self.set_db_version(current).await?;
            }
            Some(version) => {
                warn!(
                    database_version = version,
                    binary_version = current,
                    "Database is newer than binary"
                );
                return Err(DbError::DatabaseIsNewer(version.to_string(), current));
            }
            None => {
                info!(
                    version = current,
                    "No existing schema found, creating fresh install"
                );
                let stmts = source.build(self.kind().into());
                execute_batch_tx(&self.pool, &stmts).await?;
                self.set_db_version(current).await?;
                info!(version = current, "Fresh schema created");
            }
        }
        Ok(())
    }

    /// Create this schema's `tabsdata_system` ledger if absent — idempotent.
    async fn ensure_system_table(&self) -> Result<(), DbError> {
        execute_batch_tx(&self.pool, &[tabsdata_system_ddl(self.kind().into())]).await
    }

    /// Upsert the `db_version` row — works for fresh install and upgrade.
    async fn set_db_version(&self, version: usize) -> Result<(), DbError> {
        Query::insert()
            .into_table(TabsdataSystem::Table)
            .columns([TabsdataSystem::Name, TabsdataSystem::Value])
            .values_panic([DB_VERSION_NAME.into(), version.to_string().into()])
            .on_conflict(
                OnConflict::column(TabsdataSystem::Name)
                    .update_column(TabsdataSystem::Value)
                    .to_owned(),
            )
            .build_query()
            .execute(&self.pool)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        Ok(())
    }

    pub async fn upgrade_tabsdata_edition(&self) -> Result<(), DbError> {
        let instance_edition = crate::pool::state::edition(&self.pool).await?;
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
                    .execute(&self.pool)
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
                        .execute(&self.pool)
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

    /// Close the pool, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
