//
// Copyright 2026 Tabs Data Inc.
//

//! [`SystemPool`] — the privileged system/upgrader pool used by the `system` module.

use crate::config::{DatabaseConfig, Schema};
use crate::dynamic::DynamicPool;
use crate::error::DbError;
use crate::pool::connect::ConnectRole;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use crate::system::sql::execute_batch_tx;
#[cfg(postgres)]
use crate::system::sql::{
    create_owner_role_sql, create_schema_sql, grant_app_role_membership_sql,
    grant_app_role_privileges_sql,
};
use sea_query::{Expr, ExprTrait, OnConflict, Query};
use td_schema::SchemaSource;
use td_schema::schema::backend::Backend;
use td_schema::{DB_EDITION_NAME, DB_VERSION_NAME, TabsdataSystem, tabsdata_system_ddl};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, instrument, warn};

/// The privileged pool the `system` module uses to provision, upgrade and check
/// schemas. On PostgreSQL it keeps the DDL rights the serving `TdPool` gives up:
/// the superuser variant (`connect_system`, no pinned schema) runs `CREATE
/// SCHEMA`/`CREATE ROLE`; the per-component variant connects as the schema owner
/// (raw role, no `SET ROLE`) scoped to its own schema for upgrades. Short-lived
/// around a management op.
#[derive(Debug, Clone)]
pub struct SystemPool {
    pool: DynamicPool,
    // The schema this pool's connections are pinned to. Every grant/privilege
    // statement is scoped to it, so it is stored (not passed) to keep it in
    // lock-step with the connection's `search_path`. `None` on SQLite/MySQL.
    schema: Option<Schema>,
    // The RLS app role this pool provisions/grants (the role serving connections
    // `SET ROLE` to). The global `td_app` in production; a per-test role in tests.
    app_role: String,
}

impl DatabaseKind for SystemPool {
    fn kind(&self) -> DatabaseBackend {
        self.pool.kind()
    }
}

impl SystemPool {
    /// Connect as a component's **system/upgrader** role: a raw-role pool with
    /// DDL rights on its schema (and, on PostgreSQL, idempotent [`APP_ROLE`]
    /// provisioning). The schema/owner must already exist (created by
    /// [`Self::provision_schema`]).
    #[instrument(name = "db.system_pool.connect", skip_all, level = "info", err(Debug))]
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        info!("db.system_pool.connect: opening system pool");
        let app_role = config.app_role().to_owned();
        let out = config.connect(ConnectRole::Admin).await?;
        debug!("Connected to system pool: {:#?}", out.pool);
        Ok(Self::from_parts(out.pool, out.schema, app_role))
    }

    /// Build from an already-opened admin connection — its pool, pinned schema, and
    /// the app role it provisions/grants. Lets the test factory keep the live URL
    /// from the same [`ConnectOutput`](crate::pool::connect::ConnectOutput) separately.
    pub(crate) fn from_parts(pool: DynamicPool, schema: Option<Schema>, app_role: String) -> Self {
        Self {
            pool,
            schema,
            app_role,
        }
    }

    /// The PostgreSQL schema this pool's `search_path` is pinned to. Panics on a
    /// non-PostgreSQL backend (SQLite/MySQL have no `search_path`); guard on the
    /// backend kind first.
    pub fn schema(&self) -> &Schema {
        self.schema
            .as_ref()
            .expect("No schema configured: only the PostgreSQL backend carries one. Guard on the backend kind before calling schema()")
    }

    /// Provision what an externally-hosted schema needs before its owner can
    /// populate it: the login-capable owner role, the schema it owns, and the
    /// owner's [`APP_ROLE`] membership (so its connections' `after_connect`
    /// `SET ROLE` succeeds). Idempotent. Table grants come later, in
    /// [`Self::ensure_app_role`], once tables exist. Requires `CREATE ROLE`/`CREATE
    /// SCHEMA` — run on the system pool.
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

    /// Create a schema owned by the connected (superuser) role — no separate owner
    /// role. Used by the test factories, where superuser ownership keeps the per-test
    /// path free of any cluster-global `CREATE ROLE` (so tests provision in parallel).
    /// Idempotent. No-op on non-PostgreSQL backends.
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

    /// Drop a schema and everything in it, leaving roles untouched. Best-effort
    /// per-test cleanup. No-op on non-PostgreSQL backends.
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

    /// Drop a component's schema and its owner role. The shared app role is left
    /// untouched. Destructive; the caller guards. No-op on non-PostgreSQL backends.
    /// Requires `DROP ROLE` — run on the system/bootstrap pool.
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

    /// Refuse to run on a PostgreSQL server older than 15. The schema build emits
    /// `security_invoker` views (15+); on an older server the upgrade fails with
    /// an opaque error, and a hand-built older schema would silently evaluate
    /// views as their (superuser) owner and bypass project isolation. PostgreSQL
    /// only.
    pub async fn check_pg_min_version(&self) -> Result<(), DbError> {
        crate::pool::state::check_pg_min_version(&self.pool).await
    }

    /// Verify the **system/upgrader** role has the privileges schema
    /// create/upgrade needs: CREATE on the application schema (to create and
    /// alter tables). Caught at startup as [`DbError::UpgraderLacksPermissions`]
    /// instead of failing mid-upgrade with an opaque permission error.
    /// PostgreSQL only.
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

    /// Grant the RLS app role ([`APP_ROLE`]) its schema/table privileges, run
    /// during `upgrade()` once the tables exist (the role/schema/owner were
    /// created earlier by [`Self::provision_schema`]). Runs as the schema owner:
    /// it can grant on what it owns but cannot `CREATE ROLE`/`CREATE SCHEMA`.
    pub async fn ensure_app_role(&self) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            // Future tables created by this (owner) role auto-grant to the app
            // role, so a later migration that adds a table doesn't silently leave
            // td_app without access. `ALTER DEFAULT PRIVILEGES` applies only to
            // tables created after it runs, by the role running it.
            let stmts = grant_app_role_privileges_sql(self.schema(), &self.app_role);
            execute_batch_tx(&self.pool, &stmts).await?;
            debug!(role = self.app_role, "Ensured RLS app role");
        }
        Ok(())
    }

    /// The recorded `db_version`, or `None` when the schema does not yet exist.
    /// For `status` reporting. Delegates to [`crate::pool::state`] (a plain read).
    pub async fn db_version(&self) -> Result<Option<usize>, DbError> {
        crate::pool::state::db_version(&self.pool).await
    }

    /// Walk the delta ladder from the stored `db_version` up to the source's
    /// current version (or build fresh when absent); each delta applies in its own
    /// transaction. Refuses a database newer than the binary. Cross-major upgrade
    /// is not supported (see [`check_tabsdata_version`](Self::check_tabsdata_version)).
    pub async fn upgrade_db_version(&self, source: &dyn SchemaSource) -> Result<(), DbError> {
        // Shared, idempotent: every schema gets its own `tabsdata_system` ledger,
        // created by the pool (not the source's `build`), before any content — and
        // the pool, not the schema's SQL, writes `db_version` after build/delta.
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

    /// Create this schema's shared `tabsdata_system` ledger if absent — idempotent,
    /// run before any component content so the pool (not each schema's `build`) owns
    /// the ledger. Pinned `search_path` resolves it to this component's own copy.
    async fn ensure_system_table(&self) -> Result<(), DbError> {
        execute_batch_tx(&self.pool, &[tabsdata_system_ddl(self.kind().into())]).await
    }

    /// Write the `db_version` row — owned by the pool, not the schema's build/delta
    /// SQL. Upsert keyed on the ledger primary key so it works for both a fresh
    /// install (insert) and an upgrade (update).
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
