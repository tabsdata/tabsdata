//
// Copyright 2026 Tabsdata Inc.
//

//! [`UpgradeTransaction`] — the single transaction a schema upgrade runs in, and the
//! [`AdminPool`] entry points that open, drive and commit it.

use crate::components::SchemaComponent;
use crate::components::admin_pool::AdminPool;
use crate::components::plan::{UpgradeAction, upgrade_action};
use crate::components::sql::grant_app_role_privileges_sql;
use crate::connection::PgConnectionExt;
use crate::error::DbError;
use crate::pool::state::{db_version, edition};
use crate::sea_query::builder::SeaQueryExt;
use sea_query::{Expr, ExprTrait, OnConflict, Query};
use sqlx::postgres::PgConnection;
use sqlx::{Postgres, Transaction};
use std::time::Instant;
use td_schema::SchemaSource;
use td_schema::schema::{derived_scripts, install_script};
use td_schema::{DB_EDITION_NAME, DB_VERSION_NAME, TabsdataSystem, tabsdata_system_ddl};
use te_system::edition::{Compatible, Edition, TabsdataEdition};
use tracing::{debug, info, warn};

impl AdminPool {
    /// Bring `component`'s schema to its current version as **one** transaction:
    /// the delta ladder, the derived-object drop and rebuild around it, the
    /// version and edition rows, and the app-role grants either all commit or
    /// all roll back. Read-only gates and the cluster-global app role run
    /// first, outside it.
    pub async fn upgrade(
        &self,
        component: &dyn SchemaComponent,
    ) -> Result<Option<UpgradeAction>, DbError> {
        component.preflight(self).await?;
        self.ensure_app_role().await?;
        let mut tx = self.begin_upgrade().await?;
        component.upgrade(&mut tx).await?;
        let action = tx.action.clone();
        tx.commit().await?;
        Ok(action)
    }

    /// Open the upgrade transaction. Public for components that compose their
    /// own step sequence; [`Self::upgrade`] is the normal entry.
    ///
    /// The transaction runs outside project scope: an upgrade is cross-tenant
    /// work, and it connects as the schema owner, whom `FORCE ROW LEVEL
    /// SECURITY` binds like anyone else. Without this a data delta over a
    /// project-scoped table would match no rows and still report success.
    pub async fn begin_upgrade(&self) -> Result<UpgradeTransaction<'_>, DbError> {
        let mut conn = self
            .pool
            .begin()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        conn.bypass_project_scope()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        Ok(UpgradeTransaction {
            admin: self,
            conn,
            action: None,
        })
    }

    /// [`Self::upgrade`] for a bare [`SchemaSource`] with no component around
    /// it — the archive-ladder and fixture-schema tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn upgrade_source(&self, source: &dyn SchemaSource) -> Result<(), DbError> {
        self.ensure_app_role().await?;
        let mut tx = self.begin_upgrade().await?;
        tx.upgrade_db_version(source).await?;
        tx.grant_app_role_privileges().await?;
        tx.commit().await
    }
}

/// The in-flight upgrade transaction. Every step a [`SchemaComponent`] runs
/// goes through here, on one connection, so an upgrade that fails half way
/// leaves the schema exactly as it was — version rows, seeds, and the derived
/// objects the ladder had to drop included.
pub struct UpgradeTransaction<'a> {
    admin: &'a AdminPool,
    conn: Transaction<'static, Postgres>,
    /// What [`Self::upgrade_db_version`] did, for the caller's report.
    action: Option<UpgradeAction>,
}

impl UpgradeTransaction<'_> {
    fn conn(&mut self) -> &mut PgConnection {
        &mut self.conn
    }

    /// Run `stmts` on the upgrade transaction.
    pub async fn execute_batch(&mut self, stmts: &[String]) -> Result<(), DbError> {
        self.conn()
            .execute_batch(stmts)
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)
    }

    /// Walk the delta ladder from the stored `db_version` up to `source`'s
    /// current version, or build fresh when absent. Refuses a database newer
    /// than the binary. Derived objects are dropped before the ladder (standing
    /// views block destructive deltas on PostgreSQL) and rebuilt after it, in
    /// this same transaction.
    pub async fn upgrade_db_version(&mut self, source: &dyn SchemaSource) -> Result<(), DbError> {
        // The pool, not the schema's SQL, owns the `tabsdata_system` ledger
        // and its `db_version` row.
        self.lock_schema_upgrades().await?;
        self.ensure_system_table().await?;

        let schema = self.admin.schema().to_string();
        let current = source.current_version();
        let action = upgrade_action(db_version(self.conn()).await?, current);
        self.action = Some(action.clone());
        let started = Instant::now();
        match &action {
            UpgradeAction::UpToDate { version } => {
                info!(schema, version, "Schema is up to date");
            }
            UpgradeAction::Ladder { from, to, targets } => {
                info!(schema, from, to, deltas = targets.len(), "Upgrading schema");
                let (drop, create) = derived_scripts(source);
                if !drop.is_empty() {
                    self.execute_batch(&drop).await?;
                    debug!(count = drop.len(), "Derived objects dropped");
                }
                for target in targets {
                    let stmts = source.delta(*target);
                    self.execute_batch(&stmts).await?;
                    info!(target, statements = stmts.len(), "Schema delta applied");
                }
                self.set_db_version(current).await?;
                if !create.is_empty() {
                    self.execute_batch(&create).await?;
                    debug!(count = create.len(), "Derived objects rebuilt");
                }
                info!(
                    schema,
                    from,
                    to,
                    elapsed_ms = started.elapsed().as_millis(),
                    "Schema upgraded"
                );
            }
            UpgradeAction::Newer { database, binary } => {
                warn!(
                    schema,
                    database_version = database,
                    binary_version = binary,
                    "Database is newer than binary"
                );
                return Err(DbError::DatabaseIsNewer(database.to_string(), *binary));
            }
            UpgradeAction::Install { version } => {
                let stmts = install_script(source);
                info!(
                    schema,
                    version,
                    statements = stmts.len(),
                    "No existing schema found, installing it"
                );
                self.execute_batch(&stmts).await?;
                self.set_db_version(current).await?;
                info!(
                    schema,
                    version,
                    elapsed_ms = started.elapsed().as_millis(),
                    "Schema installed"
                );
            }
        }
        Ok(())
    }

    /// Stamp the edition, or refuse a database this binary cannot serve.
    pub async fn upgrade_tabsdata_edition(&mut self) -> Result<(), DbError> {
        let instance_edition = edition(self.conn()).await?;
        let runtime_edition = TabsdataEdition;

        match instance_edition {
            None => {
                info!(edition = runtime_edition.label(), "Setting initial edition");
                let mut query = Query::insert()
                    .into_table(TabsdataSystem::Table)
                    .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                    .values([DB_EDITION_NAME.into(), runtime_edition.label().into()])
                    .map_err(|e| DbError::CannotUpgradeEdition(e.to_string()))?
                    .to_owned();
                query
                    .build_query()
                    .execute(self.conn())
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
                        .execute(self.conn())
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

    /// Grant the RLS app role its schema and table privileges, once the tables
    /// exist. `ALTER DEFAULT PRIVILEGES` auto-grants future tables, so a later
    /// migration cannot silently leave the app role without access. The role
    /// itself is created before the transaction ([`AdminPool::ensure_app_role`]).
    pub async fn grant_app_role_privileges(&mut self) -> Result<(), DbError> {
        let stmts = grant_app_role_privileges_sql(self.admin.schema(), &self.admin.app_role);
        self.execute_batch(&stmts).await?;
        debug!(
            role = self.admin.app_role,
            "Granted RLS app role privileges"
        );
        Ok(())
    }

    /// Create this schema's `tabsdata_system` ledger if absent — idempotent.
    async fn ensure_system_table(&mut self) -> Result<(), DbError> {
        let ddl = tabsdata_system_ddl();
        self.execute_batch(&[ddl]).await
    }

    /// Claim this schema's upgrade, or refuse: a peer mid-ladder is a conflict
    /// to report, not a wait to sit out. An advisory lock, not a lock on
    /// `tabsdata_system`: a fresh install has no ledger yet, and `CREATE TABLE
    /// IF NOT EXISTS` is not itself race-free.
    async fn lock_schema_upgrades(&mut self) -> Result<(), DbError> {
        let schema = self.admin.schema().to_string();
        let claimed = self
            .conn()
            .try_advisory_xact_lock_text(&format!("upgrade:schema:{schema}"))
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        if !claimed {
            return Err(DbError::ConcurrentUpgradeInProgress(schema));
        }
        Ok(())
    }

    /// Upsert the `db_version` row — works for fresh install and upgrade.
    async fn set_db_version(&mut self, version: usize) -> Result<(), DbError> {
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
            .execute(self.conn())
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)?;
        Ok(())
    }

    /// Commit the upgrade.
    pub async fn commit(self) -> Result<(), DbError> {
        self.conn
            .commit()
            .await
            .map_err(DbError::FailedToCreateOrUpgradeDatabaseSchema)
    }
}
