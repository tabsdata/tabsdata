//
// Copyright 2026 Tabsdata Inc.
//

//! [`AdminPool`] — the privileged admin/upgrader pool used by the `components` module.

use crate::components::ProvisionSpec;
use crate::components::sql::execute_batch_tx;
#[cfg(all(postgres, any(test, feature = "test-utils")))]
use crate::components::sql::grant_app_role_maintain_sql;
#[cfg(postgres)]
use crate::components::sql::{
    OWNER_PASSWORD_GUC, create_app_role_sql, create_owner_role_sql, create_schema_sql,
    drop_app_role_sql, execute_batch_tx_with_secrets, grant_app_role_membership_sql,
    grant_app_role_self_membership_sql,
};
use crate::config::{DatabaseConfig, Schema};
use crate::dynamic::DynamicPool;
use crate::error::DbError;
use crate::pool::connect::ConnectRole;
use crate::pool::state::db_version;
use crate::sea_query::builder::SeaQueryExt;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use sea_query::{Expr, Query};
use td_schema::schema::backend::Backend;
use tracing::{debug, info, instrument};

/// The privileged pool used to provision, upgrade and check schemas. On
/// PostgreSQL it keeps the DDL rights the serving `TdPool` gives up: the
/// superuser variant runs `CREATE SCHEMA`/`CREATE ROLE`; the per-component
/// variant connects as the schema owner. Short-lived around a management op.
#[derive(Debug, Clone)]
pub struct AdminPool {
    pub(super) pool: DynamicPool,
    // Stored (not passed) to stay in lock-step with the connection's
    // `search_path`. `None` on SQLite/MySQL.
    schema: Option<Schema>,
    // The RLS app role this pool provisions/grants: `td_app` in production, a
    // per-test role in tests.
    pub(super) app_role: String,
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

    /// Provision a component: the login-capable owner role, the schema it
    /// owns, the entry's RLS app role, and the owner's membership in it.
    /// Idempotent. Table grants come later ([`Self::ensure_app_role`]), once
    /// tables exist. The app role comes from the spec — the component entry's
    /// own — not from this (privileged, server-entry) pool's config.
    pub async fn provision_schema(&self, spec: &ProvisionSpec<'_>) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            let stmts = vec![
                create_owner_role_sql(spec.owner),
                create_schema_sql(spec.schema, spec.owner),
                create_app_role_sql(spec.app_role),
                grant_app_role_membership_sql(spec.owner, spec.app_role),
            ];
            execute_batch_tx_with_secrets(
                &self.pool,
                &[(OWNER_PASSWORD_GUC, spec.password)],
                &stmts,
            )
            .await?;
            debug!(
                owner = spec.owner,
                schema = spec.schema,
                role = spec.app_role,
                "Provisioned schema, owner and app role"
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

    /// Drop an instance-scoped app role at `db drop` (tolerant — see
    /// [`drop_app_role_sql`]). Runs after every component's schema is gone.
    pub async fn drop_app_role(&self, app_role: &str) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            execute_batch_tx(&self.pool, &[drop_app_role_sql(app_role)]).await?;
            debug!(role = app_role, "Dropped app role");
        }
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

    /// Create the RLS app role and acquire `SET` on it. Cluster-global, so it
    /// stays **outside** the upgrade transaction: holding a `pg_authid` lock
    /// for a whole upgrade would block every concurrent provision of the same
    /// role. Idempotent; the schema/table grants run inside the transaction
    /// ([`UpgradeTx::grant_app_role_privileges`]), once the tables exist.
    pub async fn ensure_app_role(&self) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            // Role + membership (no-ops when they hold), so `db upgrade`
            // self-heals a changed/dropped app role when the connected role
            // can, and otherwise fails at the actual cause, not the grant.
            let stmts = vec![
                create_app_role_sql(&self.app_role),
                grant_app_role_self_membership_sql(&self.app_role),
            ];
            execute_batch_tx(&self.pool, &stmts).await?;
            debug!(role = self.app_role, "Ensured RLS app role");
        }
        Ok(())
    }

    /// Grant the app role `MAINTAIN` on `schema`'s tables so its `ANALYZE`
    /// takes effect. Test-only — the test factories keep the schema
    /// superuser-owned, and the app role owns nothing. Runs after the tables
    /// exist, i.e. after [`Self::upgrade`].
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn grant_app_role_maintain(&self, schema: &str) -> Result<(), DbError> {
        if !matches!(self.kind().into(), Backend::Postgres) {
            return Ok(());
        }
        #[cfg(postgres)]
        {
            execute_batch_tx(
                &self.pool,
                &[grant_app_role_maintain_sql(schema, &self.app_role)],
            )
            .await?;
            debug!(role = self.app_role, schema, "Granted app role MAINTAIN");
        }
        Ok(())
    }

    /// The recorded `db_version`, or `None` when the schema does not exist.
    pub async fn db_version(&self) -> Result<Option<usize>, DbError> {
        db_version(&self.pool).await
    }

    /// Close the pool, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
