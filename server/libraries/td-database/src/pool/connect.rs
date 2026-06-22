//
// Copyright 2026 Tabs Data Inc.
//

//! Turning a [`DatabaseConfig`] into live pools: the per-backend connect
//! machinery, the [`ConnectRole`] privilege selector, and the [`ConnectOutput`]
//! it produces. This is *behavior*, not configuration — it opens sqlx pools and
//! boots embedded postgres — so it lives outside [`crate::config`], which holds
//! only the declarative types.

use crate::config::{ConnectableBackend, DatabaseConfig, Schema};
use crate::dynamic::DynamicPool;
use crate::error::DbError;
use sqlx::any::AnyConnectOptions;
use sqlx::pool::PoolOptions;
use sqlx::{ConnectOptions, Database};
use std::time::Duration;
use tracing::debug;
use tracing::log::LevelFilter;
#[cfg(sqlite)]
use tracing::warn;
use url::Url;
#[cfg(sqlite)]
use std::fs;

const SLOW_QUERIES_THRESHOLD: u64 = 5000;

/// Which role a [`DatabaseConfig::connect`] should open the pool as.
///
/// PostgreSQL is the only backend where the two differ: [`Self::Serving`]
/// switches every connection to the non-bypass [`APP_ROLE`](crate::APP_ROLE)
/// (`TdPool`, RLS-enforced), while [`Self::Admin`] keeps the raw role and its
/// DDL/provisioning privileges (`SystemPool`, used for create/upgrade/check).
/// SQLite and MySQL have no RLS, so both roles build the same pool.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ConnectRole {
    Serving,
    Admin,
}

/// Result of [`DatabaseConfig::connect`]. Bundles the sqlx pool with the URL it
/// actually connected to and the pinned schema. `connect` only ever connects to a
/// URL — it never boots a server (embedded is brought up out of band and resolved
/// to its URL before connecting), so there is no server handle to carry.
pub struct ConnectOutput {
    pub pool: DynamicPool,
    pub url: Url,
    /// Schema the connection's `search_path` was pinned to. `None` on
    /// SQLite/MySQL, which have no `search_path`; `Some` on PostgreSQL, where it
    /// is the mandatory configured schema.
    pub schema: Option<Schema>,
}

impl DatabaseConfig {
    /// Connect using this configuration, opening the pool as `role`. For the
    /// embedded-postgres variant, boots the in-process server first and returns
    /// the live handle in `ConnectOutput.embedded`.
    ///
    /// On PostgreSQL the role selects the privilege posture (see [`ConnectRole`]):
    /// [`ConnectRole::Admin`] is a single raw-role pool (DDL/provisioning) and
    /// also provisions [`APP_ROLE`](crate::APP_ROLE); [`ConnectRole::Serving`] is
    /// the RLS-enforced `td_app` read/write pair and assumes the role already
    /// exists. SQLite/MySQL ignore the role (no RLS).
    pub(crate) async fn connect(&self, role: ConnectRole) -> Result<ConnectOutput, DbError> {
        if self.pool.max_connections == u32::MIN {
            Err(DbError::InvalidMaxConnections(self.pool.max_connections))?
        }
        if self.pool.min_connections > self.pool.max_connections {
            Err(DbError::InvalidConnections(
                self.pool.max_connections,
                self.pool.min_connections,
            ))?
        }

        self.validate()?;

        match &self.conn {
            #[cfg(sqlite)]
            ConnectableBackend::Sqlite(c) => {
                Self::try_create_missing_dir(&c.url);
                let conn_options = self.conn_options(&c.url)?;
                let (rw, ro) = (
                    // important to create rw first so create_if_missing kicks in
                    self.connect_sqlite(&conn_options, false).await?,
                    self.connect_sqlite(&conn_options, true).await?,
                );
                // No RLS on SQLite: admin and serving build the same pool.
                Ok(ConnectOutput {
                    pool: DynamicPool::Sqlite { ro, rw },
                    url: (*c.url).clone(),
                    // SQLite has no `search_path`; schema is not consulted.
                    schema: None,
                })
            }
            #[cfg(postgres)]
            ConnectableBackend::Postgres(c) => {
                let schema = self.connect_schema(role)?;
                let url = self.url_with_credentials(&c.url)?;
                let conn_options = self.conn_options(&url)?;
                let pool = self
                    .connect_postgres_pool(&conn_options, role, schema.as_deref())
                    .await?;
                Ok(ConnectOutput { pool, url, schema })
            }
            #[cfg(mysql)]
            ConnectableBackend::MySql(c) => {
                let url = self.url_with_credentials(&c.url)?;
                let conn_options = self.conn_options(&url)?;
                let (rw, ro) = tokio::try_join!(
                    self.connect_mysql(&conn_options, false),
                    self.connect_mysql(&conn_options, true),
                )?;
                // No RLS on MySQL: admin and serving build the same pool.
                Ok(ConnectOutput {
                    pool: DynamicPool::MySql { ro, rw },
                    url: (*c.url).clone(),
                    // MySQL has no `search_path`; schema is not consulted.
                    schema: None,
                })
            }
        }
    }

    /// The schema to pin a postgres connection's `search_path` to. The serving
    /// role requires one (RLS scopes to it); the admin role may omit it — it
    /// provisions across schemas with explicit, schema-qualified DDL, so it has
    /// no home schema (the privileged system pool passes `None`).
    #[cfg(postgres)]
    fn connect_schema(&self, role: ConnectRole) -> Result<Option<Schema>, DbError> {
        match role {
            ConnectRole::Serving => Ok(Some(self.require_schema()?.clone())),
            ConnectRole::Admin => Ok(self.schema.clone()),
        }
    }

    /// Build the PostgreSQL pool for `role` (see [`ConnectRole`]).
    ///
    /// [`ConnectRole::Admin`] opens a single raw-role connection for reads and
    /// writes (DDL/provisioning rights intact) and provisions
    /// [`APP_ROLE`](crate::APP_ROLE) — idempotent and privilege-free when the role
    /// already exists (external deployments pre-create it). No session default
    /// GUC: an unscoped write into a project-scoped table fails closed.
    ///
    /// [`ConnectRole::Serving`] opens the RLS-enforced read/write pair whose
    /// `after_connect` switches to `td_app`; the role must already exist (the
    /// admin path provisioned it beforehand).
    #[cfg(postgres)]
    async fn connect_postgres_pool(
        &self,
        conn_options: &AnyConnectOptions,
        role: ConnectRole,
        schema: Option<&str>,
    ) -> Result<DynamicPool, DbError> {
        match role {
            ConnectRole::Admin => {
                let raw = self
                    .connect_postgres(conn_options, false, false, schema)
                    .await?;
                provision_app_role(&raw, self.app_role()).await?;
                Ok(DynamicPool::Postgres {
                    ro: raw.clone(),
                    rw: raw,
                })
            }
            // The serving role never provisions: the admin path (embedded boot,
            // or `dbmanager` for an external database) created `td_app` beforehand.
            ConnectRole::Serving => {
                let (rw, ro) = tokio::try_join!(
                    self.connect_postgres(conn_options, false, true, schema),
                    self.connect_postgres(conn_options, true, true, schema),
                )?;
                Ok(DynamicPool::Postgres { ro, rw })
            }
        }
    }

    /// The connection URL with the configured credentials injected as userinfo,
    /// for handing to the sqlx connector. Assumes [`DatabaseConfig::validate`]
    /// already confirmed the credentials are present.
    #[cfg(any(postgres, mysql))]
    fn url_with_credentials(&self, url: &Url) -> Result<Url, DbError> {
        let mut url = url.clone();
        url.set_username(self.username.as_deref().unwrap_or_default())
            .and_then(|()| url.set_password(self.password.as_deref()))
            .map_err(|()| DbError::CredentialsInUrl)?;
        Ok(url)
    }

    fn conn_options(&self, url: &Url) -> Result<AnyConnectOptions, DbError> {
        let conn_options = AnyConnectOptions::from_url(url)
            .map_err(DbError::FailedToConnectToDatabase)?
            .log_statements(self.pool.log_level)
            .log_slow_statements(
                LevelFilter::Warn,
                Duration::from_millis(SLOW_QUERIES_THRESHOLD),
            );
        Ok(conn_options)
    }

    fn pool_options<DB: Database>(&self, read_only: bool) -> PoolOptions<DB> {
        // The write pool is pinned to a single connection, so all writes in this
        // process serialize through it (in-process check-then-insert can't race).
        // Cross-process (multiple replicas on one DB) is not serialized here — see
        // the `dedup_lock` advisory lock.
        PoolOptions::new()
            .min_connections(if read_only {
                self.pool.min_connections
            } else {
                1
            })
            .max_connections(if read_only {
                self.pool.max_connections
            } else {
                1
            })
            .acquire_timeout(self.pool.acquire_timeout)
            .max_lifetime(self.pool.max_lifetime)
            .idle_timeout(self.pool.idle_timeout)
            .test_before_acquire(self.pool.test_before_acquire)
    }

    #[cfg(sqlite)]
    fn try_create_missing_dir(url: &Url) {
        if let Ok(path) = url.to_file_path()
            && let Some(parent) = path.parent()
            && !parent.exists()
        {
            if let Err(err) = fs::create_dir_all(parent) {
                warn!("Failed to create database directory: {parent:?}. Error: {err}");
            } else {
                debug!("Created database directory: {parent:?}");
            }
        }
    }

    #[cfg(sqlite)]
    async fn connect_sqlite(
        &self,
        conn_options: &AnyConnectOptions,
        read_only: bool,
    ) -> Result<sqlx::SqlitePool, sqlx::Error> {
        let conn_options = sqlx::sqlite::SqliteConnectOptions::try_from(conn_options)?
            .create_if_missing(!read_only)
            .busy_timeout(Duration::from_secs(10))
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .read_only(read_only);
        debug!("Database(read_only:{read_only}). Connection options: {conn_options:#?}");

        let pool_options = self.pool_options(read_only);
        debug!("Database(read_only:{read_only}). Pool options: {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }

    /// Build one PostgreSQL pool.
    ///
    /// `app_role`: when true (the **serving** pools), every connection switches
    /// to the non-bypass [`APP_ROLE`](crate::APP_ROLE) in `after_connect`, so
    /// requests run under row-level security on a pool that authenticated as a
    /// superuser (embedded) or schema owner. When false (the **admin/upgrader**
    /// pool) the connection keeps its raw role and its DDL/provisioning rights.
    /// The role must already exist for `SET ROLE` to succeed — embedded
    /// provisions it before building the serving pools; external deployments
    /// pre-create it.
    #[cfg(postgres)]
    async fn connect_postgres(
        &self,
        conn_options: &AnyConnectOptions,
        read_only: bool,
        app_role: bool,
        schema: Option<&str>,
    ) -> Result<sqlx::PgPool, sqlx::Error> {
        let conn_options = sqlx::postgres::PgConnectOptions::try_from(conn_options)?;
        // Never log the full connect options — they carry the username and
        // password. Only the non-secret target.
        debug!(
            read_only,
            app_role,
            host = conn_options.get_host(),
            port = conn_options.get_port(),
            database = conn_options.get_database().unwrap_or_default(),
            "Connecting postgres pool (user/password redacted)"
        );

        let schema = schema.map(str::to_owned);
        // The role serving connections switch to (the global `td_app` by default;
        // a per-test role under test configs). Captured so the closure is 'static.
        let serving_role = self.app_role().to_owned();
        let pool_options = self
            .pool_options(read_only)
            .after_connect(move |conn, _meta| {
                let schema = schema.clone();
                let serving_role = serving_role.clone();
                Box::pin(async move {
                    // Pin the schema the app's objects live in (both pools), so
                    // DDL, RLS-policy/view resolution, and queries all target it.
                    // The admin/system pool has no home schema and skips this.
                    if let Some(schema) = schema {
                        sqlx::query(&format!("SET search_path TO \"{schema}\""))
                            .execute(&mut *conn)
                            .await?;
                    }
                    if app_role {
                        // Session-level: the connection runs as the app role for its
                        // whole pooled lifetime, so every transaction on it is
                        // RLS-enforced without a per-request role switch.
                        sqlx::query(&format!("SET ROLE \"{serving_role}\""))
                            .execute(&mut *conn)
                            .await?;

                        #[cfg(feature = "td-test")]
                        sqlx::query("SET app.bypass_rls = on")
                            .execute(&mut *conn)
                            .await?;
                    }
                    if read_only {
                        sqlx::query("SET default_transaction_read_only = on")
                            .execute(&mut *conn)
                            .await?;
                    }
                    Ok(())
                })
            });
        debug!(
            "Database(read_only:{read_only}, app_role:{app_role}). Pool options: {pool_options:#?}"
        );

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }

    #[cfg(mysql)]
    async fn connect_mysql(
        &self,
        conn_options: &AnyConnectOptions,
        read_only: bool,
    ) -> Result<sqlx::MySqlPool, sqlx::Error> {
        let conn_options = sqlx::mysql::MySqlConnectOptions::try_from(conn_options)?;
        // Never log the full connect options — they carry the username and password.
        debug!(read_only, "Connecting mysql pool (connection options redacted)");

        let pool_options = self
            .pool_options(read_only)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if read_only {
                        sqlx::query("SET SESSION TRANSACTION READ ONLY")
                            .execute(&mut *conn)
                            .await?;
                    }

                    Ok(())
                })
            });
        debug!("Database(read_only:{read_only}). Pool options: {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options).await?;
        Ok(pool)
    }
}

/// `url` with userinfo (user[:password]) stripped — for deriving a credential-free
/// base URL (credentials travel in explicit config fields, never the URL).
/// Infallible: stripping only fails on cannot-be-a-base URLs, which database URLs
/// are not. Only the test server's `admin_base` consumes it, so it is gated to the
/// same builds as `test_utils`.
#[cfg(all(postgres, feature = "embedded-postgres", any(test, feature = "test-utils")))]
pub(crate) fn redacted_url(url: &Url) -> Url {
    let mut url = url.clone();
    url.set_username("").ok();
    url.set_password(None).ok();
    url
}

#[cfg(postgres)]
fn create_app_role_sql(app_role: &str) -> String {
    // Race-safe idempotent create: a `IF NOT EXISTS` check followed by `CREATE ROLE`
    // is not atomic across concurrent sessions — two can both pass the check and one
    // then hits a duplicate-key error. Create unconditionally and swallow the
    // duplicate instead (the shared app role is provisioned by every admin connect,
    // so concurrent first-connects would otherwise collide).
    // Guard the create with an existence check so the common case (role already
    // present) is a lock-free read of `pg_roles` — important when every admin
    // connect provisions it under high concurrency. Only the first, racing creators
    // write `pg_authid`; the inner handler swallows the lost-race duplicate
    // (`unique_violation`) or an already-existing role (`duplicate_object`).
    format!(
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{app_role}') THEN \
             BEGIN CREATE ROLE \"{app_role}\" NOBYPASSRLS; \
             EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END; \
           END IF; \
         END $$"
    )
}

/// Create the row-level-security app role (`app_role`, the global
/// [`APP_ROLE`](crate::APP_ROLE) in production) if it does not exist, `NOBYPASSRLS`. Called on the admin pool during
/// connect — *before* the serving pools' `after_connect` `SET ROLE` runs — so the
/// switch can succeed. Idempotent, and privilege-free when the role already exists
/// (the `IF NOT EXISTS` guard means external deployments, which pre-create the role,
/// need no `CREATEROLE`). The schema/table grants are applied later, once the tables
/// exist, by the `system` module's `SystemPool::ensure_app_role`.
#[cfg(postgres)]
async fn provision_app_role(pool: &sqlx::PgPool, app_role: &str) -> Result<(), DbError> {
    sqlx::query(&create_app_role_sql(app_role))
        .execute(pool)
        .await
        .map_err(DbError::SqlError)?;
    debug!(role = app_role, "Provisioned RLS app role");
    Ok(())
}
