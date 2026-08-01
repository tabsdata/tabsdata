//
// Copyright 2026 Tabsdata Inc.
//

//! Turning a [`DatabaseConfig`] into live pools: the per-backend connect
//! machinery, the [`ConnectRole`] privilege selector, and the [`ConnectOutput`]
//! it produces. This is *behavior*, not configuration — it opens sqlx pools —
//! so it lives outside [`crate::config`], which holds only the declarative
//! types. Each backend maps its config onto sqlx's native connect options
//! setter by setter (no `sqlx::any` indirection). It never boots a server: the
//! managed one is brought up out of band by `td_dbmanager` (see
//! [`crate::server`]).

#[cfg(mysql)]
use crate::config::MySqlConnect;
#[cfg(postgres)]
use crate::config::PostgresConnect;
#[cfg(sqlite)]
use crate::config::SqliteConfig;
use crate::config::{ConnectConfig, DatabaseConfig, Schema, StatementLogConfig};
use crate::dynamic::DynamicPool;
use crate::error::DbError;
#[cfg(postgres)]
use sqlx::Connection;
use sqlx::pool::PoolOptions;
use sqlx::{ConnectOptions, Database};
#[cfg(mysql)]
use sqlx::{MySqlPool, mysql::MySqlConnectOptions};
#[cfg(postgres)]
use sqlx::{PgPool, postgres::PgConnectOptions};
#[cfg(sqlite)]
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
#[cfg(sqlite)]
use std::fs;
#[cfg(sqlite)]
use std::path::Path;
use tracing::debug;
#[cfg(sqlite)]
use tracing::warn;

/// Which role a [`DatabaseConfig::connect`] should open the pool as.
///
/// PostgreSQL is the only backend where the two differ: [`Self::Serving`]
/// switches every connection to the non-bypass [`APP_ROLE`](crate::APP_ROLE)
/// (`TdPool`, RLS-enforced), while [`Self::Admin`] keeps the raw role and its
/// DDL/provisioning privileges (`AdminPool`, used for create/upgrade/check).
/// SQLite and MySQL have no RLS, so both roles build the same pool.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ConnectRole {
    Serving,
    Admin,
}

/// Result of [`DatabaseConfig::connect`]. Bundles the sqlx pool with the
/// pinned schema. `connect` only ever connects to an already-running server —
/// it never boots one (embedded is brought up out of band), so there is no
/// server handle to carry.
pub struct ConnectOutput {
    pub pool: DynamicPool,
    /// Schema the connection's `search_path` was pinned to. `None` on
    /// SQLite/MySQL, which have no `search_path`; `Some` on PostgreSQL, where it
    /// is the mandatory configured schema.
    pub schema: Option<Schema>,
}

impl DatabaseConfig {
    /// Connect using this configuration, opening the pool as `role`.
    ///
    /// On PostgreSQL the role selects the privilege posture (see [`ConnectRole`]):
    /// [`ConnectRole::Admin`] is a single raw-role pool (DDL/provisioning);
    /// [`ConnectRole::Serving`] is the RLS-enforced app-role read/write pair
    /// and requires the role to exist — provisioning creates it, never a
    /// connect. SQLite/MySQL ignore the role (no RLS).
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

        match &self.conn {
            #[cfg(sqlite)]
            ConnectConfig::Sqlite(c) => {
                match &c.path {
                    Some(path) => try_create_missing_dir(path),
                    None if !c.in_memory => return Err(DbError::MissingSqlitePath),
                    None => {}
                }
                let conn_options = sqlite_connect_options(c);
                let (rw, ro) = (
                    // important to create rw first so create_if_missing kicks in
                    self.connect_sqlite(&conn_options, false).await?,
                    self.connect_sqlite(&conn_options, true).await?,
                );
                // No RLS on SQLite: admin and serving build the same pool.
                Ok(ConnectOutput {
                    pool: DynamicPool::Sqlite { ro, rw },
                    // SQLite has no `search_path`; schema is not consulted.
                    schema: None,
                })
            }
            #[cfg(postgres)]
            ConnectConfig::Postgres(c) => {
                let schema = self.connect_schema(role)?;
                let conn_options = pg_connect_options(c);
                let pool = self
                    .connect_postgres_pool(&conn_options, role, schema.as_deref())
                    .await?;
                Ok(ConnectOutput { pool, schema })
            }
            #[cfg(mysql)]
            ConnectConfig::MySql(c) => {
                let conn_options = mysql_connect_options(c);
                let (rw, ro) = tokio::try_join!(
                    self.connect_mysql(&conn_options, false),
                    self.connect_mysql(&conn_options, true),
                )?;
                // No RLS on MySQL: admin and serving build the same pool.
                Ok(ConnectOutput {
                    pool: DynamicPool::MySql { ro, rw },
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
    /// writes (DDL/provisioning rights intact). No session default GUC: an
    /// unscoped write into a project-scoped table fails closed.
    ///
    /// [`ConnectRole::Serving`] opens the RLS-enforced read/write pair whose
    /// `after_connect` switches to the app role; the role must already exist
    /// and be switchable — provisioning (`db create`) creates and grants it,
    /// never a connect.
    #[cfg(postgres)]
    async fn connect_postgres_pool(
        &self,
        conn_options: &PgConnectOptions,
        role: ConnectRole,
        schema: Option<&str>,
    ) -> Result<DynamicPool, DbError> {
        match role {
            ConnectRole::Admin => {
                let raw = self
                    .connect_postgres(conn_options, false, false, schema)
                    .await?;
                Ok(DynamicPool::Postgres {
                    ro: raw.clone(),
                    rw: raw,
                })
            }
            ConnectRole::Serving => {
                self.probe_app_role(conn_options, self.require_schema()?)
                    .await?;
                let (rw, ro) = tokio::try_join!(
                    self.connect_postgres(conn_options, false, true, schema),
                    self.connect_postgres(conn_options, true, true, schema),
                )?;
                Ok(DynamicPool::Postgres { ro, rw })
            }
        }
    }

    /// Prove the app role is switchable on one throwaway connection before
    /// building the serving pools. The pools cannot report this themselves: a
    /// failing `after_connect` is retried until the acquire deadline and
    /// surfaces only as `PoolTimedOut`, so an unprovisioned database would
    /// burn the whole timeout and then blame the pool. The probe fails fast
    /// with [`DbError::AppRoleNotSwitchable`] on `22023` (role does not exist —
    /// GUC validation, not `42704`) or `42501` (connecting role is not a
    /// member); both mean provisioning (`db create`) has not run against this
    /// database. Bounded by the pool's acquire timeout — a raw connect has
    /// none, and the probe must not hang where the pools would have timed out.
    #[cfg(postgres)]
    async fn probe_app_role(
        &self,
        conn_options: &PgConnectOptions,
        schema: &str,
    ) -> Result<(), DbError> {
        let switched = tokio::time::timeout(self.pool.acquire_timeout, async {
            let mut conn = conn_options.connect().await?;
            let switched = sqlx::query(&format!("SET ROLE \"{}\"", self.app_role()))
                .execute(&mut conn)
                .await;
            let _ = conn.close().await;
            switched
        })
        .await
        .unwrap_or(Err(sqlx::Error::PoolTimedOut));
        match switched {
            Ok(_) => Ok(()),
            Err(e) => match e.as_database_error().and_then(|db| db.code()) {
                Some(code) if code == "22023" || code == "42501" => Err(
                    DbError::AppRoleNotSwitchable(self.app_role().to_owned(), schema.to_owned(), e),
                ),
                _ => Err(DbError::FailedToConnectToDatabase(e)),
            },
        }
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
            .acquire_time_level(self.pool.acquire_time_level)
            .acquire_slow_level(self.pool.acquire_slow_level)
            .acquire_slow_threshold(self.pool.acquire_slow_threshold)
    }

    #[cfg(sqlite)]
    async fn connect_sqlite(
        &self,
        conn_options: &SqliteConnectOptions,
        read_only: bool,
    ) -> Result<SqlitePool, sqlx::Error> {
        // Only the read-write pool may create the file; the read-only pool
        // opening a missing database is an error, not a silent empty file.
        let conn_options = if read_only {
            conn_options
                .clone()
                .read_only(true)
                .create_if_missing(false)
        } else {
            conn_options.clone()
        };
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
    /// The role must already exist for `SET ROLE` to succeed — provisioning
    /// (`db create`) creates it before any serving pool opens, and
    /// [`Self::probe_app_role`] has already proven the switch works.
    #[cfg(postgres)]
    async fn connect_postgres(
        &self,
        conn_options: &PgConnectOptions,
        read_only: bool,
        app_role: bool,
        schema: Option<&str>,
    ) -> Result<PgPool, sqlx::Error> {
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
        // The role serving connections switch to (the global `td_app` by
        // default; per-instance on shared clusters, per-test on the shared
        // test server). Captured so the closure is 'static.
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

        let pool = pool_options.connect_with(conn_options.clone()).await?;
        Ok(pool)
    }

    #[cfg(mysql)]
    async fn connect_mysql(
        &self,
        conn_options: &MySqlConnectOptions,
        read_only: bool,
    ) -> Result<MySqlPool, sqlx::Error> {
        // Never log the full connect options — they carry the username and password.
        debug!(
            read_only,
            "Connecting mysql pool (connection options redacted)"
        );

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

        let pool = pool_options.connect_with(conn_options.clone()).await?;
        Ok(pool)
    }
}

/// Statement logging, identical on every backend's options (the only
/// `ConnectOptions`-trait knobs this module sets).
fn statement_logging<O: ConnectOptions>(options: O, log: &StatementLogConfig) -> O {
    options
        .log_statements(log.log_statements)
        .log_slow_statements(log.log_slow_statements, log.log_slow_statements_threshold)
}

/// Map the resolved postgres config onto sqlx's native options, field by
/// field. The constructor seeds fields from `PG*` env vars (and `new` would
/// additionally consult `~/.pgpass`), so the connection identity — host, port,
/// user, password, ssl mode, database — is always set from the config;
/// remaining unset fields keep the libpq-style env layering.
#[cfg(postgres)]
pub(crate) fn pg_connect_options(c: &PostgresConnect) -> PgConnectOptions {
    let o = &c.options;
    let mut options = PgConnectOptions::new_without_pgpass()
        .host(&o.host)
        .port(o.port)
        .username(&c.username)
        .password(&c.password)
        .ssl_mode(o.ssl_mode.into())
        .database(o.database.as_deref().unwrap_or(&c.username));
    if let Some(socket) = &o.socket {
        options = options.socket(socket);
    }
    if let Some(cert) = &o.ssl_root_cert {
        options = options.ssl_root_cert(cert);
    }
    if let Some(pem) = &o.ssl_root_cert_pem {
        options = options.ssl_root_cert_from_pem(pem.clone().into_bytes());
    }
    if let Some(cert) = &o.ssl_client_cert {
        options = options.ssl_client_cert(cert);
    }
    if let Some(pem) = &o.ssl_client_cert_pem {
        options = options.ssl_client_cert_from_pem(pem);
    }
    if let Some(key) = &o.ssl_client_key {
        options = options.ssl_client_key(key);
    }
    if let Some(pem) = &o.ssl_client_key_pem {
        options = options.ssl_client_key_from_pem(pem);
    }
    if let Some(capacity) = o.statement_cache_capacity {
        options = options.statement_cache_capacity(capacity);
    }
    if let Some(name) = &o.application_name {
        options = options.application_name(name);
    }
    if let Some(digits) = o.extra_float_digits {
        options = options.extra_float_digits(digits);
    }
    if !o.options.is_empty() {
        options = options.options(&o.options);
    }
    statement_logging(options, &o.log)
}

/// Map the sqlite config onto sqlx's native options, field by field.
/// `read_only` is decided per pool by the caller, never here.
#[cfg(sqlite)]
fn sqlite_connect_options(o: &SqliteConfig) -> SqliteConnectOptions {
    let mut options = SqliteConnectOptions::new()
        .in_memory(o.in_memory)
        .create_if_missing(o.create_if_missing)
        .foreign_keys(o.foreign_keys)
        .journal_mode(o.journal_mode.into())
        .busy_timeout(o.busy_timeout)
        .shared_cache(o.shared_cache)
        .immutable(o.immutable)
        .serialized(o.serialized)
        .optimize_on_close(o.optimize_on_close, o.analysis_limit);
    if let Some(path) = &o.path {
        options = options.filename(path);
    }
    if let Some(mode) = o.locking_mode {
        options = options.locking_mode(mode.into());
    }
    if let Some(mode) = o.synchronous {
        options = options.synchronous(mode.into());
    }
    if let Some(mode) = o.auto_vacuum {
        options = options.auto_vacuum(mode.into());
    }
    if let Some(size) = o.page_size {
        options = options.page_size(size);
    }
    if let Some(capacity) = o.statement_cache_capacity {
        options = options.statement_cache_capacity(capacity);
    }
    for pragma in &o.pragma {
        options = options.pragma(pragma.key.clone(), pragma.value.clone());
    }
    if let Some(size) = o.command_buffer_size {
        options = options.command_buffer_size(size);
    }
    if let Some(size) = o.row_buffer_size {
        options = options.row_buffer_size(size);
    }
    if let Some(vfs) = &o.vfs {
        options = options.vfs(vfs.clone());
    }
    for extension in &o.extensions {
        options = match &extension.entrypoint {
            Some(entrypoint) => {
                options.extension_with_entrypoint(extension.name.clone(), entrypoint.clone())
            }
            None => options.extension(extension.name.clone()),
        };
    }
    statement_logging(options, &o.log)
}

/// Map the resolved mysql config onto sqlx's native options, field by field.
#[cfg(mysql)]
fn mysql_connect_options(c: &MySqlConnect) -> MySqlConnectOptions {
    let o = &c.options;
    let mut options = MySqlConnectOptions::new()
        .host(&o.host)
        .port(o.port)
        .username(&c.username)
        .password(&c.password)
        .ssl_mode(o.ssl_mode.into())
        .pipes_as_concat(o.pipes_as_concat)
        .enable_cleartext_plugin(o.enable_cleartext_plugin)
        .no_engine_substitution(o.no_engine_substitution)
        .set_names(o.set_names);
    if let Some(socket) = &o.socket {
        options = options.socket(socket);
    }
    if let Some(database) = &o.database {
        options = options.database(database);
    }
    if let Some(ca) = &o.ssl_ca {
        options = options.ssl_ca(ca);
    }
    if let Some(pem) = &o.ssl_ca_pem {
        options = options.ssl_ca_from_pem(pem.clone().into_bytes());
    }
    if let Some(cert) = &o.ssl_client_cert {
        options = options.ssl_client_cert(cert);
    }
    if let Some(pem) = &o.ssl_client_cert_pem {
        options = options.ssl_client_cert_from_pem(pem);
    }
    if let Some(key) = &o.ssl_client_key {
        options = options.ssl_client_key(key);
    }
    if let Some(pem) = &o.ssl_client_key_pem {
        options = options.ssl_client_key_from_pem(pem);
    }
    if let Some(capacity) = o.statement_cache_capacity {
        options = options.statement_cache_capacity(capacity);
    }
    if let Some(charset) = &o.charset {
        options = options.charset(charset);
    }
    if let Some(collation) = &o.collation {
        options = options.collation(collation);
    }
    if let Some(timezone) = &o.timezone {
        options = options.timezone(timezone.clone());
    }
    statement_logging(options, &o.log)
}

#[cfg(sqlite)]
fn try_create_missing_dir(path: &Path) {
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        if let Err(err) = fs::create_dir_all(parent) {
            warn!("Failed to create database directory: {parent:?}. Error: {err}");
        } else {
            debug!("Created database directory: {parent:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(mysql)]
    use crate::config::MySqlConfig;
    use crate::config::PoolConfig;
    #[cfg(postgres)]
    use crate::config::PostgresConfig;
    #[cfg(postgres)]
    use crate::test_utils::server;
    #[cfg(postgres)]
    use sqlx::postgres::PgPoolOptions;
    #[cfg(postgres)]
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tracing::log::LevelFilter;

    #[cfg(postgres)]
    type TestBackend = sqlx::Postgres;
    #[cfg(all(sqlite, not(postgres)))]
    type TestBackend = sqlx::Sqlite;

    fn database_config(pool: PoolConfig) -> DatabaseConfig {
        #[cfg(postgres)]
        let conn = ConnectConfig::Postgres(PostgresConnect {
            options: PostgresConfig::default(),
            username: "user".to_string(),
            password: "pass".to_string(),
        });
        #[cfg(all(sqlite, not(postgres)))]
        let conn = ConnectConfig::Sqlite(SqliteConfig::default());
        DatabaseConfig {
            conn,
            schema: None,
            app_role: None,
            pool,
        }
    }

    #[cfg(postgres)]
    #[test]
    fn pg_connect_options_mapping() {
        let connect = PostgresConnect {
            options: PostgresConfig {
                host: "db.example.com".to_string(),
                port: 6432,
                database: Some("postgres".to_string()),
                ..Default::default()
            },
            username: "admin".to_string(),
            password: "secret".to_string(),
        };
        let options = pg_connect_options(&connect);
        assert_eq!(options.get_host(), "db.example.com");
        assert_eq!(options.get_port(), 6432);
        assert_eq!(options.get_database(), Some("postgres"));
        assert_eq!(options.get_username(), "admin");
    }

    /// With `database:` unset the pool targets the username's database — set
    /// explicitly so an ambient `PGDATABASE` cannot redirect it.
    #[cfg(postgres)]
    #[test]
    fn pg_connect_options_database_defaults_to_username() {
        let connect = PostgresConnect {
            options: PostgresConfig::default(),
            username: "admin".to_string(),
            password: "secret".to_string(),
        };
        let options = pg_connect_options(&connect);
        assert_eq!(options.get_database(), Some("admin"));
    }

    #[cfg(sqlite)]
    #[test]
    fn sqlite_connect_options_mapping() {
        let config = SqliteConfig {
            path: Some("/tmp/mapping.db".into()),
            ..Default::default()
        };
        let options = sqlite_connect_options(&config);
        assert_eq!(options.get_filename(), Path::new("/tmp/mapping.db"));
    }

    #[cfg(sqlite)]
    #[tokio::test]
    async fn sqlite_connect_requires_path_or_in_memory() {
        let config = DatabaseConfig {
            conn: ConnectConfig::Sqlite(SqliteConfig::default()),
            schema: None,
            app_role: None,
            pool: PoolConfig::default(),
        };
        let result = config.connect(ConnectRole::Serving).await;
        assert!(matches!(result, Err(DbError::MissingSqlitePath)));
    }

    #[cfg(mysql)]
    #[test]
    fn mysql_connect_options_mapping() {
        let connect = MySqlConnect {
            options: MySqlConfig {
                host: "db.example.com".to_string(),
                port: 3307,
                database: Some("td".to_string()),
                ..Default::default()
            },
            username: "admin".to_string(),
            password: "secret".to_string(),
        };
        let options = mysql_connect_options(&connect);
        assert_eq!(options.get_host(), "db.example.com");
        assert_eq!(options.get_port(), 3307);
        assert_eq!(options.get_database(), Some("td"));
        assert_eq!(options.get_username(), "admin");
    }

    #[test]
    fn pool_options_full_mapping_on_read_pool() {
        let pool = PoolConfig {
            min_connections: 3,
            max_connections: 17,
            acquire_timeout: Duration::from_secs(11),
            max_lifetime: Some(Duration::from_secs(1200)),
            idle_timeout: None,
            test_before_acquire: false,
            acquire_time_level: LevelFilter::Debug,
            acquire_slow_level: LevelFilter::Error,
            acquire_slow_threshold: Duration::from_secs(7),
        };
        let config = database_config(pool);
        let options: PoolOptions<TestBackend> = config.pool_options(true);
        assert_eq!(options.get_min_connections(), 3);
        assert_eq!(options.get_max_connections(), 17);
        assert_eq!(options.get_acquire_timeout(), Duration::from_secs(11));
        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(1200)));
        assert_eq!(options.get_idle_timeout(), None);
        assert!(!options.get_test_before_acquire());
        assert_eq!(options.get_acquire_slow_threshold(), Duration::from_secs(7));
    }

    /// The session settings sqlx exposes no getters for (`application_name`,
    /// the startup `options` GUCs, `extra_float_digits`) actually reach the
    /// server — read them back over a live connection. `extra_float_digits: 1`
    /// is deliberate: sqlx sends `2` when unset, so a dropped mapping fails.
    #[cfg(postgres)]
    #[tokio::test]
    async fn pg_connect_options_reach_the_server() {
        server::ensure_started().await;
        let base = server::admin_base();
        let connect = PostgresConnect {
            options: PostgresConfig {
                application_name: Some("td-connect-options-test".to_string()),
                extra_float_digits: Some(1),
                options: BTreeMap::from([(
                    "search_path".to_string(),
                    "information_schema".to_string(),
                )]),
                ..base.options
            },
            ..base
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pg_connect_options(&connect))
            .await
            .unwrap();
        let (application_name, search_path, extra_float_digits): (String, String, String) =
            sqlx::query_as(
                "SELECT current_setting('application_name'), \
                 current_setting('search_path'), \
                 current_setting('extra_float_digits')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(application_name, "td-connect-options-test");
        assert_eq!(search_path, "information_schema");
        assert_eq!(extra_float_digits, "1");
        pool.close().await;
    }

    /// A serving connect whose app role cannot be switched to (missing role —
    /// provisioning never ran) surfaces as [`DbError::AppRoleNotSwitchable`],
    /// not as an opaque pool timeout.
    #[cfg(postgres)]
    #[tokio::test]
    async fn serving_connect_surfaces_unswitchable_app_role() {
        server::ensure_started().await;
        let config = DatabaseConfig {
            conn: ConnectConfig::Postgres(server::admin_base()),
            schema: Some(Schema::new("public")),
            app_role: Some(format!("td_no_role_{}", td_common::id::id())),
            pool: PoolConfig {
                max_connections: 2,
                acquire_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        };
        let error = config
            .connect(ConnectRole::Serving)
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(
            matches!(error, DbError::AppRoleNotSwitchable(..)),
            "expected AppRoleNotSwitchable, got: {error:?}"
        );
    }

    /// The write pool is pinned to a single connection regardless of the
    /// configured sizes (in-process write serialization).
    #[test]
    fn pool_options_pin_write_pool() {
        let pool = PoolConfig {
            min_connections: 5,
            max_connections: 50,
            ..Default::default()
        };
        let config = database_config(pool);
        let options: PoolOptions<TestBackend> = config.pool_options(false);
        assert_eq!(options.get_min_connections(), 1);
        assert_eq!(options.get_max_connections(), 1);
    }
}
