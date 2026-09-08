//
// Copyright 2026 Tabsdata Inc.
//

//! Turning a [`DatabaseConfig`] into live pools: the connect machinery, the
//! serving/admin connect entries, and the [`ConnectOutput`] it produces. This
//! is *behavior*, not configuration — it opens sqlx pools — so it lives
//! outside [`crate::config`], which holds only the declarative types. The
//! config maps onto sqlx's native connect options setter by setter (no
//! `sqlx::any` indirection). It never boots a server: the managed one is
//! brought up out of band by `td_dbmanager` (see [`crate::server`]).

use crate::config::{DatabaseConfig, PoolConfig, Schema};
use crate::connection::{ACROSS_PROJECTS_ROLE_GUC, IN_PROJECT_ROLE_GUC};
use crate::error::DbError;
use crate::pool::PoolKind;
use crate::pool::options::pg_connect_options;
use sqlx::pool::PoolOptions;
use sqlx::postgres::PgConnectOptions;
use sqlx::{AssertSqlSafe, ConnectOptions, Connection, PgPool, Postgres};
use std::time::Duration;
use tracing::debug;

/// Result of a connect. Bundles the sqlx pool with the pinned schema.
/// `connect` only ever connects to an already-running server — it never boots
/// one (embedded is brought up out of band), so there is no server handle to
/// carry.
pub struct ConnectOutput {
    pub(crate) pool: PgPool,
    /// The connected block's acquire timeout — also bounds how long an
    /// exclusive connection from the resulting pool queues on its project's
    /// lock.
    pub(crate) acquire_timeout: Duration,
    /// Schema the connection's `search_path` was pinned to. Mandatory on the
    /// serving path; `None` for the privileged system entry, which has no home
    /// schema.
    pub(crate) schema: Option<Schema>,
}

impl DatabaseConfig {
    /// Open `kind`'s serving pool: sized by its own
    /// `pool.{regular,exclusive}` block — required, nothing is inherited — and
    /// RLS-enforced under the [`APP_ROLE`](crate::APP_ROLE), which must
    /// already exist (provisioning creates it, never a connect).
    ///
    /// Both kinds serve writes, so neither pins its sessions read-only: an
    /// `RO` unit of work is `BEGIN READ ONLY`
    /// ([`TdPool::begin_ro`](crate::pool::TdPool::begin_ro)), which is what
    /// makes a stray read-path write fail loudly.
    pub(crate) async fn connect_serving(&self, kind: PoolKind) -> Result<ConnectOutput, DbError> {
        let pool = self.serving_pool_config(kind)?;
        // RLS scopes to the schema the objects live in, so a serving pool
        // always pins one.
        let schema = self.require_schema()?.clone();
        self.open(&pool, true, Some(schema)).await
    }

    /// `kind`'s block. Serving pools have no default — nothing is inherited,
    /// an absent block is a config error.
    fn serving_pool_config(&self, kind: PoolKind) -> Result<PoolConfig, DbError> {
        self.pool
            .get(kind)
            .clone()
            .ok_or(DbError::MissingPoolConfig(kind.as_str()))
    }

    /// Open the privileged connection: the raw role with its DDL/provisioning
    /// rights, for create/upgrade/check. A single connection unless the
    /// optional `pool.admin` block says otherwise. No session default GUC: an
    /// unscoped write into a project-scoped table fails closed. It provisions
    /// across schemas with explicit, schema-qualified DDL, so it may run
    /// without a home schema (the privileged system entry passes none).
    pub(crate) async fn connect_admin(&self) -> Result<ConnectOutput, DbError> {
        let pool = self.admin_pool_config();
        let schema = self.schema.clone();
        self.open(&pool, false, schema).await
    }

    /// The `admin` block, else the single privileged connection.
    fn admin_pool_config(&self) -> PoolConfig {
        self.pool.admin.clone().unwrap_or(PoolConfig {
            min_connections: 1,
            max_connections: 1,
            ..PoolConfig::default()
        })
    }

    /// The one connect engine: validate the block, then build the pool with
    /// this posture (`app_role` switch) and `search_path`.
    async fn open(
        &self,
        pool: &PoolConfig,
        app_role: bool,
        schema: Option<Schema>,
    ) -> Result<ConnectOutput, DbError> {
        if pool.max_connections == u32::MIN {
            Err(DbError::InvalidMaxConnections(pool.max_connections))?
        }
        if pool.min_connections > pool.max_connections {
            Err(DbError::InvalidConnections(
                pool.max_connections,
                pool.min_connections,
            ))?
        }

        let conn_options = pg_connect_options(&self.conn);
        if app_role {
            let roles = self.require_roles()?;
            let schema = self.require_schema()?;
            for role in [&roles.in_project, &roles.across_projects] {
                self.probe_role(&conn_options, pool.acquire_timeout, schema, role)
                    .await?;
            }
        }
        let db_pool = self
            .connect_postgres(&conn_options, pool, app_role, schema.as_deref())
            .await?;
        Ok(ConnectOutput {
            pool: db_pool,
            acquire_timeout: pool.acquire_timeout,
            schema,
        })
    }

    /// Prove an RLS role is switchable on one throwaway connection before
    /// building the serving pools. The pools cannot report this themselves: a
    /// failing `after_connect` is retried until the acquire deadline and
    /// surfaces only as `PoolTimedOut`, so an unprovisioned database would
    /// burn the whole timeout and then blame the pool. The probe fails fast
    /// with [`DbError::AppRoleNotSwitchable`] on `22023` (role does not exist —
    /// GUC validation, not `42704`) or `42501` (connecting role is not a
    /// member); both mean provisioning (`db create`) has not run against this
    /// database. Bounded by the pool's acquire timeout — a raw connect has
    /// none, and the probe must not hang where the pools would have timed out.
    async fn probe_role(
        &self,
        conn_options: &PgConnectOptions,
        acquire_timeout: Duration,
        schema: &str,
        role: &str,
    ) -> Result<(), DbError> {
        let switched = tokio::time::timeout(acquire_timeout, async {
            let mut conn = conn_options.connect().await?;
            let switched = sqlx::query(AssertSqlSafe(format!("SET ROLE \"{}\"", role)))
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
                    DbError::AppRoleNotSwitchable(role.to_owned(), schema.to_owned(), e),
                ),
                _ => Err(DbError::FailedToConnectToDatabase(e)),
            },
        }
    }

    /// Pool options from a resolved block.
    fn pool_options(pool: &PoolConfig) -> PoolOptions<Postgres> {
        PoolOptions::new()
            .max_connections(pool.max_connections)
            .min_connections(pool.min_connections)
            .acquire_timeout(pool.acquire_timeout)
            .max_lifetime(pool.max_lifetime)
            .idle_timeout(pool.idle_timeout)
            .test_before_acquire(pool.test_before_acquire)
            .acquire_time_level(pool.acquire_time_level)
            .acquire_slow_level(pool.acquire_slow_level)
            .acquire_slow_threshold(pool.acquire_slow_threshold)
    }

    /// Build one PostgreSQL pool.
    ///
    /// `app_role`: when true (the **serving** pool), every connection switches
    /// to the non-bypass [`APP_ROLE`](crate::APP_ROLE) in `after_connect`, so
    /// requests run under row-level security on a pool that authenticated as a
    /// superuser (embedded) or schema owner. When false (the **admin/upgrader**
    /// pool) the connection keeps its raw role and its DDL/provisioning rights.
    /// The role must already exist for `SET ROLE` to succeed — provisioning
    /// (`db create`) creates it before any serving pool opens, and
    /// [`Self::probe_role`] has already proven the switch works.
    ///
    /// No pool pins its sessions read-only: both serving kinds carry writes,
    /// and a read's `READ ONLY` posture comes from its own begin
    /// ([`TdPool::begin_ro`](crate::pool::TdPool::begin_ro)).
    async fn connect_postgres(
        &self,
        conn_options: &PgConnectOptions,
        pool: &PoolConfig,
        app_role: bool,
        schema: Option<&str>,
    ) -> Result<PgPool, sqlx::Error> {
        // Never log the full connect options — they carry the username and
        // password. Only the non-secret target.
        debug!(
            app_role,
            host = conn_options.get_host(),
            port = conn_options.get_port(),
            database = conn_options.get_database().unwrap_or_default(),
            "Connecting postgres pool (user/password redacted)"
        );

        let schema = schema.map(str::to_owned);
        // Captured so the closure is 'static.
        let roles = self.roles.clone();
        let pool_options = Self::pool_options(pool).after_connect(move |conn, _meta| {
            let schema = schema.clone();
            let roles = roles.clone();
            Box::pin(async move {
                // Pin the schema the app's objects live in, so DDL,
                // RLS-policy/view resolution, and queries all target it.
                // The admin/system pool has no home schema and skips this.
                if let Some(schema) = schema {
                    sqlx::query(AssertSqlSafe(format!("SET search_path TO \"{schema}\"")))
                        .execute(&mut *conn)
                        .await?;
                }
                // Also on the admin posture: an upgrade transaction scopes
                // itself across projects through the same seam.
                if let Some(roles) = &roles {
                    sqlx::query(AssertSqlSafe(format!(
                        "SET {IN_PROJECT_ROLE_GUC} = \"{}\"",
                        roles.in_project
                    )))
                    .execute(&mut *conn)
                    .await?;
                    sqlx::query(AssertSqlSafe(format!(
                        "SET {ACROSS_PROJECTS_ROLE_GUC} = \"{}\"",
                        roles.across_projects
                    )))
                    .execute(&mut *conn)
                    .await?;
                }
                if app_role {
                    let roles = roles.expect("a serving pool resolves its RLS roles");
                    // Session-level, so every transaction is RLS-enforced
                    // without a per-request switch. Test builds start across
                    // projects instead, so an unscoped read spans them.
                    #[cfg(not(feature = "td-test"))]
                    let session_role = &roles.in_project;
                    #[cfg(feature = "td-test")]
                    let session_role = &roles.across_projects;
                    sqlx::query(AssertSqlSafe(format!("SET ROLE \"{session_role}\"")))
                        .execute(&mut *conn)
                        .await?;

                    // Archived schemas carry the pre-v4 policy, which reads
                    // this instead of the role; the ladder tests install them.
                    #[cfg(feature = "td-test")]
                    sqlx::query("SET app.bypass_rls = on")
                        .execute(&mut *conn)
                        .await?;
                }
                Ok(())
            })
        });
        debug!("Database(app_role:{app_role}). Pool options: {pool_options:#?}");

        let pool = pool_options.connect_with(conn_options.clone()).await?;
        Ok(pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RlsRoles;
    use crate::config::{PoolConfig, PoolsConfig, PostgresConfig, PostgresConnect};
    use crate::connection::PgConnectionExt;
    use crate::test_utils::server;
    use std::time::Duration;
    use tracing::log::LevelFilter;

    fn database_config(pool: PoolsConfig) -> DatabaseConfig {
        DatabaseConfig {
            conn: PostgresConnect {
                options: PostgresConfig::default(),
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            schema: None,
            roles: None,
            pool,
        }
    }

    /// The read path's only guard, now that the regular pool carries writes
    /// too: `begin_ro` rejects them per transaction.
    #[tokio::test]
    async fn pg_begin_ro_rejects_writes() {
        let test_db = crate::test_utils::postgres::pool().await;
        let config = crate::test_utils::postgres::schema_connectable(
            &crate::test_utils::server::admin_base(),
            test_db.schema(),
        );
        let pool = crate::db(&config).await.unwrap();
        let mut conn = pool.begin_ro().await.unwrap();
        let err = conn
            .execute_batch(&["CREATE TABLE ro_trx_guard (x INT)".to_string()])
            .await
            .expect_err("write must fail on a read connection");
        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("25006")
        );
    }

    #[test]
    fn pool_options_full_mapping() {
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
        let options = DatabaseConfig::pool_options(&pool);
        assert_eq!(options.get_min_connections(), 3);
        assert_eq!(options.get_max_connections(), 17);
        assert_eq!(options.get_acquire_timeout(), Duration::from_secs(11));
        assert_eq!(options.get_max_lifetime(), Some(Duration::from_secs(1200)));
        assert_eq!(options.get_idle_timeout(), None);
        assert!(!options.get_test_before_acquire());
        assert_eq!(options.get_acquire_slow_threshold(), Duration::from_secs(7));
    }

    /// Every serving pool reads its own explicit block; nothing is inherited
    /// and an absent block is a configuration error.
    #[test]
    fn each_kind_selects_its_own_block() {
        let block = |max: u32| PoolConfig {
            min_connections: 1,
            max_connections: max,
            ..PoolConfig::default()
        };
        let config = database_config(PoolsConfig {
            regular: Some(block(50)),
            exclusive: Some(block(10)),
            admin: None,
        });
        let max = |kind| config.serving_pool_config(kind).unwrap().max_connections;
        assert_eq!(max(PoolKind::Regular), 50);
        assert_eq!(max(PoolKind::Exclusive), 10);

        let empty = database_config(PoolsConfig::default());
        for kind in [PoolKind::Regular, PoolKind::Exclusive] {
            let error = empty.serving_pool_config(kind).unwrap_err();
            assert!(
                matches!(error, DbError::MissingPoolConfig(_)),
                "expected MissingPoolConfig, got: {error:?}"
            );
        }
    }

    /// A serving connect whose app role cannot be switched to (missing role —
    /// provisioning never ran) surfaces as [`DbError::AppRoleNotSwitchable`],
    /// not as an opaque pool timeout.
    #[tokio::test]
    async fn serving_connect_surfaces_unswitchable_app_role() {
        server::ensure_started().await;
        let config = DatabaseConfig {
            conn: server::admin_base(),
            schema: Some(Schema::new("public")),
            roles: Some(RlsRoles {
                in_project: format!("td_no_role_{}", td_common::id::id()),
                across_projects: format!("td_no_role_{}", td_common::id::id()),
            }),
            pool: PoolsConfig {
                regular: Some(PoolConfig {
                    min_connections: 1,
                    max_connections: 2,
                    acquire_timeout: Duration::from_secs(3),
                    ..PoolConfig::default()
                }),
                ..Default::default()
            },
        };
        let error = config
            .connect_serving(PoolKind::Regular)
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(
            matches!(error, DbError::AppRoleNotSwitchable(..)),
            "expected AppRoleNotSwitchable, got: {error:?}"
        );
    }

    /// The admin connection defaults to a single raw-role connection when the
    /// entry carries no `admin` block, and honors one when it does.
    #[test]
    fn admin_block_defaults_and_overrides() {
        let config = database_config(PoolsConfig::default());
        let admin = config.admin_pool_config();
        assert_eq!(admin.min_connections, 1);
        assert_eq!(admin.max_connections, 1);

        let config = database_config(PoolsConfig {
            admin: Some(PoolConfig {
                min_connections: 1,
                max_connections: 2,
                acquire_timeout: Duration::from_secs(9),
                ..PoolConfig::default()
            }),
            ..Default::default()
        });
        let admin = config.admin_pool_config();
        assert_eq!(admin.max_connections, 2);
        assert_eq!(admin.acquire_timeout, Duration::from_secs(9));
    }
}
