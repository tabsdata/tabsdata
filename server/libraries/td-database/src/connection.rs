//
// Copyright 2026 Tabsdata Inc.
//

//! [`PgConnectionExt`] — what a connection does besides run queries. Implemented on
//! [`PgConnection`], so it reaches a
//! [`PoolConnection`](sqlx::pool::PoolConnection) or a
//! [`DomainConnection`](crate::domain::DomainConnection) through deref.

use crate::sea_query::builder::SeaQueryExt;
use sea_query::{Expr, Func, Query};
use sqlx::postgres::PgConnection;
use sqlx::{AssertSqlSafe, Error, Executor};
use std::time::Duration;
use td_schema::{LEGACY_BYPASS_RLS_GUC, PROJECT_ID_GUC};
use tracing::debug;

const ADVISORY_XACT_LOCK: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";
const TRY_ADVISORY_XACT_LOCK: &str = "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))";

/// Set once per connection, so the per-transaction scope switch is a single
/// statement needing no configuration of its own.
pub(crate) const IN_PROJECT_ROLE_GUC: &str = "td.in_project_role";
pub(crate) const ACROSS_PROJECTS_ROLE_GUC: &str = "td.across_projects_role";

pub trait PgConnectionExt {
    fn execute_batch(&mut self, stmts: &[String])
    -> impl Future<Output = Result<(), Error>> + Send;

    /// Serializes concurrent transactions locking the same `key` until this one
    /// ends, waiting at most `wait`; `false` if it elapsed. `key` is hashed
    /// in-SQL, so any string is safe and a collision only over-serializes two
    /// unrelated keys. Floors at 1ms: `lock_timeout = 0` means no timeout in
    /// PostgreSQL, i.e. the opposite of "don't wait".
    ///
    /// Bounded is the only waiting form: an unbounded wait on a serving
    /// connection is a request that never answers.
    fn advisory_xact_lock_text_within(
        &mut self,
        key: &str,
        wait: Duration,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    /// Takes the same lock without waiting: `false` when another transaction
    /// holds it. Sets no `lock_timeout`, so the caller's later locks still wait
    /// normally.
    fn try_advisory_xact_lock_text(
        &mut self,
        key: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    /// Bound as a parameter, so the value reaches no SQL text — client batch
    /// logs, server statement logs and wire captures see only `set_config`.
    fn set_local_secret(
        &mut self,
        name: &str,
        value: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Cross-project scope for the rest of the transaction.
    fn scope_across_projects(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// No project for the rest of the transaction: the tenant role with
    /// `app.project_id` unset, so unpoliced relations read normally and a
    /// policed one yields nothing. Fail-closed by construction — the
    /// cross-project role is never taken for work that has no project.
    fn scope_global(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// The same, for pre-v4 policies: they read
    /// [`LEGACY_BYPASS_RLS_GUC`] instead of the `across_projects` role, and
    /// the archived schemas an upgrade ladder starts from carry them.
    fn admit_legacy_project_scope(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Scopes the rest of the transaction to one project. Switching the role
    /// matters when re-scoping from an earlier cross-project scope, where a
    /// lingering cross-project role would un-isolate the writes.
    fn scope_in_project(
        &mut self,
        project_id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    #[cfg(feature = "td-test")]
    fn explain(&mut self, sql: &str) -> impl Future<Output = Result<Vec<String>, Error>> + Send;

    /// Scoped to `current_schema()`: a bare `ANALYZE` sweeps the database, and
    /// tests share one isolated only by schema, so its `pg_class` pass dies
    /// with 42P01 whenever a concurrent test drops its schema mid-run.
    #[cfg(feature = "td-test")]
    fn analyze_current_schema(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Waits until another backend is blocked on a lock this transaction
    /// holds: the point at which a concurrent unit under test has reached
    /// the statement the interleaving is about. `false` if `within` elapsed.
    #[cfg(feature = "td-test")]
    fn wait_until_blocking(
        &mut self,
        within: Duration,
    ) -> impl Future<Output = Result<bool, Error>> + Send;
}

impl PgConnectionExt for PgConnection {
    async fn execute_batch(&mut self, stmts: &[String]) -> Result<(), Error> {
        let total = stmts.len();
        for (idx, stmt) in stmts.iter().enumerate() {
            debug!(
                stmt_index = idx + 1,
                stmt_total = total,
                sql = stmt.as_str(),
                "Executing batch statement"
            );
            sqlx::query(generated(stmt.clone()))
                .execute(&mut *self)
                .await?;
        }
        Ok(())
    }

    async fn try_advisory_xact_lock_text(&mut self, key: &str) -> Result<bool, Error> {
        Query::select()
            .expr(
                Func::cust("pg_try_advisory_xact_lock")
                    .arg(Func::cust("hashtextextended").args([Expr::val(key), Expr::val(0)])),
            )
            .build_query_scalar()
            .fetch_one(self)
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if lock_wait_elapsed(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn try_advisory_lock_text(&mut self, key: &str) -> Result<bool, Error> {
        Query::select()
            .expr(
                Func::cust("pg_try_advisory_lock")
                    .arg(Func::cust("hashtextextended").args([Expr::val(key), Expr::val(0)])),
            )
            .build_query_scalar()
            .fetch_one(self)
            .await
    }

    async fn set_local_secret(&mut self, name: &str, value: &str) -> Result<(), Error> {
        Query::select()
            .expr(Func::cust("set_config").args([
                Expr::val(name),
                Expr::val(value),
                Expr::val(true),
            ]))
            .build_query()
            .execute(self)
            .await?;
        Ok(())
    }

    async fn scope_across_projects(&mut self) -> Result<(), Error> {
        sqlx::query("SELECT set_config('role', current_setting($1), true)")
            .bind(ACROSS_PROJECTS_ROLE_GUC)
            .execute(&mut *self)
            .await?;
        Ok(())
    }

    async fn scope_global(&mut self) -> Result<(), Error> {
        sqlx::query("SELECT set_config('role', current_setting($1), true)")
            .bind(IN_PROJECT_ROLE_GUC)
            .execute(&mut *self)
            .await?;
        Ok(())
    }

    async fn admit_legacy_project_scope(&mut self) -> Result<(), Error> {
        sqlx::query("SELECT set_config($1, 'on', true)")
            .bind(LEGACY_BYPASS_RLS_GUC)
            .execute(&mut *self)
            .await?;
        Ok(())
    }

    async fn scope_in_project(&mut self, project_id: &str) -> Result<(), Error> {
        sqlx::query(
            "SELECT set_config('role', current_setting($1), true), set_config($2, $3, true)",
        )
        .bind(IN_PROJECT_ROLE_GUC)
        .bind(PROJECT_ID_GUC)
        .bind(project_id)
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    #[cfg(feature = "td-test")]
    async fn explain(&mut self, sql: &str) -> Result<Vec<String>, Error> {
        sqlx::query_scalar::<_, String>(generated(format!("EXPLAIN {sql}")))
            .fetch_all(&mut *self)
            .await
    }

    #[cfg(feature = "td-test")]
    async fn analyze_current_schema(&mut self) -> Result<(), Error> {
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT format('%I.%I', schemaname, tablename) FROM pg_tables \
             WHERE schemaname = current_schema()",
        )
        .fetch_all(&mut *self)
        .await?;
        if !tables.is_empty() {
            sqlx::query(generated(format!("ANALYZE {}", tables.join(", "))))
                .execute(&mut *self)
                .await?;
        }
        Ok(())
    }
}

    #[cfg(feature = "td-test")]
    async fn wait_until_blocking(&mut self, within: Duration) -> Result<bool, Error> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            sqlx::query("SELECT pg_stat_clear_snapshot()")
                .execute(&mut *self)
                .await?;
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE pg_backend_pid() = ANY(pg_blocking_pids(pid))",
            )
            .fetch_one(&mut *self)
            .await?;
            if blocked > 0 {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
