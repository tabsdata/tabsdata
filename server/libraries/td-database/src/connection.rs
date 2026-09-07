//
// Copyright 2026 Tabsdata Inc.
//

//! [`PgConnectionExt`] — what a connection does besides run queries. Implemented on
//! [`PgConnection`], so it reaches a
//! [`PoolConnection`](sqlx::pool::PoolConnection) or a
//! [`DomainConnection`](crate::domain::DomainConnection) through deref.

use sqlx::postgres::PgConnection;
use sqlx::{Error, Executor};
use std::time::Duration;
use td_schema::{BYPASS_RLS_GUC, PROJECT_ID_GUC};
use tracing::debug;

const ADVISORY_XACT_LOCK: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";
const TRY_ADVISORY_XACT_LOCK: &str = "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))";

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

    /// Cross-project scope, which the RLS policies admit for system work.
    fn bypass_project_scope(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Sets `app.project_id` **and clears `app.bypass_rls`**. Clearing matters
    /// when re-scoping from an earlier bypass — project creation registers the
    /// project under bypass, then scopes to it to seed — where a lingering
    /// bypass would leave those writes un-isolated.
    fn set_project_scope(
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
            sqlx::query(stmt).execute(&mut *self).await?;
        }
        Ok(())
    }

    async fn advisory_xact_lock_text_within(
        &mut self,
        key: &str,
        wait: Duration,
    ) -> Result<bool, Error> {
        let millis = wait.as_millis().max(1);
        sqlx::query(&format!("SET LOCAL lock_timeout = {millis}"))
            .execute(&mut *self)
            .await?;
        match sqlx::query(ADVISORY_XACT_LOCK)
            .bind(key)
            .execute(&mut *self)
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if lock_wait_elapsed(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn try_advisory_xact_lock_text(&mut self, key: &str) -> Result<bool, Error> {
        sqlx::query_scalar(TRY_ADVISORY_XACT_LOCK)
            .bind(key)
            .fetch_one(&mut *self)
            .await
    }

    async fn set_local_secret(&mut self, name: &str, value: &str) -> Result<(), Error> {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(name)
            .bind(value)
            .execute(&mut *self)
            .await?;
        Ok(())
    }

    async fn bypass_project_scope(&mut self) -> Result<(), Error> {
        sqlx::query(&format!(
            "SELECT set_config('{BYPASS_RLS_GUC}', 'on', true)"
        ))
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn set_project_scope(&mut self, project_id: &str) -> Result<(), Error> {
        let id = project_id.replace('\'', "''");
        // Simple-query protocol: one round trip for both, where the prepared
        // path `sqlx::query` uses would reject a multi-statement string.
        let sql = format!(
            "SELECT set_config('{PROJECT_ID_GUC}', '{id}', true); \
             SELECT set_config('{BYPASS_RLS_GUC}', 'off', true)"
        );
        self.execute(sql.as_str()).await?;
        Ok(())
    }

    #[cfg(feature = "td-test")]
    async fn explain(&mut self, sql: &str) -> Result<Vec<String>, Error> {
        sqlx::query_scalar::<_, String>(&format!("EXPLAIN {sql}"))
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
            sqlx::query(&format!("ANALYZE {}", tables.join(", ")))
                .execute(&mut *self)
                .await?;
        }
        Ok(())
    }
}

/// PostgreSQL `lock_not_available`.
fn lock_wait_elapsed(e: &Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code == "55P03")
}
