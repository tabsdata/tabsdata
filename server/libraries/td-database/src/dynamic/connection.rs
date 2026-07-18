//
// Copyright 2026 Tabs Data Inc.
//

//! [`DynamicConnection`] — a backend-erased connection (or borrowed connection /
//! owned transaction, via [`ConnectionHolder`]) plus the per-request scope GUC
//! primitives (RLS project scope / bypass, advisory locks).

use super::DynamicTransaction;
#[cfg(mysql)]
use sqlx::MySql;
#[cfg(postgres)]
use sqlx::Postgres;
#[cfg(sqlite)]
use sqlx::Sqlite;
use sqlx::pool::PoolConnection;
use sqlx::{Connection, Error, Transaction};
use std::ops::DerefMut;
use tracing::debug;

/// A helper to store either the owned guard from the pool, a raw borrow,
/// or an owned transaction.
#[derive(Debug)]
pub enum ConnectionHolder<'c, DB: sqlx::Database, G> {
    Owned(G),
    Borrowed(&'c mut DB::Connection),
    OwnedTransaction(Transaction<'c, DB>),
}

// Implement Deref so we can access the underlying connection easily
impl<'c, DB, G> std::ops::Deref for ConnectionHolder<'c, DB, G>
where
    DB: sqlx::Database,
    G: std::ops::Deref<Target = DB::Connection>,
{
    type Target = DB::Connection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(g) => g.deref(),
            Self::Borrowed(r) => r,
            Self::OwnedTransaction(tx) => tx.deref(),
        }
    }
}

impl<'c, DB, G> DerefMut for ConnectionHolder<'c, DB, G>
where
    DB: sqlx::Database,
    G: DerefMut<Target = DB::Connection>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(g) => g.deref_mut(),
            Self::Borrowed(r) => r,
            Self::OwnedTransaction(tx) => tx.deref_mut(),
        }
    }
}

#[derive(Debug)]
pub enum DynamicConnection<'c> {
    #[cfg(sqlite)]
    Sqlite(ConnectionHolder<'c, Sqlite, PoolConnection<Sqlite>>),
    #[cfg(postgres)]
    Postgres(ConnectionHolder<'c, Postgres, PoolConnection<Postgres>>),
    #[cfg(mysql)]
    MySql(ConnectionHolder<'c, MySql, PoolConnection<MySql>>),
}

impl<'c> DynamicConnection<'c> {
    pub async fn ping(&mut self) -> Result<(), Error> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(h) => h.ping().await,
            #[cfg(postgres)]
            Self::Postgres(h) => h.ping().await,
            #[cfg(mysql)]
            Self::MySql(h) => h.ping().await,
        }
    }

    /// Execute a batch of SQL statements on this connection.
    pub async fn execute_batch(&mut self, stmts: &[String]) -> Result<(), Error> {
        let total = stmts.len();
        for (idx, stmt) in stmts.iter().enumerate() {
            debug!(
                stmt_index = idx + 1,
                stmt_total = total,
                sql = stmt.as_str(),
                "Executing batch statement"
            );
            match self {
                #[cfg(sqlite)]
                Self::Sqlite(h) => {
                    sqlx::query(stmt).execute(&mut **h).await?;
                }
                #[cfg(postgres)]
                Self::Postgres(h) => {
                    sqlx::query(stmt).execute(&mut **h).await?;
                }
                #[cfg(mysql)]
                Self::MySql(h) => {
                    sqlx::query(stmt).execute(&mut **h).await?;
                }
            };
        }
        Ok(())
    }

    /// Whether this connection is backed by PostgreSQL. Lets a service layer
    /// branch on backend without taking a `DatabaseBackend` dependency — e.g.
    /// to gate features that only make sense under RLS.
    pub fn is_postgres(&self) -> bool {
        match self {
            #[cfg(postgres)]
            Self::Postgres(_) => true,
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }

    /// Take a transaction-scoped PostgreSQL advisory lock keyed on `key`
    /// (`pg_advisory_xact_lock(hashtextextended(key, 0))`). The lock is held
    /// until the surrounding transaction commits or rolls back, so it serializes
    /// concurrent transactions that lock the same `key`. Used to make a
    /// check-then-insert sequence atomic against racing identical requests
    /// without escalating the isolation level. No-op on SQLite/MySQL (single
    /// writer; the table's unique index is the backstop there). **Must** run
    /// inside a transaction. `key` is hashed in-SQL so any string is safe; a
    /// hash collision only over-serializes two unrelated keys (harmless).
    #[cfg_attr(not(postgres), allow(unused_variables))]
    pub async fn advisory_xact_lock_text(&mut self, key: &str) -> Result<(), Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                    .bind(key)
                    .execute(&mut **h)
                    .await?;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
        Ok(())
    }

    /// Switch this connection's transaction to project-scope bypass:
    /// PostgreSQL `SET LOCAL app.bypass_rls = 'on'`, which the row-level
    /// security policies admit for cross-project (system / scheduler / admin)
    /// work. No-op on SQLite/MySQL. Like
    /// [`set_project_scope`](Self::set_project_scope), must run inside a
    /// transaction.
    pub async fn bypass_project_scope(&mut self) -> Result<(), Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                sqlx::query("SELECT set_config('app.bypass_rls', 'on', true)")
                    .execute(&mut **h)
                    .await?;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
        Ok(())
    }

    /// `EXPLAIN` a statement and return the plan lines. Test-only plumbing for
    /// plan-shape guards (e.g. asserting a query stays window-driven through
    /// the expected index). PostgreSQL only — other backends return no lines.
    #[cfg(feature = "td-test")]
    pub async fn explain(&mut self, sql: &str) -> Result<Vec<String>, Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                sqlx::query_scalar::<_, String>(&format!("EXPLAIN {sql}"))
                    .fetch_all(&mut **h)
                    .await
            }
            #[allow(unreachable_patterns)]
            _ => Ok(Vec::new()),
        }
    }

    /// Bind this connection's transaction to a project: set `app.project_id`
    /// **and clear `app.bypass_rls`**, so row-level security enforces exactly
    /// this project. The connection already runs as the non-bypass
    /// [`APP_ROLE`](crate::APP_ROLE) — switched once in the serving pool's
    /// `after_connect` — so this GUC is all a per-request scope needs.
    ///
    /// Clearing bypass matters when a connection is re-scoped from an earlier
    /// global/bypass scope within the same transaction (e.g. project creation,
    /// which inserts the `projects` registry row under bypass, then scopes to
    /// the new project for its system-group seeds): the lingering bypass would
    /// otherwise leave those writes un-isolated. A no-op when bypass was unset.
    ///
    /// Both `set_config`s go out as one simple-query batch (one round trip).
    /// `project_id` is escaped defensively. No-op on SQLite/MySQL (no RLS).
    /// **Must** run inside a transaction — the `LOCAL` GUC resets on
    /// commit/rollback, so on an autocommit connection it would not persist.
    #[cfg_attr(not(postgres), allow(unused_variables))]
    pub async fn set_project_scope(&mut self, project_id: &str) -> Result<(), Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                use sqlx::Executor;
                let id = project_id.replace('\'', "''");
                // Simple-query protocol (a `&str` query) sends both statements
                // in one round trip; the extended/prepared path `sqlx::query`
                // uses would reject a multi-statement string.
                let sql = format!(
                    "SELECT set_config('app.project_id', '{id}', true); \
                     SELECT set_config('app.bypass_rls', 'off', true)"
                );
                h.execute(sql.as_str()).await?;
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
        Ok(())
    }

    /// Try to extract the embedded transaction from this connection.
    /// Returns `Some(DynamicTransaction)` if this connection was created via
    /// `DynamicTransaction::into_connection()`, otherwise returns `None`.
    pub fn into_transaction(self) -> Option<DynamicTransaction<'c>> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(ConnectionHolder::OwnedTransaction(tx)) => {
                Some(DynamicTransaction::Sqlite(tx))
            }
            #[cfg(postgres)]
            Self::Postgres(ConnectionHolder::OwnedTransaction(tx)) => {
                Some(DynamicTransaction::Postgres(tx))
            }
            #[cfg(mysql)]
            Self::MySql(ConnectionHolder::OwnedTransaction(tx)) => {
                Some(DynamicTransaction::MySql(tx))
            }
            _ => None,
        }
    }
}
