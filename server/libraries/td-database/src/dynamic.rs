//
// Copyright 2026 Tabs Data Inc.
//

use crate::sea_query::kind::DatabaseBackend;
use sqlx::pool::PoolConnection;
use sqlx::{Connection, Decode, Error, FromRow, Transaction, Type};
#[cfg(mysql)]
use sqlx::{
    MySql, MySqlPool,
    mysql::{MySqlQueryResult, MySqlRow},
};
#[cfg(postgres)]
use sqlx::{
    PgPool, Postgres,
    postgres::{PgQueryResult, PgRow},
};
#[cfg(sqlite)]
use sqlx::{
    Sqlite, SqlitePool,
    sqlite::{SqliteQueryResult, SqliteRow},
};
use std::ops::DerefMut;
use tracing::debug;

// Pool

#[derive(Debug, Clone)]
pub enum DynamicPool {
    #[cfg(sqlite)]
    Sqlite { ro: SqlitePool, rw: SqlitePool },
    #[cfg(postgres)]
    Postgres { ro: PgPool, rw: PgPool },
    #[cfg(mysql)]
    MySql { ro: MySqlPool, rw: MySqlPool },
}

impl AsRef<DynamicPool> for DynamicPool {
    fn as_ref(&self) -> &DynamicPool {
        self
    }
}

impl DynamicPool {
    /// Acquire a connection from the read-only pool
    pub async fn acquire<'c>(&self) -> Result<DynamicConnection<'c>, Error> {
        match self {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, .. } => {
                let conn = ro.acquire().await?;
                Ok(DynamicConnection::Sqlite(ConnectionHolder::Owned(conn)))
            }
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, .. } => {
                let conn = ro.acquire().await?;
                Ok(DynamicConnection::Postgres(ConnectionHolder::Owned(conn)))
            }
            #[cfg(mysql)]
            DynamicPool::MySql { ro, .. } => {
                let conn = ro.acquire().await?;
                Ok(DynamicConnection::MySql(ConnectionHolder::Owned(conn)))
            }
        }
    }

    /// Begin a transaction on the read-only pool. Used by the read path so a
    /// per-request `SET LOCAL` (role / RLS GUC) has a transaction to scope to;
    /// the caller rolls it back (reads make no changes to commit).
    pub async fn begin_ro<'c>(&self) -> Result<DynamicTransaction<'c>, Error> {
        match self {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, .. } => Ok(DynamicTransaction::Sqlite(ro.begin().await?)),
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, .. } => Ok(DynamicTransaction::Postgres(ro.begin().await?)),
            #[cfg(mysql)]
            DynamicPool::MySql { ro, .. } => Ok(DynamicTransaction::MySql(ro.begin().await?)),
        }
    }

    /// Acquire a connection from the read-only pool in the form the per-request
    /// read path needs. On PostgreSQL this is a read-only transaction (so a
    /// per-request `SET LOCAL` for RLS has a transaction to scope to); it makes
    /// no changes and auto-rolls-back when the returned [`DynamicConnection`]
    /// drops. On SQLite/MySQL — which have no RLS — it is the bare pooled
    /// connection, preserving the original autocommit read semantics.
    ///
    /// Keeps backend dispatch inside this crate so the tower providers stay
    /// backend-agnostic.
    pub async fn acquire_for_read<'c>(&self) -> Result<DynamicConnection<'c>, Error> {
        match self {
            #[cfg(postgres)]
            DynamicPool::Postgres { .. } => Ok(self.begin_ro().await?.into_connection()),
            #[allow(unreachable_patterns)]
            _ => self.acquire().await,
        }
    }

    /// Begin a transaction on the read-write pool
    pub async fn begin<'c>(&self) -> Result<DynamicTransaction<'c>, Error> {
        match self {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { rw, .. } => {
                let tx = rw.begin().await?;
                Ok(DynamicTransaction::Sqlite(tx))
            }
            #[cfg(postgres)]
            DynamicPool::Postgres { rw, .. } => {
                let tx = rw.begin().await?;
                Ok(DynamicTransaction::Postgres(tx))
            }
            #[cfg(mysql)]
            DynamicPool::MySql { rw, .. } => {
                let tx = rw.begin().await?;
                Ok(DynamicTransaction::MySql(tx))
            }
        }
    }

    /// Returns if the pool is closed.
    pub fn is_closed(&self) -> bool {
        match self {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, rw } => ro.is_closed() && rw.is_closed(),
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, rw } => ro.is_closed() && rw.is_closed(),
            #[cfg(mysql)]
            DynamicPool::MySql { ro, rw } => ro.is_closed() && rw.is_closed(),
        }
    }

    /// Close both pools, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        match self {
            #[cfg(sqlite)]
            DynamicPool::Sqlite { ro, rw } => {
                ro.close().await;
                rw.close().await;
            }
            #[cfg(postgres)]
            DynamicPool::Postgres { ro, rw } => {
                ro.close().await;
                rw.close().await;
            }
            #[cfg(mysql)]
            DynamicPool::MySql { ro, rw } => {
                ro.close().await;
                rw.close().await;
            }
        }
    }
}

// Connection

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

/// Quote a trusted SQL identifier for interpolation (doubles embedded `"`).
/// Single source of the `SET LOCAL ROLE` quoting rule shared by
/// [`DynamicConnection::set_rls_role`] and
/// [`DynamicConnection::enter_project_scope`].
#[cfg(postgres)]
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
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

    pub fn as_borrowed(&mut self) -> DynamicConnection<'_> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(h) => DynamicConnection::Sqlite(ConnectionHolder::Borrowed(h.deref_mut())),
            #[cfg(postgres)]
            Self::Postgres(h) => {
                DynamicConnection::Postgres(ConnectionHolder::Borrowed(h.deref_mut()))
            }
            #[cfg(mysql)]
            Self::MySql(h) => DynamicConnection::MySql(ConnectionHolder::Borrowed(h.deref_mut())),
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

    /// Switch this connection's transaction to `role` (PostgreSQL
    /// `SET LOCAL ROLE`). Row-level security is enforced against the *current*
    /// role, so switching to a non-superuser, non-`BYPASSRLS` role makes RLS
    /// apply even when the pool authenticated as a superuser — letting the
    /// bootstrap/superuser connection still serve RLS-scoped requests. No-op on
    /// SQLite/MySQL. Must run inside a transaction (the `LOCAL` scope resets on
    /// commit/rollback). `role` is a trusted identifier, quoted defensively.
    #[cfg_attr(not(postgres), allow(unused_variables))]
    pub async fn set_rls_role(&mut self, role: &str) -> Result<(), Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                let sql = format!("SET LOCAL ROLE {}", quote_ident(role));
                sqlx::query(&sql).execute(&mut **h).await?;
            }
            #[allow(unreachable_patterns)]
            _ => {}
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

    /// Bind this connection's transaction to a project: PostgreSQL
    /// `SET LOCAL app.project_id = <project_id>`. Under row-level security
    /// rows are then visible/writable only for that project (see the
    /// `project_isolation` policy). No-op on SQLite/MySQL, which have no RLS.
    ///
    /// Uses `set_config(..., is_local => true)`, so the binding is scoped to the
    /// surrounding transaction and cleared on commit/rollback — safe on a pooled
    /// connection. **Must** be called inside a transaction; on an autocommit
    /// connection the `true` (local) setting would not persist to later queries.
    #[cfg_attr(not(postgres), allow(unused_variables))]
    pub async fn set_project_scope(&mut self, project_id: &str) -> Result<(), Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                sqlx::query("SELECT set_config('app.project_id', $1, true)")
                    .bind(project_id)
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

    /// Enter the per-request project scope in a single round trip: switch this
    /// connection's transaction to `role` (`SET LOCAL ROLE`) and set the scope
    /// GUC — `app.project_id = <project_id>` when `project_id` is `Some`, or
    /// `app.bypass_rls = on` when `None` (cross-project system / scheduler /
    /// admin work).
    ///
    /// Combines what would otherwise be two separate statements (and two
    /// network round trips) into one simple-query batch. `role` and
    /// `project_id` are trusted identifiers, escaped defensively. No-op on
    /// SQLite/MySQL (no RLS). **Must** run inside a transaction — the `LOCAL`
    /// scope resets on commit/rollback, so on an autocommit connection the
    /// settings would not persist to later queries.
    #[cfg_attr(not(postgres), allow(unused_variables))]
    pub async fn enter_project_scope(
        &mut self,
        role: &str,
        project_id: Option<&str>,
    ) -> Result<(), Error> {
        match self {
            #[cfg(postgres)]
            Self::Postgres(h) => {
                let guc = match project_id {
                    Some(id) => {
                        let id = id.replace('\'', "''");
                        format!("SELECT set_config('app.project_id', '{id}', true)")
                    }
                    None => "SELECT set_config('app.bypass_rls', 'on', true)".to_string(),
                };
                // Simple-query protocol (a `&str` query) sends both statements
                // in one round trip; the extended/prepared path `sqlx::query`
                // uses would reject a multi-statement string.
                use sqlx::Executor;
                let sql = format!("SET LOCAL ROLE {}; {guc}", quote_ident(role));
                (&mut **h).execute(sql.as_str()).await?;
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

// Transaction

#[derive(Debug)]
pub enum DynamicTransaction<'c> {
    #[cfg(sqlite)]
    Sqlite(Transaction<'c, Sqlite>),
    #[cfg(postgres)]
    Postgres(Transaction<'c, Postgres>),
    #[cfg(mysql)]
    MySql(Transaction<'c, MySql>),
}

impl<'c> DynamicTransaction<'c> {
    /// Commit the transaction
    pub async fn commit(self) -> Result<(), Error> {
        match self {
            #[cfg(sqlite)]
            DynamicTransaction::Sqlite(tx) => tx.commit().await,
            #[cfg(postgres)]
            DynamicTransaction::Postgres(tx) => tx.commit().await,
            #[cfg(mysql)]
            DynamicTransaction::MySql(tx) => tx.commit().await,
        }
    }

    /// Rollback the transaction
    pub async fn rollback(self) -> Result<(), Error> {
        match self {
            #[cfg(sqlite)]
            DynamicTransaction::Sqlite(tx) => tx.rollback().await,
            #[cfg(postgres)]
            DynamicTransaction::Postgres(tx) => tx.rollback().await,
            #[cfg(mysql)]
            DynamicTransaction::MySql(tx) => tx.rollback().await,
        }
    }

    pub fn as_mut_conn(&mut self) -> DynamicConnection<'_> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(tx) => DynamicConnection::Sqlite(ConnectionHolder::Borrowed(tx.as_mut())),
            #[cfg(postgres)]
            Self::Postgres(tx) => {
                DynamicConnection::Postgres(ConnectionHolder::Borrowed(tx.as_mut()))
            }
            #[cfg(mysql)]
            Self::MySql(tx) => DynamicConnection::MySql(ConnectionHolder::Borrowed(tx.as_mut())),
        }
    }

    /// Convert this transaction into a `DynamicConnection` by embedding the
    /// transaction inside the `OwnedTransaction` variant of `ConnectionHolder`.
    pub fn into_connection(self) -> DynamicConnection<'c> {
        match self {
            #[cfg(sqlite)]
            Self::Sqlite(tx) => DynamicConnection::Sqlite(ConnectionHolder::OwnedTransaction(tx)),
            #[cfg(postgres)]
            Self::Postgres(tx) => {
                DynamicConnection::Postgres(ConnectionHolder::OwnedTransaction(tx))
            }
            #[cfg(mysql)]
            Self::MySql(tx) => DynamicConnection::MySql(ConnectionHolder::OwnedTransaction(tx)),
        }
    }
}

// Query result

#[derive(Debug)]
pub enum DynamicQueryResult {
    #[cfg(sqlite)]
    Sqlite(SqliteQueryResult),
    #[cfg(postgres)]
    Postgres(PgQueryResult),
    #[cfg(mysql)]
    MySql(MySqlQueryResult),
}

#[cfg(sqlite)]
impl From<SqliteQueryResult> for DynamicQueryResult {
    fn from(value: SqliteQueryResult) -> Self {
        DynamicQueryResult::Sqlite(value)
    }
}

#[cfg(postgres)]
impl From<PgQueryResult> for DynamicQueryResult {
    fn from(value: PgQueryResult) -> Self {
        DynamicQueryResult::Postgres(value)
    }
}

#[cfg(mysql)]
impl From<MySqlQueryResult> for DynamicQueryResult {
    fn from(value: MySqlQueryResult) -> Self {
        DynamicQueryResult::MySql(value)
    }
}

impl DynamicQueryResult {
    /// Create a zero-rows-affected result for the given database backend.
    pub fn empty(backend: &DatabaseBackend) -> Self {
        match backend {
            #[cfg(sqlite)]
            DatabaseBackend::Sqlite => DynamicQueryResult::Sqlite(SqliteQueryResult::default()),
            #[cfg(postgres)]
            DatabaseBackend::Postgres => DynamicQueryResult::Postgres(PgQueryResult::default()),
            #[cfg(mysql)]
            DatabaseBackend::MySql => DynamicQueryResult::MySql(MySqlQueryResult::default()),
        }
    }

    pub fn rows_affected(&self) -> u64 {
        match self {
            #[cfg(sqlite)]
            DynamicQueryResult::Sqlite(res) => res.rows_affected(),
            #[cfg(postgres)]
            DynamicQueryResult::Postgres(res) => res.rows_affected(),
            #[cfg(mysql)]
            DynamicQueryResult::MySql(res) => res.rows_affected(),
        }
    }
}

impl Extend<DynamicQueryResult> for DynamicQueryResult {
    fn extend<T: IntoIterator<Item = DynamicQueryResult>>(&mut self, iter: T) {
        #[allow(unreachable_patterns)]
        for elem in iter {
            match (&mut *self, elem) {
                #[cfg(sqlite)]
                (DynamicQueryResult::Sqlite(a), DynamicQueryResult::Sqlite(b)) => {
                    a.extend(std::iter::once(b));
                }
                #[cfg(postgres)]
                (DynamicQueryResult::Postgres(a), DynamicQueryResult::Postgres(b)) => {
                    a.extend(std::iter::once(b));
                }
                #[cfg(mysql)]
                (DynamicQueryResult::MySql(a), DynamicQueryResult::MySql(b)) => {
                    a.extend(std::iter::once(b));
                }
                _ => unreachable!("mixed database variants"),
            }
        }
    }
}

// Row

pub enum DynamicRow {
    #[cfg(sqlite)]
    Sqlite(SqliteRow),
    #[cfg(postgres)]
    Postgres(PgRow),
    #[cfg(mysql)]
    MySql(MySqlRow),
}

#[cfg(sqlite)]
impl From<SqliteRow> for DynamicRow {
    fn from(value: SqliteRow) -> Self {
        DynamicRow::Sqlite(value)
    }
}

#[cfg(postgres)]
impl From<PgRow> for DynamicRow {
    fn from(value: PgRow) -> Self {
        DynamicRow::Postgres(value)
    }
}

#[cfg(mysql)]
impl From<MySqlRow> for DynamicRow {
    fn from(value: MySqlRow) -> Self {
        DynamicRow::MySql(value)
    }
}

// FromRow

#[cfg(sqlite)]
pub trait FromSqliteRow: for<'r> FromRow<'r, SqliteRow> {}
#[cfg(not(feature = "sqlite"))]
pub trait FromSqliteRow {}

#[cfg(postgres)]
pub trait FromPgRow: for<'r> FromRow<'r, PgRow> {}
#[cfg(not(feature = "postgres"))]
pub trait FromPgRow {}

#[cfg(mysql)]
pub trait FromMySqlRow: for<'r> FromRow<'r, MySqlRow> {}
#[cfg(not(feature = "mysql"))]
pub trait FromMySqlRow {}

pub trait DynamicFromRow: FromSqliteRow + FromPgRow + FromMySqlRow {}

#[cfg(sqlite)]
impl<T: for<'r> FromRow<'r, SqliteRow>> FromSqliteRow for T {}
#[cfg(not(feature = "sqlite"))]
impl<T> FromSqliteRow for T {}

#[cfg(postgres)]
impl<T: for<'r> FromRow<'r, PgRow>> FromPgRow for T {}
#[cfg(not(feature = "postgres"))]
impl<T> FromPgRow for T {}

#[cfg(mysql)]
impl<T: for<'r> FromRow<'r, MySqlRow>> FromMySqlRow for T {}
#[cfg(not(feature = "mysql"))]
impl<T> FromMySqlRow for T {}

impl<T> DynamicFromRow for T where T: FromSqliteRow + FromPgRow + FromMySqlRow {}

// Decode

#[cfg(sqlite)]
pub trait DecodeSqlite: for<'r> Decode<'r, Sqlite> {}
#[cfg(not(feature = "sqlite"))]
pub trait DecodeSqlite {}

#[cfg(postgres)]
pub trait DecodePostgres: for<'r> Decode<'r, Postgres> {}
#[cfg(not(feature = "postgres"))]
pub trait DecodePostgres {}

#[cfg(mysql)]
pub trait DecodeMySql: for<'r> Decode<'r, MySql> {}
#[cfg(not(feature = "mysql"))]
pub trait DecodeMySql {}

pub trait DynamicDecode: DecodeSqlite + DecodePostgres + DecodeMySql {}

#[cfg(sqlite)]
impl<T: for<'r> Decode<'r, Sqlite>> DecodeSqlite for T {}
#[cfg(not(feature = "sqlite"))]
impl<T> DecodeSqlite for T {}

#[cfg(postgres)]
impl<T: for<'r> Decode<'r, Postgres>> DecodePostgres for T {}
#[cfg(not(feature = "postgres"))]
impl<T> DecodePostgres for T {}

#[cfg(mysql)]
impl<T: for<'r> Decode<'r, MySql>> DecodeMySql for T {}
#[cfg(not(feature = "mysql"))]
impl<T> DecodeMySql for T {}

impl<T> DynamicDecode for T where T: DecodeSqlite + DecodePostgres + DecodeMySql {}

// Type

#[cfg(sqlite)]
pub trait TypeSqlite: Type<Sqlite> {}
#[cfg(not(feature = "sqlite"))]
pub trait TypeSqlite {}

#[cfg(postgres)]
pub trait TypePostgres: Type<Postgres> {}
#[cfg(not(feature = "postgres"))]
pub trait TypePostgres {}

#[cfg(mysql)]
pub trait TypeMySql: Type<MySql> {}
#[cfg(not(feature = "mysql"))]
pub trait TypeMySql {}

pub trait DynamicType: TypeSqlite + TypePostgres + TypeMySql {}

#[cfg(sqlite)]
impl<T: Type<Sqlite>> TypeSqlite for T {}
#[cfg(not(feature = "sqlite"))]
impl<T> TypeSqlite for T {}

#[cfg(postgres)]
impl<T: Type<Postgres>> TypePostgres for T {}
#[cfg(not(feature = "postgres"))]
impl<T> TypePostgres for T {}

#[cfg(mysql)]
impl<T: Type<MySql>> TypeMySql for T {}
#[cfg(not(feature = "mysql"))]
impl<T> TypeMySql for T {}

impl<T> DynamicType for T where T: TypeSqlite + TypePostgres + TypeMySql {}
