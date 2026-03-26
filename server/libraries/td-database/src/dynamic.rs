//
// Copyright 2026 Tabs Data Inc.
//

use sqlx::pool::PoolConnection;
use sqlx::{Connection, Decode, Error, FromRow, Transaction, Type};
#[cfg(feature = "mysql")]
use sqlx::{
    MySql, MySqlPool,
    mysql::{MySqlQueryResult, MySqlRow},
};
#[cfg(feature = "postgres")]
use sqlx::{
    PgPool, Postgres,
    postgres::{PgQueryResult, PgRow},
};
#[cfg(feature = "sqlite")]
use sqlx::{
    Sqlite, SqlitePool,
    sqlite::{SqliteQueryResult, SqliteRow},
};
use std::ops::DerefMut;

// Pool

#[derive(Debug, Clone)]
pub enum DynamicPool {
    #[cfg(feature = "sqlite")]
    Sqlite { ro: SqlitePool, rw: SqlitePool },
    #[cfg(feature = "postgres")]
    Postgres { ro: PgPool, rw: PgPool },
    #[cfg(feature = "mysql")]
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
            #[cfg(feature = "sqlite")]
            DynamicPool::Sqlite { ro, .. } => {
                let conn = ro.acquire().await?;
                Ok(DynamicConnection::Sqlite(ConnectionHolder::Owned(conn)))
            }
            #[cfg(feature = "postgres")]
            DynamicPool::Postgres { ro, .. } => {
                let conn = ro.acquire().await?;
                Ok(DynamicConnection::Postgres(ConnectionHolder::Owned(conn)))
            }
            #[cfg(feature = "mysql")]
            DynamicPool::MySql { ro, .. } => {
                let conn = ro.acquire().await?;
                Ok(DynamicConnection::MySql(ConnectionHolder::Owned(conn)))
            }
        }
    }

    /// Begin a transaction on the read-write pool
    pub async fn begin<'c>(&self) -> Result<DynamicTransaction<'c>, Error> {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicPool::Sqlite { rw, .. } => {
                let tx = rw.begin().await?;
                Ok(DynamicTransaction::Sqlite(tx))
            }
            #[cfg(feature = "postgres")]
            DynamicPool::Postgres { rw, .. } => {
                let tx = rw.begin().await?;
                Ok(DynamicTransaction::Postgres(tx))
            }
            #[cfg(feature = "mysql")]
            DynamicPool::MySql { rw, .. } => {
                let tx = rw.begin().await?;
                Ok(DynamicTransaction::MySql(tx))
            }
        }
    }

    /// Returns if the pool is closed.
    pub fn is_closed(&self) -> bool {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicPool::Sqlite { ro, rw } => ro.is_closed() && rw.is_closed(),
            #[cfg(feature = "postgres")]
            DynamicPool::Postgres { ro, rw } => ro.is_closed() && rw.is_closed(),
            #[cfg(feature = "mysql")]
            DynamicPool::MySql { ro, rw } => ro.is_closed() && rw.is_closed(),
        }
    }

    /// Close both pools, waiting for all connections to be returned and closed.
    pub async fn close(&self) {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicPool::Sqlite { ro, rw } => {
                ro.close().await;
                rw.close().await;
            }
            #[cfg(feature = "postgres")]
            DynamicPool::Postgres { ro, rw } => {
                ro.close().await;
                rw.close().await;
            }
            #[cfg(feature = "mysql")]
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
    #[cfg(feature = "sqlite")]
    Sqlite(ConnectionHolder<'c, Sqlite, PoolConnection<Sqlite>>),
    #[cfg(feature = "postgres")]
    Postgres(ConnectionHolder<'c, Postgres, PoolConnection<Postgres>>),
    #[cfg(feature = "mysql")]
    MySql(ConnectionHolder<'c, MySql, PoolConnection<MySql>>),
}

impl<'c> DynamicConnection<'c> {
    pub async fn ping(&mut self) -> Result<(), Error> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(h) => h.ping().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(h) => h.ping().await,
            #[cfg(feature = "mysql")]
            Self::MySql(h) => h.ping().await,
        }
    }

    pub fn as_borrowed(&mut self) -> DynamicConnection<'_> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(h) => DynamicConnection::Sqlite(ConnectionHolder::Borrowed(h.deref_mut())),
            #[cfg(feature = "postgres")]
            Self::Postgres(h) => {
                DynamicConnection::Postgres(ConnectionHolder::Borrowed(h.deref_mut()))
            }
            #[cfg(feature = "mysql")]
            Self::MySql(h) => DynamicConnection::MySql(ConnectionHolder::Borrowed(h.deref_mut())),
        }
    }

    /// Execute a batch of SQL statements on this connection.
    pub async fn execute_batch(&mut self, stmts: &[String]) -> Result<(), Error> {
        for stmt in stmts {
            match self {
                #[cfg(feature = "sqlite")]
                Self::Sqlite(h) => {
                    sqlx::query(stmt).execute(&mut **h).await?;
                }
                #[cfg(feature = "postgres")]
                Self::Postgres(h) => {
                    sqlx::query(stmt).execute(&mut **h).await?;
                }
                #[cfg(feature = "mysql")]
                Self::MySql(h) => {
                    sqlx::query(stmt).execute(&mut **h).await?;
                }
            }
        }
        Ok(())
    }

    /// Try to extract the embedded transaction from this connection.
    /// Returns `Some(DynamicTransaction)` if this connection was created via
    /// `DynamicTransaction::into_connection()`, otherwise returns `None`.
    pub fn into_transaction(self) -> Option<DynamicTransaction<'c>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(ConnectionHolder::OwnedTransaction(tx)) => {
                Some(DynamicTransaction::Sqlite(tx))
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(ConnectionHolder::OwnedTransaction(tx)) => {
                Some(DynamicTransaction::Postgres(tx))
            }
            #[cfg(feature = "mysql")]
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
    #[cfg(feature = "sqlite")]
    Sqlite(Transaction<'c, Sqlite>),
    #[cfg(feature = "postgres")]
    Postgres(Transaction<'c, Postgres>),
    #[cfg(feature = "mysql")]
    MySql(Transaction<'c, MySql>),
}

impl<'c> DynamicTransaction<'c> {
    /// Commit the transaction
    pub async fn commit(self) -> Result<(), Error> {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicTransaction::Sqlite(tx) => tx.commit().await,
            #[cfg(feature = "postgres")]
            DynamicTransaction::Postgres(tx) => tx.commit().await,
            #[cfg(feature = "mysql")]
            DynamicTransaction::MySql(tx) => tx.commit().await,
        }
    }

    /// Rollback the transaction
    pub async fn rollback(self) -> Result<(), Error> {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicTransaction::Sqlite(tx) => tx.rollback().await,
            #[cfg(feature = "postgres")]
            DynamicTransaction::Postgres(tx) => tx.rollback().await,
            #[cfg(feature = "mysql")]
            DynamicTransaction::MySql(tx) => tx.rollback().await,
        }
    }

    pub fn as_mut_conn(&mut self) -> DynamicConnection<'_> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(tx) => DynamicConnection::Sqlite(ConnectionHolder::Borrowed(tx.as_mut())),
            #[cfg(feature = "postgres")]
            Self::Postgres(tx) => {
                DynamicConnection::Postgres(ConnectionHolder::Borrowed(tx.as_mut()))
            }
            #[cfg(feature = "mysql")]
            Self::MySql(tx) => DynamicConnection::MySql(ConnectionHolder::Borrowed(tx.as_mut())),
        }
    }

    /// Convert this transaction into a `DynamicConnection` by embedding the
    /// transaction inside the `OwnedTransaction` variant of `ConnectionHolder`.
    pub fn into_connection(self) -> DynamicConnection<'c> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(tx) => DynamicConnection::Sqlite(ConnectionHolder::OwnedTransaction(tx)),
            #[cfg(feature = "postgres")]
            Self::Postgres(tx) => {
                DynamicConnection::Postgres(ConnectionHolder::OwnedTransaction(tx))
            }
            #[cfg(feature = "mysql")]
            Self::MySql(tx) => DynamicConnection::MySql(ConnectionHolder::OwnedTransaction(tx)),
        }
    }
}

// Query result

#[derive(Debug)]
pub enum DynamicQueryResult {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteQueryResult),
    #[cfg(feature = "postgres")]
    Postgres(PgQueryResult),
    #[cfg(feature = "mysql")]
    MySql(MySqlQueryResult),
}

#[cfg(feature = "sqlite")]
impl From<SqliteQueryResult> for DynamicQueryResult {
    fn from(value: SqliteQueryResult) -> Self {
        DynamicQueryResult::Sqlite(value)
    }
}

#[cfg(feature = "postgres")]
impl From<PgQueryResult> for DynamicQueryResult {
    fn from(value: PgQueryResult) -> Self {
        DynamicQueryResult::Postgres(value)
    }
}

#[cfg(feature = "mysql")]
impl From<MySqlQueryResult> for DynamicQueryResult {
    fn from(value: MySqlQueryResult) -> Self {
        DynamicQueryResult::MySql(value)
    }
}

impl DynamicQueryResult {
    pub fn rows_affected(&self) -> u64 {
        match self {
            #[cfg(feature = "sqlite")]
            DynamicQueryResult::Sqlite(res) => res.rows_affected(),
            #[cfg(feature = "postgres")]
            DynamicQueryResult::Postgres(res) => res.rows_affected(),
            #[cfg(feature = "mysql")]
            DynamicQueryResult::MySql(res) => res.rows_affected(),
        }
    }
}

// Row

pub enum DynamicRow {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteRow),
    #[cfg(feature = "postgres")]
    Postgres(PgRow),
    #[cfg(feature = "mysql")]
    MySql(MySqlRow),
}

#[cfg(feature = "sqlite")]
impl From<SqliteRow> for DynamicRow {
    fn from(value: SqliteRow) -> Self {
        DynamicRow::Sqlite(value)
    }
}

#[cfg(feature = "postgres")]
impl From<PgRow> for DynamicRow {
    fn from(value: PgRow) -> Self {
        DynamicRow::Postgres(value)
    }
}

#[cfg(feature = "mysql")]
impl From<MySqlRow> for DynamicRow {
    fn from(value: MySqlRow) -> Self {
        DynamicRow::MySql(value)
    }
}

// FromRow

#[cfg(feature = "sqlite")]
pub trait FromSqliteRow: for<'r> FromRow<'r, SqliteRow> {}
#[cfg(not(feature = "sqlite"))]
pub trait FromSqliteRow {}

#[cfg(feature = "postgres")]
pub trait FromPgRow: for<'r> FromRow<'r, PgRow> {}
#[cfg(not(feature = "postgres"))]
pub trait FromPgRow {}

#[cfg(feature = "mysql")]
pub trait FromMySqlRow: for<'r> FromRow<'r, MySqlRow> {}
#[cfg(not(feature = "mysql"))]
pub trait FromMySqlRow {}

pub trait DynamicFromRow: FromSqliteRow + FromPgRow + FromMySqlRow {}

#[cfg(feature = "sqlite")]
impl<T: for<'r> FromRow<'r, SqliteRow>> FromSqliteRow for T {}
#[cfg(not(feature = "sqlite"))]
impl<T> FromSqliteRow for T {}

#[cfg(feature = "postgres")]
impl<T: for<'r> FromRow<'r, PgRow>> FromPgRow for T {}
#[cfg(not(feature = "postgres"))]
impl<T> FromPgRow for T {}

#[cfg(feature = "mysql")]
impl<T: for<'r> FromRow<'r, MySqlRow>> FromMySqlRow for T {}
#[cfg(not(feature = "mysql"))]
impl<T> FromMySqlRow for T {}

impl<T> DynamicFromRow for T where T: FromSqliteRow + FromPgRow + FromMySqlRow {}

// Decode

#[cfg(feature = "sqlite")]
pub trait DecodeSqlite: for<'r> Decode<'r, Sqlite> {}
#[cfg(not(feature = "sqlite"))]
pub trait DecodeSqlite {}

#[cfg(feature = "postgres")]
pub trait DecodePostgres: for<'r> Decode<'r, Postgres> {}
#[cfg(not(feature = "postgres"))]
pub trait DecodePostgres {}

#[cfg(feature = "mysql")]
pub trait DecodeMySql: for<'r> Decode<'r, MySql> {}
#[cfg(not(feature = "mysql"))]
pub trait DecodeMySql {}

pub trait DynamicDecode: DecodeSqlite + DecodePostgres + DecodeMySql {}

#[cfg(feature = "sqlite")]
impl<T: for<'r> Decode<'r, Sqlite>> DecodeSqlite for T {}
#[cfg(not(feature = "sqlite"))]
impl<T> DecodeSqlite for T {}

#[cfg(feature = "postgres")]
impl<T: for<'r> Decode<'r, Postgres>> DecodePostgres for T {}
#[cfg(not(feature = "postgres"))]
impl<T> DecodePostgres for T {}

#[cfg(feature = "mysql")]
impl<T: for<'r> Decode<'r, MySql>> DecodeMySql for T {}
#[cfg(not(feature = "mysql"))]
impl<T> DecodeMySql for T {}

impl<T> DynamicDecode for T where T: DecodeSqlite + DecodePostgres + DecodeMySql {}

// Type

#[cfg(feature = "sqlite")]
pub trait TypeSqlite: Type<Sqlite> {}
#[cfg(not(feature = "sqlite"))]
pub trait TypeSqlite {}

#[cfg(feature = "postgres")]
pub trait TypePostgres: Type<Postgres> {}
#[cfg(not(feature = "postgres"))]
pub trait TypePostgres {}

#[cfg(feature = "mysql")]
pub trait TypeMySql: Type<MySql> {}
#[cfg(not(feature = "mysql"))]
pub trait TypeMySql {}

pub trait DynamicType: TypeSqlite + TypePostgres + TypeMySql {}

#[cfg(feature = "sqlite")]
impl<T: Type<Sqlite>> TypeSqlite for T {}
#[cfg(not(feature = "sqlite"))]
impl<T> TypeSqlite for T {}

#[cfg(feature = "postgres")]
impl<T: Type<Postgres>> TypePostgres for T {}
#[cfg(not(feature = "postgres"))]
impl<T> TypePostgres for T {}

#[cfg(feature = "mysql")]
impl<T: Type<MySql>> TypeMySql for T {}
#[cfg(not(feature = "mysql"))]
impl<T> TypeMySql for T {}

impl<T> DynamicType for T where T: TypeSqlite + TypePostgres + TypeMySql {}
