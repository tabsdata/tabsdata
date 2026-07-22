//
// Copyright 2026 Tabsdata Inc.
//

//! Backend-erased database primitives: a single set of types that wrap the
//! per-backend sqlx pools/connections so the rest of the crate never names a
//! concrete backend.
//!
//! * [`DynamicPool`] — the read-only / read-write pool pair (here).
//! * [`DynamicConnection`] / [`ConnectionHolder`] — a connection (or borrow /
//!   owned transaction) plus the per-request scope primitives ([`connection`]).
//! * [`DynamicTransaction`] — a transaction ([`transaction`]).
//! * [`DynamicQueryResult`] / [`DynamicRow`] — query outputs ([`result`]).
//! * The `Dynamic{FromRow,Decode,Type}` marker traits ([`convert`]).

mod connection;
mod convert;
mod result;
mod transaction;

pub use connection::{ConnectionHolder, DynamicConnection};
pub use convert::{
    DecodeMySql, DecodePostgres, DecodeSqlite, DynamicDecode, DynamicFromRow, DynamicType,
    FromMySqlRow, FromPgRow, FromSqliteRow, TypeMySql, TypePostgres, TypeSqlite,
};
pub use result::{DynamicQueryResult, DynamicRow};
pub use transaction::DynamicTransaction;

use sqlx::Error;
#[cfg(mysql)]
use sqlx::MySqlPool;
#[cfg(postgres)]
use sqlx::PgPool;
#[cfg(sqlite)]
use sqlx::SqlitePool;

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
