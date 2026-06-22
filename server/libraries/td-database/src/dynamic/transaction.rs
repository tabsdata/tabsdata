//
// Copyright 2026 Tabs Data Inc.
//

//! [`DynamicTransaction`] — a backend-erased transaction, convertible to and
//! from a [`DynamicConnection`].

use super::{ConnectionHolder, DynamicConnection};
#[cfg(mysql)]
use sqlx::MySql;
#[cfg(postgres)]
use sqlx::Postgres;
#[cfg(sqlite)]
use sqlx::Sqlite;
use sqlx::{Error, Transaction};

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
