//
// Copyright 2026 Tabsdata Inc.
//

//! [`DomainConnection`] — a connection to one domain. `&mut
//! DomainConnection<D>` IS the domain's executor: query it directly
//! (`conn.live()...`). Opening and closing the transaction it runs in belong to
//! an [`AccessMode`](crate::access::AccessMode).

use crate::domain::{DbDomain, Domained};
use crate::sea_query::builder::SeaQuerySqlxBuild;
use crate::sea_query::executor::{RunOp, SeaExecutor};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use sqlx::Error;
use sqlx::Postgres;
use sqlx::Transaction;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub struct DomainConnection<D: DbDomain> {
    conn: Transaction<'static, Postgres>,
    _d: PhantomData<D>,
}

impl<D: DbDomain> DomainConnection<D> {
    /// The caller asserts `conn` is pointed at `D`'s schema.
    pub fn new(conn: Transaction<'static, Postgres>) -> Self {
        Self {
            conn,
            _d: PhantomData,
        }
    }

    pub async fn commit(self) -> Result<(), Error> {
        self.conn.commit().await
    }

    pub async fn rollback(self) -> Result<(), Error> {
        self.conn.rollback().await
    }
}

impl<D: DbDomain> Deref for DomainConnection<D> {
    type Target = Transaction<'static, Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<D: DbDomain> DerefMut for DomainConnection<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl<D: DbDomain> Domained for &'_ mut DomainConnection<D> {
    type Domain = D;
}

impl<D: DbDomain> SeaExecutor for &mut DomainConnection<D> {
    type Reused<'a>
        = &'a mut DomainConnection<D>
    where
        Self: 'a;

    fn reuse(&mut self) -> Self::Reused<'_> {
        self
    }

    fn run<'e, T: Send + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
        op: RunOp<T>,
    ) -> BoxFuture<'e, Result<T, Error>>
    where
        Self: 'e,
    {
        let (sql, values) = query.build_sea_query();
        async move { op(&sql, values, &mut self.conn).await }.boxed()
    }
}
