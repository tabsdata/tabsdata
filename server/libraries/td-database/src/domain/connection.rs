//
// Copyright 2026 Tabsdata Inc.
//

//! [`DomainConnection`] — a connection to one domain. `&mut
//! DomainConnection<D, S, M>` IS the domain's executor: query it directly
//! (`conn...`). Opening and closing the transaction it runs in belong to
//! its [`AccessMode`] `M`, which the type keeps: a write needs a
//! [`WriteMode`](crate::access::WriteMode), and work that must run under
//! the project's lock spells `XW`.

use crate::access::{AccessMode, DbScope};
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

pub struct DomainConnection<D: DbDomain, S: DbScope, M: AccessMode> {
    conn: Transaction<'static, Postgres>,
    _d: PhantomData<D>,
    _s: PhantomData<S>,
    _m: PhantomData<M>,
}

impl<D: DbDomain, S: DbScope, M: AccessMode> DomainConnection<D, S, M> {
    /// The caller asserts `conn` is pointed at `D`'s schema, in scope `S`,
    /// opened as `M`.
    pub fn new(conn: Transaction<'static, Postgres>) -> Self {
        Self {
            conn,
            _d: PhantomData,
            _s: PhantomData,
            _m: PhantomData,
        }
    }

    pub async fn commit(self) -> Result<(), Error> {
        self.conn.commit().await
    }

    pub async fn rollback(self) -> Result<(), Error> {
        self.conn.rollback().await
    }

    /// The transaction itself, for a re-typing that keeps the unit of work —
    /// see `GatedConnection::enter_project`.
    pub(crate) fn into_transaction(self) -> Transaction<'static, Postgres> {
        self.conn
    }
}

impl<D: DbDomain, S: DbScope, M: AccessMode> Deref for DomainConnection<D, S, M> {
    type Target = Transaction<'static, Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<D: DbDomain, S: DbScope, M: AccessMode> DerefMut for DomainConnection<D, S, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl<D: DbDomain, S: DbScope, M: AccessMode> Domained for &'_ mut DomainConnection<D, S, M> {
    type Domain = D;
    type Scope = S;
    type Mode = M;
}

impl<D: DbDomain, S: DbScope, M: AccessMode> SeaExecutor for &mut DomainConnection<D, S, M> {
    type Reused<'a>
        = &'a mut DomainConnection<D, S, M>
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
        async move { op(sql, values, &mut self.conn).await }.boxed()
    }
}
