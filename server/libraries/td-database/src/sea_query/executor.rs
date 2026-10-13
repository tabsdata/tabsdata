//
// Copyright 2026 Tabsdata Inc.
//

//! [`SeaExecutor`] — running a sea-query statement against one database domain.
//! An implementor supplies only [`run`](SeaExecutor::run), which obtains a
//! [`PgConnection`] and applies a [`RunOp`]; every method a caller reaches for
//! is provided on top of it.
//!
//! Every method is `run_`-prefixed: this is the raw seam, and the unprefixed
//! names belong to the typed surface in `td-queries`, which is in scope on
//! the same executors.
//!
//! [`RunOp`] is a `fn` pointer rather than a closure on purpose: it makes the
//! `for<'a>` binding explicit, which closure inference will not produce for a
//! future borrowing its connection argument.
//!
//! The SQL is owned: sqlx takes only `'static` or generated text.

use crate::domain::Domained;
use crate::generated_sql::generated;
use crate::pool::TdPool;
use crate::sea_query::builder::SeaQuerySqlxBuild;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use sea_query_sqlx::SqlxValues;
use sqlx::postgres::{PgConnection, PgPool, PgQueryResult, PgRow};
use sqlx::{Decode, Error, FromRow, Postgres, Type};

/// The `'a` binds the connection and the future together, so the future may
/// borrow it.
pub type RunOp<T> =
    for<'a> fn(String, SqlxValues, &'a mut PgConnection) -> BoxFuture<'a, Result<T, Error>>;

mod op {
    use super::*;

    macro_rules! op {
        ($name:ident, $ret:ty, $query:ident, $fetch:ident) => {
            pub(super) fn $name<'a>(
                sql: String,
                values: SqlxValues,
                conn: &'a mut PgConnection,
            ) -> BoxFuture<'a, Result<$ret, Error>> {
                async move { sqlx::$query(generated(sql), values).$fetch(conn).await }.boxed()
            }
        };
        ($name:ident, $ret:ty, $query:ident, $fetch:ident, $o:ident: $($bound:tt)+) => {
            pub(super) fn $name<'a, $o: Send + Unpin + $($bound)+>(
                sql: String,
                values: SqlxValues,
                conn: &'a mut PgConnection,
            ) -> BoxFuture<'a, Result<$ret, Error>> {
                async move { sqlx::$query(generated(sql), values).$fetch(conn).await }.boxed()
            }
        };
    }

    /// An unnamed statement: PostgreSQL plans it at bind time with the real
    /// parameter values, where a named statement's generic plan is blind to
    /// them after its fifth execution on a connection.
    pub(super) fn fetch_all_as_planned_per_call<
        'a,
        O: Send + Unpin + for<'r> FromRow<'r, PgRow>,
    >(
        sql: String,
        values: SqlxValues,
        conn: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<Vec<O>, Error>> {
        async move {
            sqlx::query_as_with(generated(sql), values)
                .persistent(false)
                .fetch_all(conn)
                .await
        }
        .boxed()
    }

    op!(execute, PgQueryResult, query_with, execute);
    op!(fetch_all, Vec<PgRow>, query_with, fetch_all);
    op!(fetch_one, PgRow, query_with, fetch_one);
    op!(fetch_optional, Option<PgRow>, query_with, fetch_optional);

    op!(fetch_all_as, Vec<O>, query_as_with, fetch_all, O: for<'r> FromRow<'r, PgRow>);
    op!(fetch_one_as, O, query_as_with, fetch_one, O: for<'r> FromRow<'r, PgRow>);
    op!(fetch_optional_as, Option<O>, query_as_with, fetch_optional, O: for<'r> FromRow<'r, PgRow>);

    op!(fetch_all_scalar, Vec<O>, query_scalar_with, fetch_all,
        O: for<'r> Decode<'r, Postgres> + Type<Postgres>);
    op!(fetch_one_scalar, O, query_scalar_with, fetch_one,
        O: for<'r> Decode<'r, Postgres> + Type<Postgres>);
    op!(fetch_optional_scalar, Option<O>, query_scalar_with, fetch_optional,
        O: for<'r> Decode<'r, Postgres> + Type<Postgres>);
}

/// Sqlx executor for SeaQueries; serves exactly one database domain
/// ([`Domained`] supertrait).
pub trait SeaExecutor: Domained + Sized {
    /// A short-lived executor obtained by reborrowing `self` — the same
    /// database in the same RLS scope and mode, so a reused executor reaches
    /// exactly what its parent does.
    type Reused<'a>: SeaExecutor
        + Send
        + Domained<Domain = Self::Domain, Scope = Self::Scope, Mode = Self::Mode>
    where
        Self: 'a;

    /// Obtain a short-lived executor for one query in a loop.
    /// For pools this is a no-op copy; for connections it reborrows.
    fn reuse(&mut self) -> Self::Reused<'_>;

    /// The one primitive; every method below forwards to it.
    fn run<'e, T: Send + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
        op: RunOp<T>,
    ) -> BoxFuture<'e, Result<T, Error>>
    where
        Self: 'e;

    fn run_execute<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<PgQueryResult, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::execute)
    }

    fn run_fetch_all<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<PgRow>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_all)
    }

    fn run_fetch_one<'e>(self, query: impl SeaQuerySqlxBuild) -> BoxFuture<'e, Result<PgRow, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_one)
    }

    fn run_fetch_optional<'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Option<PgRow>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_optional)
    }

    fn run_fetch_all_as<'e, O: Send + Unpin + for<'r> FromRow<'r, PgRow> + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<O>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_all_as::<O>)
    }

    fn run_fetch_all_as_planned_per_call<'e, O: Send + Unpin + for<'r> FromRow<'r, PgRow> + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<O>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_all_as_planned_per_call::<O>)
    }

    fn run_fetch_one_as<'e, O: Send + Unpin + for<'r> FromRow<'r, PgRow> + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<O, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_one_as::<O>)
    }

    fn run_fetch_optional_as<'e, O: Send + Unpin + for<'r> FromRow<'r, PgRow> + 'e>(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Option<O>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_optional_as::<O>)
    }

    fn run_fetch_all_scalar<
        'e,
        O: Send + Unpin + for<'r> Decode<'r, Postgres> + Type<Postgres> + 'e,
    >(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Vec<O>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_all_scalar::<O>)
    }

    fn run_fetch_one_scalar<
        'e,
        O: Send + Unpin + for<'r> Decode<'r, Postgres> + Type<Postgres> + 'e,
    >(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<O, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_one_scalar::<O>)
    }

    fn run_fetch_optional_scalar<
        'e,
        O: Send + Unpin + for<'r> Decode<'r, Postgres> + Type<Postgres> + 'e,
    >(
        self,
        query: impl SeaQuerySqlxBuild,
    ) -> BoxFuture<'e, Result<Option<O>, Error>>
    where
        Self: 'e,
    {
        self.run(query, op::fetch_optional_scalar::<O>)
    }
}

/// Per statement, as sqlx does internally when handed a pool.
impl SeaExecutor for &'_ PgPool {
    type Reused<'a>
        = &'a PgPool
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
        let pool = self.clone();
        async move {
            let mut conn = pool.acquire().await?;
            op(sql, values, &mut conn).await
        }
        .boxed()
    }
}

impl SeaExecutor for &'_ TdPool {
    type Reused<'a>
        = &'a TdPool
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
        self.as_ref().run(query, op)
    }
}

impl SeaExecutor for &mut PgConnection {
    type Reused<'a>
        = &'a mut PgConnection
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
        async move { op(sql, values, self).await }.boxed()
    }
}
