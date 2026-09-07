//
// Copyright 2026 Tabsdata Inc.
//

use crate::sea_query::executor::SeaExecutor;
use futures_util::future::BoxFuture;
use sea_query::{PostgresQueryBuilder, QueryStatementWriter};
use sea_query_sqlx::SqlxValues;
use sqlx::postgres::{PgQueryResult, PgRow};
use sqlx::{Decode, Error, FromRow, Postgres, Type};
use std::marker::PhantomData;

/// QueryBuilder trait to convert from SeaQuery to String/Values tuple for sqlx.
pub trait SeaQuerySqlxBuild {
    fn build_sea_query(self) -> (String, SqlxValues);
}

impl<T> SeaQuerySqlxBuild for &mut T
where
    T: QueryStatementWriter,
{
    fn build_sea_query(self) -> (String, SqlxValues) {
        let (sql, values) = self.build(PostgresQueryBuilder);
        (sql, SqlxValues(values))
    }
}

impl SeaQuerySqlxBuild for (String, SqlxValues) {
    fn build_sea_query(self) -> (String, SqlxValues) {
        self
    }
}

/// SeaQuery builders
pub struct SeaQueryBuilder<T> {
    query: T,
}

pub struct SeaQueryAsBuilder<T, O> {
    query: T,
    _phantom: PhantomData<O>,
}

pub struct SeaQueryScalarBuilder<T, O> {
    query: T,
    _phantom: PhantomData<O>,
}

/// Extension trait for QueryStatementWriter (for fluent API)
pub trait SeaQueryExt: SeaQuerySqlxBuild + Sized {
    fn build_query(self) -> SeaQueryBuilder<Self> {
        SeaQueryBuilder { query: self }
    }

    fn build_query_as<O>(self) -> SeaQueryAsBuilder<Self, O> {
        SeaQueryAsBuilder {
            query: self,
            _phantom: PhantomData,
        }
    }

    fn build_query_scalar<O>(self) -> SeaQueryScalarBuilder<Self, O> {
        SeaQueryScalarBuilder {
            query: self,
            _phantom: PhantomData,
        }
    }
}

impl<T: SeaQuerySqlxBuild> SeaQueryExt for T {}

impl<T> SeaQueryBuilder<T>
where
    T: SeaQuerySqlxBuild,
{
    pub fn execute<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxFuture<'q, Result<PgQueryResult, Error>> {
        pool.execute(self.query)
    }

    pub fn fetch_all<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxFuture<'q, Result<Vec<PgRow>, Error>> {
        pool.fetch_all(self.query)
    }

    pub fn fetch_one<'q>(self, pool: impl SeaExecutor + 'q) -> BoxFuture<'q, Result<PgRow, Error>> {
        pool.fetch_one(self.query)
    }

    pub fn fetch_optional<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxFuture<'q, Result<Option<PgRow>, Error>> {
        pool.fetch_optional(self.query)
    }
}

impl<T, O> SeaQueryAsBuilder<T, O>
where
    T: SeaQuerySqlxBuild,
    O: Send + Unpin + for<'r> FromRow<'r, PgRow>,
{
    pub fn fetch_all<'c>(self, pool: impl SeaExecutor + 'c) -> BoxFuture<'c, Result<Vec<O>, Error>>
    where
        O: 'c,
    {
        pool.fetch_all_as::<O>(self.query)
    }

    pub fn fetch_one<'c>(self, pool: impl SeaExecutor + 'c) -> BoxFuture<'c, Result<O, Error>>
    where
        O: 'c,
    {
        pool.fetch_one_as::<O>(self.query)
    }

    pub fn fetch_optional<'c>(
        self,
        pool: impl SeaExecutor + 'c,
    ) -> BoxFuture<'c, Result<Option<O>, Error>>
    where
        O: 'c,
    {
        pool.fetch_optional_as::<O>(self.query)
    }
}

impl<T, O> SeaQueryScalarBuilder<T, O>
where
    T: SeaQuerySqlxBuild,
    O: Send + Unpin + for<'r> Decode<'r, Postgres> + Type<Postgres>,
{
    pub fn fetch_all<'c>(self, pool: impl SeaExecutor + 'c) -> BoxFuture<'c, Result<Vec<O>, Error>>
    where
        O: 'c,
    {
        pool.fetch_all_scalar::<O>(self.query)
    }

    pub fn fetch_one<'c>(self, pool: impl SeaExecutor + 'c) -> BoxFuture<'c, Result<O, Error>>
    where
        O: 'c,
    {
        pool.fetch_one_scalar::<O>(self.query)
    }

    pub fn fetch_optional<'c>(
        self,
        pool: impl SeaExecutor + 'c,
    ) -> BoxFuture<'c, Result<Option<O>, Error>>
    where
        O: 'c,
    {
        pool.fetch_optional_scalar::<O>(self.query)
    }
}
