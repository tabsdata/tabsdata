//
// Copyright 2026 Tabs Data Inc.
//

use crate::dynamic::{DynamicDecode, DynamicFromRow, DynamicQueryResult, DynamicRow, DynamicType};
use crate::sea_query::executor::SeaExecutor;
use crate::sea_query::kind::{DatabaseBackend, DatabaseKind};
use crate::sea_query::raw::RawSeaQuery;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sea_query::QueryStatementWriter;
use sea_query_sqlx::SqlxValues;
use sqlx::{Either, Error};
use std::marker::PhantomData;

/// QueryBuilder trait to convert from SeaQuery to String/Values tuple for sqlx.
pub trait SeaQuerySqlxBuild {
    fn build_sea_query(self, db: impl DatabaseKind) -> (String, SqlxValues);
}

impl<T> SeaQuerySqlxBuild for &mut T
where
    T: QueryStatementWriter,
{
    fn build_sea_query(self, db: impl DatabaseKind) -> (String, SqlxValues) {
        let (sql, values) = match db.kind() {
            #[cfg(sqlite)]
            DatabaseBackend::Sqlite => self.build(sea_query::SqliteQueryBuilder),
            #[cfg(postgres)]
            DatabaseBackend::Postgres => self.build(sea_query::PostgresQueryBuilder),
            #[cfg(mysql)]
            DatabaseBackend::MySql => self.build(sea_query::MysqlQueryBuilder),
        };
        (sql, SqlxValues(values))
    }
}

impl SeaQuerySqlxBuild for RawSeaQuery {
    fn build_sea_query(self, _db: impl DatabaseKind) -> (String, SqlxValues) {
        let (sql, values) = self.0.into_parts();
        (sql, SqlxValues(values))
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
    ) -> BoxFuture<'q, Result<DynamicQueryResult, Error>> {
        pool.execute(self.query)
    }

    pub fn fetch<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxStream<'q, Result<DynamicRow, Error>> {
        pool.fetch(self.query)
    }

    #[allow(deprecated)] // remove when needed
    pub fn fetch_many<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxStream<'q, Result<Either<DynamicQueryResult, DynamicRow>, Error>> {
        pool.fetch_many(self.query)
    }

    pub fn fetch_all<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxFuture<'q, Result<Vec<DynamicRow>, Error>> {
        pool.fetch_all(self.query)
    }

    pub fn fetch_one<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxFuture<'q, Result<DynamicRow, Error>> {
        pool.fetch_one(self.query)
    }

    pub fn fetch_optional<'q>(
        self,
        pool: impl SeaExecutor + 'q,
    ) -> BoxFuture<'q, Result<Option<DynamicRow>, Error>> {
        pool.fetch_optional(self.query)
    }
}

impl<T, O> SeaQueryAsBuilder<T, O>
where
    T: SeaQuerySqlxBuild,
    O: Send + Unpin + DynamicFromRow,
{
    pub fn fetch_all<'c>(
        self,
        pool: impl SeaExecutor + 'c,
    ) -> BoxFuture<'c, Result<Vec<O>, Error>> {
        pool.fetch_all_as::<O>(self.query)
    }

    pub fn fetch_one<'c>(self, pool: impl SeaExecutor + 'c) -> BoxFuture<'c, Result<O, Error>> {
        pool.fetch_one_as::<O>(self.query)
    }

    pub fn fetch_optional<'c>(
        self,
        pool: impl SeaExecutor + 'c,
    ) -> BoxFuture<'c, Result<Option<O>, Error>> {
        pool.fetch_optional_as::<O>(self.query)
    }

    pub fn fetch<'c>(self, pool: impl SeaExecutor + 'c) -> BoxStream<'c, Result<O, Error>>
    where
        O: 'c,
    {
        pool.fetch_as::<O>(self.query)
    }
}

impl<T, O> SeaQueryScalarBuilder<T, O>
where
    T: SeaQuerySqlxBuild,
    O: Send + Unpin + DynamicDecode + DynamicType,
{
    pub fn fetch_all<'c>(
        self,
        pool: impl SeaExecutor + 'c,
    ) -> BoxFuture<'c, Result<Vec<O>, Error>> {
        pool.fetch_all_scalar::<O>(self.query)
    }

    pub fn fetch_one<'c>(self, pool: impl SeaExecutor + 'c) -> BoxFuture<'c, Result<O, Error>> {
        pool.fetch_one_scalar::<O>(self.query)
    }

    pub fn fetch_optional<'c>(
        self,
        pool: impl SeaExecutor + 'c,
    ) -> BoxFuture<'c, Result<Option<O>, Error>> {
        pool.fetch_optional_scalar::<O>(self.query)
    }
}
