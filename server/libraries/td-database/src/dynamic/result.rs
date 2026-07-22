//
// Copyright 2026 Tabsdata Inc.
//

//! [`DynamicQueryResult`] and [`DynamicRow`] — backend-erased query outputs.

use crate::sea_query::kind::DatabaseBackend;
#[cfg(mysql)]
use sqlx::mysql::{MySqlQueryResult, MySqlRow};
#[cfg(postgres)]
use sqlx::postgres::{PgQueryResult, PgRow};
#[cfg(sqlite)]
use sqlx::sqlite::{SqliteQueryResult, SqliteRow};

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
