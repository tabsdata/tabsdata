//
// Copyright 2026 Tabs Data Inc.
//

//! Backend-agnostic marker traits. Each erases the per-backend `FromRow` /
//! `Decode` / `Type` bound behind a single trait (`DynamicFromRow`,
//! `DynamicDecode`, `DynamicType`) that is blanket-implemented for any type
//! satisfying all enabled backends, so generic query code need not name a
//! specific backend. The `cfg(not(feature = ...))` arms keep the trait present
//! (with no bound) when a backend is disabled.

#[cfg(mysql)]
use sqlx::MySql;
#[cfg(postgres)]
use sqlx::Postgres;
#[cfg(sqlite)]
use sqlx::Sqlite;
use sqlx::{Decode, FromRow, Type};
#[cfg(mysql)]
use sqlx::mysql::MySqlRow;
#[cfg(postgres)]
use sqlx::postgres::PgRow;
#[cfg(sqlite)]
use sqlx::sqlite::SqliteRow;

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
