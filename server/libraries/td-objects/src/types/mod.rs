//
// Copyright 2025 Tabs Data Inc.
//

pub mod basic;

pub mod collection;
pub mod dependency;
pub mod function;
pub mod permission;
pub mod role;
pub mod table;
pub mod trigger;
pub mod user;

mod parse;
mod table_ref;

#[cfg(test)]
mod tests;

pub trait SqlEntity: Send + Sync + 'static {
    type Type: for<'a> sqlx::Encode<'a, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite> + std::fmt::Display;
    fn value(&self) -> &Self::Type;
}

pub trait IdOrName: Send + Sync {
    type Id: SqlEntity;
    fn id(&self) -> Option<&Self::Id>;

    type Name: SqlEntity;
    fn name(&self) -> Option<&Self::Name>;
}

pub trait DataAccessObject:
    for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> + Send + Sync + Unpin
{
    fn sql_table() -> &'static str;
    fn fields() -> &'static [&'static str];
    fn sql_field_for_type<E: SqlEntity>() -> Option<&'static str>;
    fn values_query_builder(
        &self,
        sql: String,
        bindings: &[&str],
    ) -> sqlx::QueryBuilder<'_, sqlx::Sqlite>;
    fn tuples_query_builder(
        &self,
        sql: String,
        bindings: &[&str],
    ) -> sqlx::QueryBuilder<'_, sqlx::Sqlite>;
}

pub trait DataLogicObject {}

pub trait DataTransferObject {}

pub trait ComposedString {
    fn parse(s: impl Into<String>) -> Result<Self, td_error::TdError>
    where
        Self: Sized;

    fn compose(&self) -> String;
}
