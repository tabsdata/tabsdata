//
// Copyright 2025 Tabs Data Inc.
//

pub mod basic;

pub mod collection;
pub mod dependency;
pub mod execution;
pub mod function;
pub mod permission;
pub mod role;
pub mod table;
pub mod trigger;
pub mod user;

mod parse;
pub mod table_ref;

pub mod auth;

#[cfg(test)]
mod tests;

#[cfg(feature = "test-utils")]
pub mod test_utils;

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
    fn order_by() -> &'static str;
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

pub trait PartitionBy {
    type PartitionBy: SqlEntity;
    fn partition_by() -> &'static str;
}

pub trait NaturalOrder {
    type NaturalOrder: SqlEntity;
    fn natural_order_by() -> &'static str;
}

pub trait Status {
    type Status: SqlEntity;
    fn status_by() -> &'static str;
}

pub trait Recursive {
    type Recursive: SqlEntity;
    fn recurse_up() -> &'static str;
    fn recurse_down() -> &'static str;
}
