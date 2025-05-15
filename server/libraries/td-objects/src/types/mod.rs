//
// Copyright 2025 Tabs Data Inc.
//

pub mod basic;

pub mod auth;
pub mod collection;
pub mod dependency;
pub mod execution;
pub mod function;
pub mod parse;
pub mod permission;
pub mod role;
pub mod table;
pub mod table_ref;
pub mod trigger;
pub mod user;
pub mod worker;

#[cfg(test)]
mod tests;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub trait SqlEntity: Send + Sync + 'static {
    type Type: for<'a> sqlx::Encode<'a, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite> + std::fmt::Display;
    fn value(&self) -> &Self::Type;
}

impl<E> SqlEntity for Option<E>
where
    E: Send
        + Sync
        + 'static
        + for<'a> sqlx::Encode<'a, sqlx::Sqlite>
        + sqlx::Type<sqlx::Sqlite>
        + std::fmt::Display,
{
    type Type = E;

    // Useful to query values that are Optional, by passing Some(value)
    fn value(&self) -> &Self::Type {
        self.as_ref().expect("Expected Some value to be present")
    }
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
    type Builder;

    fn sql_table() -> &'static str;
    fn order_by() -> &'static str;
    fn fields() -> &'static [&'static str];
    fn immutable_fields() -> &'static [&'static str];
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

pub trait DataLogicObject {
    type Builder;
}

pub trait DataTransferObject {
    type Builder;
}

pub trait ListQuery: DataTransferObject {
    type Dao: DataAccessObject;

    fn list_on() -> &'static str {
        Self::Dao::sql_table()
    }

    fn fields() -> &'static [&'static str] {
        Self::Dao::fields()
    }

    fn try_from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError>
    where
        Self: Sized;
    fn natural_order_by() -> &'static str;
    fn order_by_fields() -> &'static [&'static str];
    fn filter_by_fields() -> &'static [&'static str];
    fn filter_by_like_fields() -> &'static [&'static str];
}

pub trait Extractor<T> {
    fn extract(&self) -> T;
}

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
