//
// Copyright 2025 Tabs Data Inc.
//

pub mod basic;

pub mod dependency;
pub mod function;
pub mod permission;
pub mod role;
pub mod table;
pub mod trigger;

#[cfg(test)]
mod tests;

pub trait SqlEntity: 'static {
    type Type: for<'a> sqlx::Encode<'a, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>;
    fn value(&self) -> &Self::Type;
}

pub trait DataAccessObject {
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
