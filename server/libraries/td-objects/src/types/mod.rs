//
// Copyright 2025 Tabs Data Inc.
//

use std::fmt::Display;

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

/// A trait for types that can be used as SQL entities.
pub trait SqlEntity: Send + Sync + Display + Sized
where
    Self: 'static,
{
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>);
    fn push_bind_unseparated<'a, S: Display>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, S>,
    );

    fn type_name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

impl<T> SqlEntity for T
where
    T: IdOrName + Display + 'static,
{
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>) {
        if let Some(id) = self.id() {
            id.push_bind(builder);
        } else if let Some(name) = self.name() {
            name.push_bind(builder);
        } else {
            panic!("No ID or Name found");
        }
    }

    fn push_bind_unseparated<'a, S: Display>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, S>,
    ) {
        if let Some(id) = self.id() {
            id.push_bind_unseparated(builder);
        } else if let Some(name) = self.name() {
            name.push_bind_unseparated(builder);
        } else {
            panic!("No ID or Name found");
        }
    }

    fn type_name(&self) -> &str {
        if let Some(id) = self.id() {
            std::any::type_name_of_val(id)
        } else if let Some(name) = self.name() {
            std::any::type_name_of_val(name)
        } else {
            panic!("No ID or Name found");
        }
    }
}

pub trait IdOrName: SqlEntity + Display + Send + Sync {
    type Id: SqlEntity;
    fn id(&self) -> Option<&Self::Id>;

    type Name: SqlEntity;
    fn name(&self) -> Option<&Self::Name>;
}

pub trait DataAccessObject:
    for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> + Send + Sync + Unpin + std::fmt::Debug
{
    type Builder;

    fn sql_table() -> &'static str;
    fn order_by() -> &'static str;
    fn fields() -> &'static [&'static str];
    fn immutable_fields() -> &'static [&'static str];
    fn sql_field_for_type(val: &str) -> Option<&'static str>;
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
    fn pagination_by() -> &'static str;
    fn pagination_value(&self) -> String;
    fn order_by_fields() -> &'static [&'static str];
    fn order_by_str_value(&self, ordered_by_field: &Option<String>) -> Option<String>;
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

pub trait VersionedAt {
    type Order: SqlEntity;
    fn order_by() -> &'static str;

    type Condition: SqlEntity;
    fn condition_by() -> &'static str;
}

pub trait PartitionBy {
    type PartitionBy: SqlEntity;
    fn partition_by() -> &'static str;
}

pub trait Recursive {
    type Recursive: SqlEntity;
    fn recurse_up() -> &'static str;
    fn recurse_down() -> &'static str;
}
