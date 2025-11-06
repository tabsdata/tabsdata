//
// Copyright 2025 Tabs Data Inc.
//

use std::any::TypeId;
use std::fmt::Debug;

// Basic types reexported for convenience under basic module
pub mod basic;

pub mod addresses;
pub mod composed;
pub mod option;
pub mod status_count;
pub mod visible_collections;

#[cfg(test)]
mod tests;

/// A trait for types that can be used as SQL entities.
pub trait SqlEntity: Debug + Send + Sync {
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>);

    fn push_bind_unseparated<'a>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
    );

    fn as_display(&self) -> String;
    fn from_display(s: impl ToString) -> Result<Self, td_error::TdError>
    where
        Self: Sized;

    fn as_dyn(&self) -> &dyn SqlEntity
    where
        Self: Sized,
    {
        self as &dyn SqlEntity
    }

    fn as_any(&self) -> &dyn std::any::Any;
    fn type_id(&self) -> TypeId {
        self.as_any().type_id()
    }
}

impl<T> SqlEntity for &T
where
    T: SqlEntity,
{
    fn push_bind<'a>(&'a self, builder: &mut sqlx::QueryBuilder<'a, sqlx::Sqlite>) {
        (**self).push_bind(builder)
    }

    fn push_bind_unseparated<'a>(
        &'a self,
        builder: &mut sqlx::query_builder::Separated<'_, 'a, sqlx::Sqlite, &str>,
    ) {
        (**self).push_bind_unseparated(builder)
    }

    fn as_display(&self) -> String {
        (**self).as_display()
    }

    fn from_display(_s: impl ToString) -> Result<Self, td_error::TdError>
    where
        Self: Sized,
    {
        unreachable!("Cannot create reference from display string")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        (**self).as_any()
    }
}

pub trait AsDynSqlEntities: Send + Sync {
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity>;
}

// Slice of references
impl AsDynSqlEntities for [&dyn SqlEntity] {
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.to_vec()
    }
}

impl<const N: usize> AsDynSqlEntities for [&dyn SqlEntity; N] {
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.to_vec()
    }
}

impl<T> AsDynSqlEntities for [T]
where
    T: AsDynSqlEntities,
{
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.iter()
            .flat_map(AsDynSqlEntities::as_dyn_entities)
            .collect()
    }
}

impl<T, const N: usize> AsDynSqlEntities for [T; N]
where
    T: AsDynSqlEntities,
{
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.iter()
            .flat_map(AsDynSqlEntities::as_dyn_entities)
            .collect()
    }
}

impl<T> AsDynSqlEntities for Vec<T>
where
    T: AsDynSqlEntities,
{
    fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
        self.iter()
            .flat_map(AsDynSqlEntities::as_dyn_entities)
            .collect()
    }
}

macro_rules! impl_dyn_tuples {
    (
        [$($T:ident),*]
    ) => {
        #[allow(non_snake_case, unused_parens)]
        impl<$($T: SqlEntity),*> AsDynSqlEntities for ($($T),*) {
            fn as_dyn_entities(&self) -> Vec<&dyn SqlEntity> {
                let ($($T),*) = self;
                vec![$($T.as_dyn()),*]
            }
        }
    };
}

all_the_tuples!(impl_dyn_tuples);

pub trait DataAccessObject:
    for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> + Send + Sync + Unpin + std::fmt::Debug
{
    type Builder;

    fn sql_table() -> &'static str;
    fn order_by() -> &'static str;
    fn fields() -> &'static [&'static str];
    fn immutable_fields() -> &'static [&'static str];
    fn sql_field_for_type(type_id: TypeId) -> Result<&'static str, td_error::TdError>;
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

pub trait ListQuery: DataTransferObject + Clone + Send + Sync {
    type Dao: DataAccessObject;

    fn list_on() -> &'static str {
        Self::Dao::sql_table()
    }

    fn fields() -> &'static [&'static str] {
        Self::Dao::fields()
    }

    fn map_sql_entity_value(
        name: &str,
        filter_value: &str,
    ) -> Result<Option<Box<dyn SqlEntity>>, td_error::TdError>;

    fn try_from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError>
    where
        Self: Sized;
    fn map_dao_field(name: &str) -> String;
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

pub trait States<const S: u8> {
    fn state() -> &'static [&'static dyn SqlEntity];
}

pub trait Versioned {
    type Order: SqlEntity;
    fn order_by() -> &'static str;

    type Partition: SqlEntity;
    fn partition_by() -> &'static str;
}

pub trait Recursive {
    type Recursive: SqlEntity + 'static;
    fn recurse_up() -> &'static str;
    fn recurse_down() -> &'static str;
}
