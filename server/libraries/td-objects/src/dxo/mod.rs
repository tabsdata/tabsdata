//
// Copyright 2025 Tabs Data Inc.
//

pub mod auth;
pub mod bundle;
pub mod collection;
pub mod crudl;
pub mod dependency;
pub mod execution;
pub mod file;
pub mod function;
pub mod function_requirement;
pub mod function_run;
pub mod function_upload;
pub mod global_status;
pub mod inter_collection_access;
pub mod inter_collection_permission;
pub mod permission;
pub mod request;
pub mod role;
pub mod runtime_info;
pub mod synchrotron;
pub mod system;
pub mod table;
pub mod table_data_version;
pub mod transaction;
pub mod trigger;
pub mod user;
pub mod user_role;
pub mod worker;

pub trait DataAccessObject:
    for<'a> sqlx::FromRow<'a, sqlx::sqlite::SqliteRow> + Send + Sync + Unpin + std::fmt::Debug
{
    type Builder;

    fn sql_table() -> &'static str;
    fn order_by() -> &'static str;
    fn fields() -> &'static [&'static str];
    fn immutable_fields() -> &'static [&'static str];
    fn sql_field_for_type(type_id: std::any::TypeId) -> Result<&'static str, td_error::TdError>;
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
    ) -> Result<Option<Box<dyn crate::types::SqlEntity>>, td_error::TdError>;

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

pub trait Setter<T>: Sized {
    fn set(&mut self, value: &T) -> &mut Self;
}

pub trait TryUpdater<T>: Sized {
    type Error;
    fn try_update_from(&mut self, value: &T) -> Result<&mut Self, Self::Error>;
}

pub trait States<const S: u8> {
    fn state() -> &'static [&'static dyn crate::types::SqlEntity];
}

pub trait Versioned {
    type Order: crate::types::SqlEntity;
    fn order_by() -> &'static str;

    type Partition: crate::types::SqlEntity;
    fn partition_by() -> &'static str;
}

pub trait Recursive {
    type Recursive: crate::types::SqlEntity + 'static;
    fn recurse_up() -> &'static str;
    fn recurse_down() -> &'static str;
}
