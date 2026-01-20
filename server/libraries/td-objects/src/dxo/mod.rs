//
// Copyright 2025 Tabs Data Inc.
//

//! # DXO
//! DXO refers to the following diagram, composed on DAO, DLO and DTO.
//!
//! ```text
//!     +---------------------------+
//!     |       API / DTO Layer     |
//!     +---------------------------+
//!                |
//!     +---------------------------+
//!     |       Logic Layer (DLO)   |
//!     +---------------------------+
//!                |
//!     +---------------------------+
//!     |   DataAccessObject (DAO)  |
//!     +---------------------------+
//!                |
//!     +---------------------------+
//!     |       Database / SQLx     |
//!     +---------------------------+
//! ```

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
pub mod yaml;

/// # DataAccessObject
/// Core trait for DAO types that operate directly on database tables.
///
/// ## Responsibilities
/// - Provide metadata about the SQL table (name, fields, immutable fields)
/// - Map Rust values into SQL query builders
/// - Resolve SQL field names based on Rust types
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

/// # DataLogicObject (DLO)
/// Represents higher-level logic that sits above DAO but below DTO.
pub trait DataLogicObject {
    type Builder;
}

/// # DataTransferObject (DTO)
/// DTOs represent API-facing structures.
pub trait DataTransferObject {
    type Builder;
}

/// # ListQuery
/// Trait implemented by DTOs that support list endpoints.
/// Provides mapping between DTO fields and DAO fields.
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

/// Extractor for reading values from objects.
pub trait Extractor<T> {
    fn extract(&self) -> T;
}

/// Setter for writing values into objects.
pub trait Setter<T>: Sized {
    fn set(&mut self, value: &T) -> &mut Self;
}

/// TryUpdater for fallible mutation.
pub trait TryUpdater<T>: Sized {
    type Error;
    fn try_update_from(&mut self, value: &T) -> Result<&mut Self, Self::Error>;
}

/// # States
/// Represents compile-time known states used to generate **stateful database views**.
/// Not equivalent to SQL WHERE clauses: states define semantic constraints baked into
/// system logic.
///
/// ## Example
/// A Function may be:
/// - Active
/// - Frozen
/// - Deleted (not user-queryable)
///
/// ## Diagram: State Filtering
/// ```text
///     ┌───────────────┐
///     │ All Functions │
///     └───────┬───────┘
///             │ internal filter (compile-time)
///             v
///     ┌───────────────────┐
///     │ Active + Frozen   │ <-- User-queryable states
///     └───────────────────┘
/// ```
pub trait States<const S: u8> {
    fn state() -> &'static [impl crate::types::AsDynSqlEntities + 'static];
}

/// The `Versioned` trait marks DAO entities whose historical state must be queryable
/// through *versioned views*. These views cannot be implemented as static SQL views
/// because they need to reflect the state of the system at arbitrary points in time.
///
/// A versioned view is constructed by creating a **ranked window** over the table,
/// using:
/// - `Versioned::Partition` as the partitioning key (e.g., `function_id`)
/// - `Versioned::Order`     as the ordering column (e.g., `timestamp`, `version_number`)
///
/// By ranking rows within each partition, we can:
/// - Reconstruct the state of an entity at any historical point in time
/// - Determine which version was active at a chosen moment
/// - Inspect older or newer versions relative to any version boundary
///
/// Example:
/// Updating a `FunctionDB` creates a new row with the same `function_id`.
/// By partitioning on `function_id` and ordering by the id/timestamp,
/// the latest ranked row represents the active version, while older rows
/// represent historical versions.
///
///
/// Diagram: Versioned Row Evolution and Ranking
///
///   Partition: function_id = 42
///
/// ```text
///       +-------------+----------------+---------+---------+-----------+
///       | function_id | version_order  |  data   | active? | rank()    |
///       +-------------+----------------+---------+---------+-----------+
///       |     42      |      1         |   ...   |   no    |    3      |
///       |     42      |      5         |   ...   |   no    |    2      |
///       |     42      |      9         |   ...   |  yes    |    1      | <-- latest version
///       +-------------+----------------+---------+---------+-----------+
/// ```
///
/// A versioned query can pick:
/// - `rank = 1` to get the state *at the current time*
/// - Any row with `rank = X` to get the relative version to that one
/// - Rows with higher ranks to explore historical versions
pub trait Versioned {
    type Order: crate::types::SqlEntity;
    fn order_by() -> &'static str;

    type Partition: crate::types::SqlEntity;
    fn partition_by() -> &'static str;
}

/// # Recursive
/// Supports queries that traverse hierarchical structures using recursive CTEs.
/// Useful for dependencies, tree structures, and graph-like relations.
/// The type of the up and down columns must be the same. Equality of those columns
/// will be used in the recursive queries.
pub trait Recursive {
    type Recursive: crate::types::SqlEntity + 'static;
    fn recurse_up() -> &'static str;
    fn recurse_down() -> &'static str;
}

/// Trait used by table_data_version__* entities to query table data versions.
pub trait DataVersion: DataAccessObject + Versioned {}
