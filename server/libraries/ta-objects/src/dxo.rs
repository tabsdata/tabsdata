//
// Copyright 2026 Tabs Data Inc.
//

/// Any type with a `derive_builder`-generated builder.
/// Implemented automatically by `#[Dao]`, `#[Dlo]`, `#[Dto]`.
pub trait Buildable {
    type Builder;

    fn builder() -> Self::Builder;

    fn into_builder(self) -> Self::Builder;

    fn build(builder: &Self::Builder) -> Result<Self, td_error::TdError>
    where
        Self: Sized;
}

impl<E> Buildable for Vec<E>
where
    E: Buildable,
{
    type Builder = Vec<E::Builder>;

    fn builder() -> Self::Builder {
        Vec::new()
    }

    fn into_builder(self) -> Self::Builder {
        self.into_iter().map(|e| e.into_builder()).collect()
    }

    fn build(builder: &Self::Builder) -> Result<Self, td_error::TdError> {
        builder.iter().map(|b| E::build(b)).collect()
    }
}

/// Extractor for reading values from objects.
pub trait Extractor<T> {
    fn extract(&self) -> T;
}

impl<T, E> Extractor<Vec<T>> for Vec<E>
where
    E: Extractor<T>,
{
    fn extract(&self) -> Vec<T> {
        self.iter().map(|item| item.extract()).collect()
    }
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

impl<T, E> TryUpdater<T> for Vec<E>
where
    E: TryUpdater<T>,
{
    type Error = E::Error;

    fn try_update_from(&mut self, value: &T) -> Result<&mut Self, Self::Error> {
        for elem in self.iter_mut() {
            elem.try_update_from(value)?;
        }
        Ok(self)
    }
}

/// Maps Rust domain types to Sea Query column references.
/// Used by `resolve_condition` to construct WHERE clauses from typed values.
///
/// ## Constraint
/// Each domain type must appear at most once as a field type in the struct.
/// Two fields of the same domain type are ambiguous — use explicit column
/// refs for those fields instead.
pub trait TypedColumns {
    type Iden: sea_query::Iden
        + std::str::FromStr<Err = td_error::TdError>
        + Copy
        + Send
        + Sync
        + std::fmt::Debug;
    fn col_for_type(type_id: std::any::TypeId) -> Option<Self::Iden>;
}

/// Minimal structural metadata shared by all DAO types (read, write, update).
pub trait DaoCore: Buildable + TypedColumns + Send + Sync + std::fmt::Debug {
    /// Table identifier.
    fn table() -> Self::Iden;

    /// Human-readable entity name for error messages (e.g., "User", "Collection").
    fn display_name() -> &'static str;

    /// Column name → display value pairs for error messages.
    fn column_display_values(&self) -> Vec<(String, String)>;
}

/// Update capability. Implemented by update-only DAOs (`#[DaoUpdate]`) and
/// by regular DAOs that support both read and update operations.
pub trait DaoUpdate: DaoCore {
    /// Column-value pairs for the UPDATE SET clause.
    fn update_values(&self) -> impl IntoIterator<Item = (Self::Iden, sea_query::Expr)>;
}

/// Full read/write DAO for a directly-queryable database table.
pub trait Dao: DaoCore + td_database::dynamic::DynamicFromRow + Unpin {
    /// All columns in the same order as `insert_values()`.
    fn columns() -> impl IntoIterator<Item = Self::Iden>;

    /// Row values in column order — used to build INSERT.
    fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr>;

    /// Default result ordering.
    fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)>;
}

/// An append-only versioned table. Never queried directly — always through
/// a ranked-window CTE. Insertable only.
///
/// ## Structural columns
/// - **Pk**: primary key. Used as a tiebreaker in ROW_NUMBER windows when
///   version_order values are tied.
/// - **Partition**: groups rows into per-entity version histories.
/// - **VersionOrder**: determines "latest" within a partition; also used for
///   point-in-time filtering (`WHERE version_order <= ?`).
pub trait VersionedDao: Dao {
    type Pk: crate::SeaValue;
    fn pk_col() -> Self::Iden;

    type Partition: crate::SeaValue;
    fn partition_col() -> Self::Iden;

    type VersionOrder: crate::SeaValue;
    fn version_order_col() -> Self::Iden;
}

/// A versioned entity that also has a `_current` table for fast list queries.
/// The `_current` table is maintained by AFTER INSERT triggers and contains
/// exactly one row per partition (the latest version).
pub trait CurrentDao: VersionedDao {
    /// The `_current__with_names` view name (e.g., `"functions_current__with_names"`).
    fn current_table_name() -> &'static str;
}

/// Compile-time state filter for queries.
/// `S = 0` is always `All`. States >= 1 are user-defined.
pub trait States<const S: u8>: DaoCore {
    fn state() -> impl crate::sql_condition::SqlCondition;
}

/// A `VersionedDao` supporting version range queries (HEAD~N, INITIAL+N, ranges).
pub trait DataVersionDao: VersionedDao + DaoUpdate {}

/// Hierarchical traversal via recursive CTEs. Only for `DataAccessObject`.
pub trait Recursive: Dao {
    type Key: crate::SeaValue + 'static;
    fn recurse_up() -> Self::Iden;
    fn recurse_down() -> Self::Iden;
}

pub trait Dlo: Buildable {}
pub trait Dto: Buildable {}

/// DTO trait for list/filter endpoints.
pub trait ListDto: Dto + Clone + Send + Sync {
    type Dao: Dao;

    /// Resolve a filter field name (eq/ne/gt/ge/lt/le/btw/is) to a DAO column.
    /// `None` = field not valid for this role.
    fn filter_col(field: &str) -> Option<<Self::Dao as TypedColumns>::Iden>;

    /// Resolve a LIKE filter field name to a DAO column.
    fn filter_like_col(field: &str) -> Option<<Self::Dao as TypedColumns>::Iden>;

    /// Resolve an order-by field name to a DAO column.
    fn order_by_col(field: &str) -> Option<<Self::Dao as TypedColumns>::Iden>;

    /// Natural pagination column and its default direction.
    fn pagination_col() -> (<Self::Dao as TypedColumns>::Iden, sea_query::Order);

    /// Parse a string filter value into a typed `SeaValue` for the given field.
    fn parse_filter_value(
        field: &<Self::Dao as TypedColumns>::Iden,
        value: &str,
    ) -> Result<Option<Box<dyn crate::SeaValue>>, td_error::TdError>;

    fn pagination_value(&self) -> String;
    fn order_by_value(&self, field: &<Self::Dao as TypedColumns>::Iden) -> Option<String>;

    /// Convert a DAO row into this DTO.
    fn from_dao(dao: &Self::Dao) -> Result<Self, td_error::TdError>;
}
