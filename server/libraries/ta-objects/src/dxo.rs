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
    /// The domain this DAO's table lives in; `#[dao(db = ...)]`, default
    /// [`Apiserver`](td_database::domain::Apiserver).
    type Db: td_database::domain::DbDomain;

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
/// Implemented by every DAO regardless of whether it's a live table or a
/// versioned event/timeline table — anywhere we need column metadata
/// (`SELECT` column list, `INSERT` value list, default ordering) we bound
/// on `Dao`. The "is it safe to read raw?" property lives on the more
/// restrictive [`LiveDao`] marker.
pub trait Dao: DaoCore + td_database::dynamic::DynamicFromRow + Unpin {
    /// All columns in the same order as `insert_values()`.
    fn columns() -> impl IntoIterator<Item = Self::Iden>;

    /// Row values in column order — used to build INSERT.
    fn insert_values(&self) -> impl IntoIterator<Item = sea_query::Expr>;

    /// Default result ordering.
    fn order_by() -> impl IntoIterator<Item = (Self::Iden, sea_query::Order)>;
}

/// Marker for DAOs whose rows are uniquely addressable by primary key:
/// one live row per id, or insert-once with stable PK (versioned
/// timelines). Bare `BasicQueries::fetch_*` methods bound on `LiveDao`
/// so true append-only event logs (multiple rows per partition) can't
/// be read raw — those go through `current()` / `versioned(...)` /
/// `historic(...)`. Opt out with `#[dao(... event_log, ...)]`.
///
/// **Footgun on insert-once timelines** (FunctionDB / TableDB / etc.):
/// `LiveDao` is emitted because the PK is unique, so
/// `db.live().fetch_one::<FunctionDB>(...)` compiles — but it's safe **only
/// when the condition is the row's PK** (e.g. `function_version_id`).
/// A partition column (`function_id`) silently matches multiple
/// versions; for "latest version at time Y", use
/// [`crate::QueryEntry::current`] / [`crate::QueryEntry::versioned`].
pub trait LiveDao: Dao {}

/// A DAO with a declared logical key (`#[dao(key(col, …))]`, which must
/// back a `UNIQUE` constraint): the key identifies the row, every other
/// column is its replaceable value. Unlocks the generic
/// `Live::upsert` (replace-by-key) / `Live::insert_or_ignore`
/// (first-write-wins-by-key) — an opt-in, so append-only / versioned
/// tables can't be conflict-replaced. The [`LiveDao`] supertrait makes
/// that structural: a keyed row is by definition one live row per key,
/// so `key(...)` on an `event_log` table refuses to compile.
pub trait KeyedDao: LiveDao {
    /// The key columns (the `ON CONFLICT (...)` target).
    fn key_columns() -> impl IntoIterator<Item = Self::Iden>;

    /// Every DAO column except the key, in `columns()` order — what a
    /// conflicting upsert replaces. Derived by the macro so it can't
    /// drift when columns are added.
    fn value_columns() -> impl IntoIterator<Item = Self::Iden>;
}

/// An append-only versioned table. Never queried directly — always
/// through a ranked-window CTE — for tables that *aren't* also
/// `LiveDao` (true event logs). For insert-once timeline tables, raw
/// PK lookups via [`Dao`] are safe.
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

/// Internal const-generic state filter trait. Kept for the soon-to-die
/// tower `By::<...>` handlers. New code should reach for
/// [`StateFilter<D>`] via the per-DAO value methods (`D::all()`,
/// `D::active()`) emitted by the `Dao` macro.
#[doc(hidden)]
pub trait States<const S: u8>: DaoCore {
    fn state() -> impl crate::sql_condition::SqlCondition;
}

/// State filter value for queries. Each `Dao` with `versioned(...)` or
/// `states(...)` gets a per-state value-returning method emitted by the
/// `Dao` macro (e.g. `FunctionDB::all()`, `FunctionDB::active()`); each
/// returns a [`StateFilter<Self>`] that pre-resolves the SQL condition.
///
/// Pass the value to a query wrapper's `.state(filter)` to bind both
/// the state filter *and* the DAO type:
///
/// ```ignore
/// db.current().state(FunctionDB::active()).fetch_one(&id).await?;
/// ```
///
/// The default for the bare wrapper (no `.state(...)`) is the implicit
/// All state — no extra WHERE clause beyond the user condition.
pub struct StateFilter<D> {
    _phantom: std::marker::PhantomData<D>,
    condition: sea_query::Condition,
}

impl<D> StateFilter<D> {
    /// Construct a filter from a pre-resolved sea-query condition.
    /// Used by macro-generated state methods; callers should reach for
    /// `D::all()` / `D::active()` etc. instead.
    pub fn new(condition: sea_query::Condition) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            condition,
        }
    }

    /// Borrow the cached condition for inclusion in a WHERE.
    pub fn condition(&self) -> &sea_query::Condition {
        &self.condition
    }
}

#[doc(hidden)]
impl<D: DaoCore> StateFilter<D> {
    /// Bridge for legacy tower handlers that still take `<const S: u8>`.
    /// New code should not call this — use `D::all()` / `D::active()` etc.
    pub fn from_states<const S: u8>() -> Self
    where
        D: States<S>,
    {
        let raw = <D as States<S>>::state();
        let mut where_cond = sea_query::Condition::all();
        if let Ok(resolved) = crate::sql_condition::resolve_condition::<D>(raw) {
            where_cond = where_cond.add(resolved);
        }
        Self::new(where_cond)
    }
}

/// A `VersionedDao` supporting version range queries (HEAD~N, INITIAL+N, ranges).
pub trait DataVersionDao: VersionedDao + DaoUpdate {}

/// Hierarchical traversal via recursive CTEs. Implementable by any
/// table-shaped DAO (live or versioned).
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
