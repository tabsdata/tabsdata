//
// Copyright 2026 Tabsdata Inc.
//

/// Error for a field conversion inside a generated DAO->DTO mapping
/// (`ListDto::from_dao`).
#[td_error::td_error]
pub enum DxoMappingError {
    #[error("{0}")]
    Field(String) = 5000,
}

/// Maps Rust domain types to Sea Query column references.
///
/// Each domain type appears at most once as a field type in a DAO struct —
/// the `#[Dao]` macro rejects duplicates (two fields of the same type would
/// make type-keyed column resolution ambiguous).
pub trait TypedColumns {
    type Iden: sea_query::Iden
        + std::str::FromStr<Err = td_error::TdError>
        + Copy
        + Send
        + Sync
        + std::fmt::Debug;

    /// Runtime type-to-column lookup, for the type-erased paths
    /// (`&dyn SeaValue` recursive references). Statically-typed conditions
    /// resolve through [`HasColumn`] instead.
    fn col_for_type(type_id: std::any::TypeId) -> Option<Self::Iden>;
}

/// Minimal structural metadata shared by all DAO types (read, write, update).
pub trait DaoCore: TypedColumns + Send + Sync + std::fmt::Debug {
    /// The domain this DAO's table lives in; `#[dao(db = ...)]`, default
    /// [`Apiserver`](td_database::domain::Apiserver).
    type Db: td_database::domain::DbDomain;

    fn table() -> Self::Iden;

    /// Human-readable entity name for error messages (e.g., "User", "Collection").
    fn display_name() -> &'static str;

    /// Column name → display value pairs for error messages.
    fn column_display_values(&self) -> Vec<(String, String)>;
}

/// The DAO's column of domain type `T`, one impl per field from the `#[Dao]`
/// macro — the compile-time column resolution behind [`SqlCondition`](crate::sql_condition::SqlCondition).
///
/// ```compile_fail
/// use ta_objects::{StateFilter, Typed, TypedOpenApi, TypedSerde, TypedSqlx};
///
/// #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
/// pub struct ItemName(String);
///
/// #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
/// pub struct OtherName(String);
///
/// #[ta_objects::Dao]
/// #[dao(sql_table = "items")]
/// pub struct ItemDao {
///     pub name: ItemName,
/// }
///
/// let _ = StateFilter::<ItemDao>::of(OtherName::try_from("x").unwrap());
/// ```
///
/// Duplicate field types are ambiguous, so the macro bans them:
///
/// ```compile_fail
/// use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
///
/// #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
/// pub struct ItemName(String);
///
/// #[ta_objects::Dao]
/// #[dao(sql_table = "items")]
/// pub struct ItemDao {
///     pub name: ItemName,
///     pub alias: ItemName,
/// }
/// ```
///
/// The bans see through `#[dao(flatten)]`, including field *names* (a
/// re-declared name would alias on decode / duplicate serde keys):
///
/// ```compile_fail
/// use ta_objects::{Typed, TypedOpenApi, TypedSerde, TypedSqlx};
///
/// #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
/// pub struct ItemName(String);
///
/// #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi)]
/// pub struct OwnerName(String);
///
/// #[ta_objects::Dao]
/// #[dao(sql_table = "items")]
/// pub struct ItemDao {
///     pub name: ItemName,
/// }
///
/// #[ta_objects::Dao]
/// #[dao(sql_table = "items__with_names")]
/// pub struct ItemDaoWithNames {
///     #[dao(flatten)]
///     pub base: ItemDao,
///     pub name: OwnerName,
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no column of type `{T}`",
    note = "conditions may only use the field types of the queried DAO"
)]
pub trait HasColumn<T>: DaoCore {
    const COL: Self::Iden;
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
pub trait Dao: DaoCore + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Unpin {
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

/// State filter value for queries. DAOs define their named states as plain
/// inherent fns (e.g. `FunctionDB::active()`) built with [`StateFilter::of`];
/// each returns a [`StateFilter<Self>`] that pre-resolves the SQL condition.
/// [`StateFilter::all`] is the universal no-filter state.
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
#[derive(Debug, Clone)]
pub struct StateFilter<D> {
    _phantom: std::marker::PhantomData<D>,
    condition: sea_query::Condition,
}

impl<D> StateFilter<D> {
    /// Construct a filter from a pre-resolved sea-query condition.
    /// State fns should reach for [`StateFilter::of`] instead.
    pub fn new(condition: sea_query::Condition) -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            condition,
        }
    }

    /// The state that admits every row (no extra WHERE).
    pub fn all() -> Self {
        Self::new(sea_query::Condition::all())
    }

    /// Borrow the cached condition for inclusion in a WHERE.
    pub fn condition(&self) -> &sea_query::Condition {
        &self.condition
    }
}

impl<D: DaoCore> StateFilter<D> {
    /// Resolve `condition` against `D`'s columns into a state filter.
    pub fn of(condition: impl crate::sql_condition::SqlCondition<D>) -> Self {
        Self::new(sea_query::Condition::all().add(condition.resolve()))
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

pub trait Dlo {}
pub trait Dto {}

/// Explicit DAO-to-DTO conversion, hand-implemented next to the DTO (the
/// counterpart of the hand-written `to_dao` constructors).
pub trait FromDao: Sized {
    type Dao: Dao;

    /// Convert a DAO row into this DTO, consuming it.
    fn from_dao(dao: Self::Dao) -> Result<Self, td_error::TdError>;
}

/// DTO trait for list/filter endpoints.
pub trait ListDto: Dto + Clone + Send + Sync + FromDao {
    /// Resolve a filter field name (eq/ne/gt/ge/lt/le/btw/is) to a DAO column.
    /// `None` = field not valid for this role.
    fn filter_col(field: &str) -> Option<<Self::Dao as TypedColumns>::Iden>;

    fn filter_like_col(field: &str) -> Option<<Self::Dao as TypedColumns>::Iden>;

    fn order_by_col(field: &str) -> Option<<Self::Dao as TypedColumns>::Iden>;

    /// Natural pagination column and its default direction.
    fn pagination_col() -> (<Self::Dao as TypedColumns>::Iden, sea_query::Order);

    fn parse_filter_value(
        field: &<Self::Dao as TypedColumns>::Iden,
        value: &str,
    ) -> Result<Option<Box<dyn crate::SeaValue>>, td_error::TdError>;

    fn pagination_value(&self) -> String;
    fn order_by_value(&self, field: &<Self::Dao as TypedColumns>::Iden) -> Option<String>;
}
